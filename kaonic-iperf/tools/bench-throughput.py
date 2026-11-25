#!/usr/bin/env python3
"""tx_only throughput sweep for kaonic-iperf: every OFDM / QPSK setting.

For each modulation setting, the receiving side runs `kaonic-iperf --server
--tx-only` and the sending side `kaonic-iperf --client --tx-only`, both with
the same `--modulation`. The sender streams packets back to back, the receiver
counts them, and the receiver's measured rate is the result. Each side runs on
the machine named by its endpoint, so one copy of this script, normally on a
dev machine, drives both boards:

    R="ssh -J me@jump root@board-b"
    python3 bench-throughput.py --tx-iperf ./kaonic-iperf --tx-addr 10.78.137.1 \\
        --rx "$R" --rx-iperf /usr/bin/kaonic-iperf --rx-addr 127.0.0.1 \\
        --rx-config /etc/kaonic-config.toml
    python3 bench-throughput.py ... --phys ofdm            # only OFDM
    python3 bench-throughput.py ... --phys qpsk            # only QPSK
    python3 bench-throughput.py ... --phys ofdm:6:1,qpsk:2000:4
    python3 bench-throughput.py ... --quick
    python3 bench-throughput.py --dry-run                  # list settings and timing

An endpoint (`--tx`, `--rx`) is a command prefix that kaonic-iperf runs under:
- empty (the default): run on this machine;
- a whole ssh command, `ssh [options] [user@]host`: options such as `-J` pass
  through, and the script adds `-T -o BatchMode=yes`. Logins must not prompt
  for a password: check with `<ssh command> -o BatchMode=yes true` first.

Each side needs, on its machine: a kaonic-iperf built from this branch
(`--tx-iperf`/`--rx-iperf`), a kaonic-config.toml (`--tx-config`/
`--rx-config`) and the address of its board's kaonic-commd (`--tx-addr`/
`--rx-addr`; 127.0.0.1 when kaonic-iperf runs on the board itself). Both
configs must put the [iperf] module on the same frequency and channel; the
modulation preset in them is replaced by each setting under test, keeping
its TX power.

Run length: each setting runs for `--secs`, or longer for slow settings, so
that at least `--min-packets` packets fit (a 2 KB packet takes seconds at the
slowest QPSK settings). The receiver stops after a few packet times without
packets. Both come from a rough airtime model; `--dry-run` shows them.

Settings the board rejects (kaonic-iperf reads the modulation back) show as
`error`. On a sub-GHz module, wide OFDM options may not fit the band plan.

Results go to `<out>/results.csv` and `<out>/results.json`, logs of every run
to `<out>/logs/`, and a table of the settings, best received throughput first,
is printed at the end. Exits 0 if every setting ran (whatever its throughput),
1 if any errored, 2 on bad arguments.
"""

import argparse
import csv
import json
import math
import os
import queue
import shlex
import subprocess
import sys
import threading
import time

# Bandwidth option -> valid MCS (MCS 0/1 need more subcarriers than options 3/4 have).
OFDM_MCS = {1: range(0, 7), 2: range(0, 7), 3: range(1, 7), 4: range(2, 7)}
OFDM_SUBCARRIERS = {1: 96, 2: 48, 3: 24, 4: 12}
# MCS -> data bits per subcarrier per symbol (modulation x coding / repetition).
OFDM_BITS = {0: 1 * 0.5 / 4, 1: 1 * 0.5 / 2, 2: 2 * 0.5 / 2, 3: 2 * 0.5, 4: 2 * 0.75, 5: 4 * 0.5, 6: 4 * 0.75}
OFDM_SYMBOL_US = 120
QPSK_KCHIP = [100, 200, 1000, 2000]
# Rough MR-O-QPSK data rates (kbit/s) per chip rate and rate mode; rate mode 4
# only exists at 2000 kchip/s.
QPSK_KBPS = {
    100: [6.25, 12.5, 25, 50],
    200: [12.5, 25, 50, 100],
    1000: [31.25, 125, 250, 500],
    2000: [62.5, 250, 500, 1000, 2000],
}

QUICK_PHYS = ["ofdm:6:1", "ofdm:3:1", "ofdm:6:4", "qpsk:2000:4", "qpsk:1000:2", "qpsk:200:3"]

# Time allowed beyond the expected run: process start-up (ssh through a jump
# host can take seconds), radio configuration and shutdown.
STARTUP_S = 30
SLACK_S = 30
READY_LINE = "Listening..."


class Phy:
    def __init__(self, kind, a, b):
        self.kind, self.a, self.b = kind, a, b

    @staticmethod
    def parse(spec):
        """`ofdm:<mcs>:<opt>` (opt 1-4) or `qpsk:<kchip>:<mode>`."""
        parts = spec.split(":")
        try:
            kind, a, b = parts[0], int(parts[1]), int(parts[2])
        except (IndexError, ValueError):
            raise ValueError(f"bad setting {spec!r}") from None
        if len(parts) == 3:
            if kind == "ofdm" and b in OFDM_MCS and a in OFDM_MCS[b]:
                return Phy("ofdm", a, b)
            if kind == "qpsk" and a in QPSK_KBPS and 0 <= b < len(QPSK_KBPS[a]):
                return Phy("qpsk", a, b)
        raise ValueError(f"bad setting {spec!r}")

    def spec(self):
        return f"{self.kind}:{self.a}:{self.b}"

    def __str__(self):
        return f"{self.kind}{self.a}/{self.b}"

    def kbps(self):
        """Nominal PHY data rate, kbit/s."""
        if self.kind == "ofdm":
            return OFDM_SUBCARRIERS[self.b] * OFDM_BITS[self.a] / OFDM_SYMBOL_US * 1000
        return QPSK_KBPS[self.a][self.b]

    def airtime_s(self, size):
        """Rough airtime of a `size`-byte packet, preamble and header included."""
        if self.kind == "ofdm":
            # STF + LTF are 6 symbols; the PHR is 3 (option 1) or 6 symbols.
            fixed = 6 + (3 if self.b == 1 else 6)
            bits_per_symbol = OFDM_SUBCARRIERS[self.b] * OFDM_BITS[self.a]
            data = math.ceil((8 * (size + 4) + 6) / bits_per_symbol)
            return (fixed + data) * OFDM_SYMBOL_US / 1e6
        fixed_us = 500 if self.a <= 200 else 200
        return (fixed_us + (size + 4) * 8 * 1000 / self.kbps()) / 1e6


def default_phys(kinds):
    phys = []
    if "ofdm" in kinds:
        phys += [Phy("ofdm", mcs, opt) for opt in OFDM_MCS for mcs in OFDM_MCS[opt]]
    if "qpsk" in kinds:
        phys += [Phy("qpsk", c, m) for c in QPSK_KCHIP for m in range(len(QPSK_KBPS[c]))]
    return phys


def run_length(phy, args):
    """(test duration, receiver idle time, first-packet wait), seconds."""
    airtime = phy.airtime_s(args.size)
    duration = max(args.secs, math.ceil(args.min_packets * airtime))
    idle = max(2.0, 3 * airtime + 1)
    return duration, idle, STARTUP_S + idle


# --- endpoints -----------------------------------------------------------------


class Side:
    """Where one kaonic-iperf runs: a command prefix, its binary, config and commd."""

    def __init__(self, name, prefix, iperf, config, addr):
        self.name, self.iperf, self.config, self.addr = name, iperf, config, addr
        self.prefix = shlex.split(prefix) if prefix else []
        if self.prefix and self.prefix[0] == "ssh":
            if len(self.prefix) < 2:
                raise ValueError(f"--{name} has no host")
            # Options go before the destination: ssh runs anything after it.
            self.prefix = [self.prefix[0], "-T", "-o", "BatchMode=yes"] + self.prefix[1:]

    def argv(self, iperf_args):
        # Keep kaonic-iperf's logging to warnings: it logs every message at
        # trace level by default.
        cmd = ["env", "RUST_LOG=warn", self.iperf, "-c", self.config, "-a", self.addr] + iperf_args
        if self.prefix and self.prefix[0] == "ssh":
            # ssh passes the remote command to a shell as one string.
            return self.prefix + [shlex.join(cmd)]
        return self.prefix + cmd


class Proc:
    """A running kaonic-iperf: its output lines, logged to a file as they arrive."""

    def __init__(self, argv, log_path):
        self.lines = queue.Queue()
        self.result = None
        self.tail = []
        self.log = open(log_path, "w")
        self.log.write("$ " + shlex.join(argv) + "\n")
        self.log.flush()
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()

    def _read(self):
        for line in self.proc.stdout:
            self.log.write(line)
            self.log.flush()
            line = line.rstrip("\n")
            self.tail = (self.tail + [line])[-5:]
            if line.startswith("RESULT "):
                try:
                    self.result = json.loads(line[len("RESULT "):])
                except json.JSONDecodeError:
                    pass
            self.lines.put(line)
        self.lines.put(None)

    def wait_line(self, text, timeout):
        """True once a line equal to `text` arrives; False on exit or timeout."""
        deadline = time.monotonic() + timeout
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                return False
            try:
                line = self.lines.get(timeout=left)
            except queue.Empty:
                return False
            if line is None:
                return False
            if line.strip() == text:
                return True

    def wait(self, timeout):
        """Exit code, or None if it is still running after `timeout`."""
        try:
            code = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None
        self.reader.join(timeout=5)
        return code

    def kill(self):
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()
        self.reader.join(timeout=5)

    def close(self):
        self.kill()
        self.log.close()

    def why(self):
        last = next((l for l in reversed(self.tail) if l.strip()), "")
        return last.strip() or f"exit code {self.proc.returncode}"


# --- one setting ---------------------------------------------------------------


def run_setting(phy, args, tx, rx, log_dir):
    duration, idle, first_wait = run_length(phy, args)
    row = {
        "setting": str(phy),
        "spec": phy.spec(),
        "status": "error",
        "nominal_kbps": round(phy.kbps(), 2),
        "airtime_ms": round(phy.airtime_s(args.size) * 1000, 2),
        "duration_s": duration,
        "size": args.size,
    }
    name = phy.spec().replace(":", "-")
    common = ["--tx-only", "--modulation", phy.spec(), "--json"]

    receiver = Proc(
        rx.argv(["--server"] + common + ["--idle-exit", f"{idle:.1f}", "--first-packet-timeout", f"{first_wait:.1f}"]),
        os.path.join(log_dir, f"{name}-rx.log"),
    )
    sender = None
    try:
        if not receiver.wait_line(READY_LINE, STARTUP_S):
            row["error"] = f"receiver didn't start: {receiver.why()}"
            return row

        sender = Proc(
            tx.argv(["--client"] + common + ["--duration", str(duration), "--payload-size", str(args.size)]),
            os.path.join(log_dir, f"{name}-tx.log"),
        )
        if sender.wait(duration + STARTUP_S + SLACK_S) is None:
            row["error"] = "sender didn't finish"
            return row
        # The receiver stops by itself once packets stop arriving.
        if receiver.wait(first_wait + SLACK_S) is None:
            row["error"] = "receiver didn't stop"
            return row

        t, r = sender.result, receiver.result
        if t is None:
            row["error"] = f"sender: {sender.why()}"
            return row
        if r is None:
            row["error"] = f"receiver: {receiver.why()}"
            return row

        sent, received = t["sent"], r["received"]
        loss = max(0, sent - received) / sent * 100 if sent else None
        row.update({
            "rx_kbps": round(r["rx_kbps"], 2),
            "loss_pct": None if loss is None else round(loss, 2),
            "rx_pkt_rate": round(r["rx_pkt_rate"], 2),
            "sent": sent,
            "received": received,
            "tx_errors": t["tx_errors"],
            "crc_errors": r["crc_errors"],
            "duplicates": r["duplicates"],
            "offered_kbps": round(t["offered_kbps"], 2),
            "offered_pkt_rate": round(t["pkt_rate"], 2),
            "rx_span_s": round(r["span_s"], 3),
        })
        if received == 0:
            row["status"] = "no rx"
        elif loss is not None and loss > args.lossy_pct:
            row["status"] = "lossy"
        else:
            row["status"] = "ok"
        row.pop("error", None)
        return row
    finally:
        for p in (sender, receiver):
            if p is not None:
                p.close()


# --- output --------------------------------------------------------------------


def table(rows):
    """One row per setting, best received throughput first; settings with
    nothing measured at the bottom, in run order."""
    head = ["setting", "status", "nominal", "rx_kbps", "loss", "pkt/s", "sent", "received", "tx_err", "crc",
            "offered"]
    measured, other = [], []
    for r in rows:
        has = r["status"] in ("ok", "lossy")
        cells = [
            r["setting"],
            r["status"],
            f"{r['nominal_kbps']:.1f}",
            f"{r['rx_kbps']:.1f}" if has else "",
            f"{r['loss_pct']:.1f}%" if r.get("loss_pct") is not None else "",
            f"{r['rx_pkt_rate']:.1f}" if has else "",
            str(r.get("sent", "")),
            str(r.get("received", "")),
            str(r.get("tx_errors", "")),
            str(r.get("crc_errors", "")),
            f"{r['offered_kbps']:.1f}" if "offered_kbps" in r else "",
        ]
        (measured if has else other).append((r.get("rx_kbps", 0), cells))
    measured.sort(key=lambda x: -x[0])
    cells = [c for _, c in measured] + [c for _, c in other]
    widths = [max(len(x) for x in col) for col in zip(head, *cells)]
    fmt = lambda r: "  ".join(c.ljust(w) if i < 2 else c.rjust(w) for i, (c, w) in enumerate(zip(r, widths)))
    return "\n".join([fmt(head)] + [fmt(c) for c in cells])


def write_csv(path, rows):
    fields = []
    for r in rows:
        fields += [k for k in r if k not in fields]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for side, what in (("tx", "sending"), ("rx", "receiving")):
        p.add_argument(f"--{side}", default="",
                       help=f"{what} side: a command prefix such as \"ssh -J me@jump root@board\" (default: local)")
        p.add_argument(f"--{side}-iperf", default="kaonic-iperf",
                       help=f"kaonic-iperf on the {what} side's machine (default: %(default)s)")
        p.add_argument(f"--{side}-config", default="kaonic-config.toml",
                       help=f"kaonic-config.toml on the {what} side's machine (default: %(default)s)")
        p.add_argument(f"--{side}-addr", default="10.78.137.1",
                       help=f"the {what} board's kaonic-commd, from its machine (default: %(default)s)")
    p.add_argument("--phys", default="ofdm,qpsk",
                   help="kinds (ofdm, qpsk) and/or settings like ofdm:6:1,qpsk:2000:4; "
                        "OFDM is ofdm:<mcs 0-6>:<option 1-4> (default: %(default)s)")
    p.add_argument("--quick", action="store_true",
                   help=f"only a few settings of the kinds in --phys: {', '.join(QUICK_PHYS)}")
    p.add_argument("--secs", type=int, default=10, help="test time per setting, at least (default: %(default)s)")
    p.add_argument("--min-packets", type=int, default=30,
                   help="lengthen slow settings' runs to fit this many packets (default: %(default)s)")
    p.add_argument("--size", type=int, default=2046, help="packet size in bytes, 24-2048 (default: %(default)s)")
    p.add_argument("--lossy-pct", type=float, default=10,
                   help="mark a setting lossy above this %% loss (default: %(default)s)")
    p.add_argument("--out", help="results directory (default: throughput-<date>-<time>)")
    p.add_argument("--dry-run", action="store_true", help="list the settings and run times, don't run")
    args = p.parse_args()

    try:
        specs = [s for s in args.phys.split(",") if s]
        kinds = [s for s in specs if s in ("ofdm", "qpsk")]
        settings = [Phy.parse(s) for s in specs if s not in kinds]
        if args.quick:
            if settings:
                p.error("--quick picks the settings itself; give --phys only kinds (ofdm, qpsk)")
            phys = [ph for ph in map(Phy.parse, QUICK_PHYS) if ph.kind in kinds]
        else:
            phys = default_phys(kinds) + settings
        tx = Side("tx", args.tx, args.tx_iperf, args.tx_config, args.tx_addr)
        rx = Side("rx", args.rx, args.rx_iperf, args.rx_config, args.rx_addr)
    except ValueError as e:
        p.error(str(e))
    if not phys:
        p.error("no settings selected")
    if not 24 <= args.size <= 2048:
        p.error("--size must be 24-2048")
    if args.secs < 1 or args.min_packets < 1:
        p.error("--secs and --min-packets must be at least 1")

    est = sum(d + i + 5 for d, i, _ in (run_length(ph, args) for ph in phys))
    if args.dry_run:
        print(f"{len(phys)} settings, {args.size}-byte packets, about {est / 60:.0f} min:")
        for ph in phys:
            d, i, _ = run_length(ph, args)
            print(f"  {str(ph):11s} nominal={ph.kbps():7.1f} kbit/s  airtime={ph.airtime_s(args.size) * 1000:8.1f} ms"
                  f"  run={d:4d} s  idle={i:5.1f} s")
        return 0

    out_dir = args.out or time.strftime("throughput-%Y%m%d-%H%M%S")
    log_dir = os.path.join(out_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    print(f">> {len(phys)} settings, about {est / 60:.0f} min; results in {out_dir}/", flush=True)
    rows, errors = [], 0
    try:
        for n, ph in enumerate(phys, 1):
            d, _, _ = run_length(ph, args)
            print(f"[{n}/{len(phys)}] {ph} ({d} s)", flush=True)
            row = run_setting(ph, args, tx, rx, log_dir)
            if row["status"] == "error":
                errors += 1
                print(f"    error: {row['error']}", flush=True)
            else:
                loss = "" if row.get("loss_pct") is None else f", loss {row['loss_pct']:.1f}%"
                print(f"    {row['status']}: {row['rx_kbps']:.1f} kbit/s{loss}, {row['rx_pkt_rate']:.1f} pkt/s",
                      flush=True)
            rows.append(row)
            with open(os.path.join(out_dir, "results.json"), "w") as f:
                json.dump(rows, f, indent=1)
    except KeyboardInterrupt:
        print("\ninterrupted; writing what ran so far")

    if rows:
        write_csv(os.path.join(out_dir, "results.csv"), rows)
        print(f"\ntx_only, {args.size}-byte packets.")
        print("nominal = the setting's PHY data rate; rx_kbps and pkt/s = received kbits and packets per second;")
        print("loss = % packets the sender's radio sent that never arrived;")
        print("sent/tx_err = packets the sender's radio sent/refused; crc = corrupted packets at the receiver;")
        print(f"offered = sender's rate; lossy = loss over {args.lossy_pct:g}%. All rates in kbit/s.\n")
        print(table(rows))
        print(f"\n>> {out_dir}/results.csv, {out_dir}/results.json, logs in {log_dir}/")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
