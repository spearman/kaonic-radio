#!/usr/bin/env python3
"""Throughput sweep for kaonic-iperf: every OFDM / QPSK setting.

Two tests (`--mode`), each run once per modulation setting with the same
`--modulation` on both sides:
- tx_only (default): the receiving side runs `kaonic-iperf --server --tx-only`
  and the sending side `kaonic-iperf --client --tx-only`. The sender streams
  packets back to back, the receiver counts them, and the receiver's measured
  rate is the result.
- round-trip: the receiving side echoes every packet (`--server`) and the
  sending side (`--client`) sends the next packet once the echo is back, or
  keeps `--window` packets in flight. The sender measures the round trips.
  `--mode both` runs each setting both ways.

Each side runs on the machine named by its endpoint, so one copy of this
script, normally on a dev machine, drives both boards:

    R="ssh -J me@jump root@board-b"
    python3 bench-throughput.py --tx-iperf ./kaonic-iperf --tx-addr 10.78.137.1 \\
        --rx "$R" --rx-iperf /usr/bin/kaonic-iperf --rx-addr 127.0.0.1 \\
        --rx-config /etc/kaonic-config.toml
    python3 bench-throughput.py ... --phys ofdm            # only OFDM
    python3 bench-throughput.py ... --phys qpsk            # only QPSK
    python3 bench-throughput.py ... --phys ofdm:6:1,qpsk:2000:4
    python3 bench-throughput.py ... --quick
    python3 bench-throughput.py ... --mode round-trip      # echoed, stop-and-wait
    python3 bench-throughput.py ... --mode both --quick
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
that at least `--min-packets` packets (round-trip: round trips) fit; a 2 KB
packet takes seconds at the slowest QPSK settings. The receiver stops after a
few packet times without packets. In round-trip mode, the sender's wait for
each echo (`--response-timeout-ms`) is set per setting to twice the expected
round trip (times the window), at least 200 ms, and the receiver waits that
much longer before stopping, since a lost packet holds the sender up for a
whole timeout. All of these come from a rough airtime model; `--dry-run`
shows them.

Settings the board rejects (kaonic-iperf reads the modulation back) show as
`error`. On a sub-GHz module, wide OFDM options may not fit the band plan.

Results go to `<out>/results.csv` and `<out>/results.json` (a `mode` column
tells the tests apart), logs of every run to `<out>/logs/`, and a table per
test of the settings, best throughput first, is printed at the end. Exits 0 if
every setting ran (whatever its throughput), 1 if any errored, 2 on bad arguments.
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

TX_ONLY, ROUND_TRIP = "tx-only", "round-trip"
# Round-trip mode: the receive-to-send delay at each end (commd, the control
# link and kaonic-iperf), added to the airtimes for the expected round trip.
TURNAROUND_S = 0.04
MIN_RESPONSE_TIMEOUT_MS = 200


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


class Timing:
    """How long one run takes and waits, from the airtime model."""

    def __init__(self, phy, mode, args):
        airtime = phy.airtime_s(args.size)
        # The receiver stops this long after its last packet...
        idle = max(2.0, 3 * airtime + 1)
        self.response_timeout_ms = None
        if mode == TX_ONLY:
            per_packet = airtime
        else:
            per_packet = 2 * airtime + TURNAROUND_S
            self.response_timeout_ms = max(
                MIN_RESPONSE_TIMEOUT_MS, math.ceil((2 * per_packet * args.window + 0.1) * 1000)
            )
            # ...but a lost packet leaves it waiting a whole response timeout.
            idle += self.response_timeout_ms / 1000
        self.duration = max(args.secs, math.ceil(args.min_packets * per_packet))
        self.idle = idle
        self.first_wait = STARTUP_S + idle

    def estimate_s(self):
        return self.duration + self.idle + 5


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


def tx_only_fields(t, r):
    """Row fields of a tx_only run: the receiver's measurement, loss against
    what the sender's radio took. Returns (fields, sent, received, nothing_sent)."""
    sent, received = t["sent"], r["received"]
    loss = max(0, sent - received) / sent * 100 if sent else None
    fields = {
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
    }
    return fields, loss, received, sent == 0 and t["tx_errors"] > 0


def round_trip_fields(t, r):
    """Row fields of a round-trip run: the sender's measurement. Its `sent`
    counts every packet, including ones its radio refused."""
    sent, received = t["sent"], t["received"]
    loss = max(0, sent - received) / sent * 100 if sent else None
    rnd = lambda v, n=2: None if v is None else round(v, n)
    fields = {
        "speed_kbps": round(t["speed_kbps"], 2),
        "loss_pct": rnd(loss),
        "rt_rate": round(t["round_trip_rate"], 2),
        "rtt_avg_ms": rnd(t["rtt_avg_ms"], 1),
        "rtt_max_ms": t["rtt_max_ms"],
        "sent": sent,
        "received": received,
        "timeouts": t["timeouts"],
        "crc_errors": t["crc_errors"],
        "tx_errors": t["tx_errors"],
        "window": t["window"],
        "echoed": r.get("echoed"),
    }
    return fields, loss, received, sent > 0 and t["tx_errors"] >= sent


def run_setting(phy, mode, args, tx, rx, log_dir):
    timing = Timing(phy, mode, args)
    row = {
        "mode": mode,
        "setting": str(phy),
        "spec": phy.spec(),
        "status": "error",
        "nominal_kbps": round(phy.kbps(), 2),
        "airtime_ms": round(phy.airtime_s(args.size) * 1000, 2),
        "duration_s": timing.duration,
        "size": args.size,
    }
    name = phy.spec().replace(":", "-") + ("-txonly" if mode == TX_ONLY else "-roundtrip")
    common = ["--modulation", phy.spec(), "--json"]
    client = ["--duration", str(timing.duration), "--payload-size", str(args.size)]
    # Always name the mode: kaonic-iperf otherwise takes it from [iperf] tx_only
    # in that side's config.
    if mode == TX_ONLY:
        common.append("--tx-only")
    else:
        common.append("--round-trip")
        row["response_timeout_ms"] = timing.response_timeout_ms
        client += ["--window", str(args.window), "--response-timeout-ms", str(timing.response_timeout_ms)]

    receiver = Proc(
        rx.argv(["--server"] + common
                + ["--idle-exit", f"{timing.idle:.1f}", "--first-packet-timeout", f"{timing.first_wait:.1f}"]),
        os.path.join(log_dir, f"{name}-rx.log"),
    )
    sender = None
    try:
        if not receiver.wait_line(READY_LINE, STARTUP_S):
            row["error"] = f"receiver didn't start: {receiver.why()}"
            return row

        sender = Proc(tx.argv(["--client"] + common + client), os.path.join(log_dir, f"{name}-tx.log"))
        # Round-trip senders drain their last packets, up to one response timeout.
        drain = (timing.response_timeout_ms or 0) / 1000
        if sender.wait(timing.duration + drain + STARTUP_S + SLACK_S) is None:
            row["error"] = "sender didn't finish"
            return row
        # The receiver stops by itself once packets stop arriving.
        if receiver.wait(timing.first_wait + SLACK_S) is None:
            row["error"] = "receiver didn't stop"
            return row

        t, r = sender.result, receiver.result
        if t is None:
            row["error"] = f"sender: {sender.why()}"
            return row
        if r is None:
            row["error"] = f"receiver: {receiver.why()}"
            return row

        want = mode == TX_ONLY
        for side, result in (("sender", t), ("receiver", r)):
            if result.get("tx_only") is not want:
                row["error"] = (f"{side} ran tx_only={result.get('tx_only')}, expected {want} "
                                "(is that side's kaonic-iperf older than this script?)")
                return row

        fields = tx_only_fields if mode == TX_ONLY else round_trip_fields
        try:
            values, loss, received, nothing_sent = fields(t, r)
        except (KeyError, TypeError) as e:
            row["error"] = f"unexpected RESULT ({type(e).__name__}: {e}); see the logs"
            return row
        row.update(values)
        if nothing_sent:
            row["status"] = "no tx"
        elif received == 0:
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


def num(r, key, fmt="{:.1f}"):
    v = r.get(key)
    return "" if v is None else fmt.format(v)


def tx_only_cells(r, has):
    return [
        r["setting"],
        r["status"],
        f"{r['nominal_kbps']:.1f}",
        num(r, "rx_kbps") if has else "",
        num(r, "loss_pct", "{:.1f}%"),
        num(r, "rx_pkt_rate") if has else "",
        str(r.get("sent", "")),
        str(r.get("received", "")),
        str(r.get("tx_errors", "")),
        str(r.get("crc_errors", "")),
        num(r, "offered_kbps"),
    ]


def round_trip_cells(r, has):
    return [
        r["setting"],
        r["status"],
        f"{r['nominal_kbps']:.1f}",
        num(r, "speed_kbps") if has else "",
        num(r, "loss_pct", "{:.1f}%"),
        num(r, "rt_rate") if has else "",
        num(r, "rtt_avg_ms") if has else "",
        num(r, "rtt_max_ms", "{}") if has else "",
        str(r.get("sent", "")),
        str(r.get("received", "")),
        str(r.get("timeouts", "")),
        str(r.get("crc_errors", "")),
        str(r.get("tx_errors", "")),
    ]


TABLES = {
    TX_ONLY: (["setting", "status", "nominal", "rx_kbps", "loss", "pkt/s", "sent", "received", "tx_err", "crc",
               "offered"], tx_only_cells, "rx_kbps"),
    ROUND_TRIP: (["setting", "status", "nominal", "speed", "loss", "rt/s", "rtt_avg", "rtt_max", "sent",
                  "received", "timeouts", "crc", "tx_err"], round_trip_cells, "speed_kbps"),
}


def table(rows, mode):
    """One row per setting, best throughput first; settings with nothing
    measured at the bottom, in run order."""
    head, row_cells, key = TABLES[mode]
    measured, other = [], []
    for r in rows:
        has = r["status"] in ("ok", "lossy")
        (measured if has else other).append((r.get(key) or 0, row_cells(r, has)))
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
    p.add_argument("--mode", choices=[TX_ONLY, ROUND_TRIP, "both"], default=TX_ONLY,
                   help="tx-only: stream, the receiver counts; round-trip: the receiver echoes, the sender "
                        "measures; both: each setting both ways (default: %(default)s)")
    p.add_argument("--window", type=int, default=1,
                   help="round-trip: packets in flight; above 1, packets and echoes collide on the one "
                        "half-duplex channel (default: %(default)s)")
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
    if args.window < 1:
        p.error("--window must be at least 1")
    modes = [TX_ONLY, ROUND_TRIP] if args.mode == "both" else [args.mode]
    runs = [(ph, mode) for ph in phys for mode in modes]

    est = sum(Timing(ph, mode, args).estimate_s() for ph, mode in runs)
    if args.dry_run:
        window = f", window {args.window}" if ROUND_TRIP in modes else ""
        print(f"{len(runs)} runs ({len(phys)} settings x {', '.join(modes)}), {args.size}-byte packets{window}, "
              f"about {est / 60:.0f} min:")
        for ph, mode in runs:
            t = Timing(ph, mode, args)
            timeout = f"  timeout={t.response_timeout_ms:6d} ms" if t.response_timeout_ms else ""
            print(f"  {str(ph):11s} {mode:10s} nominal={ph.kbps():7.1f} kbit/s"
                  f"  airtime={ph.airtime_s(args.size) * 1000:8.1f} ms  run={t.duration:4d} s"
                  f"  idle={t.idle:5.1f} s{timeout}")
        return 0

    out_dir = args.out or time.strftime("throughput-%Y%m%d-%H%M%S")
    log_dir = os.path.join(out_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    print(f">> {len(runs)} runs, about {est / 60:.0f} min; results in {out_dir}/", flush=True)
    rows, errors = [], 0
    try:
        for n, (ph, mode) in enumerate(runs, 1):
            t = Timing(ph, mode, args)
            print(f"[{n}/{len(runs)}] {ph} {mode} ({t.duration} s)", flush=True)
            row = run_setting(ph, mode, args, tx, rx, log_dir)
            status = row["status"]
            loss = "" if row.get("loss_pct") is None else f", loss {row['loss_pct']:.1f}%"
            if status == "error":
                errors += 1
                print(f"    error: {row['error']}", flush=True)
            elif mode == TX_ONLY:
                print(f"    {status}: {row['rx_kbps']:.1f} kbit/s{loss}, {row['rx_pkt_rate']:.1f} pkt/s", flush=True)
            else:
                rtt = "" if row.get("rtt_avg_ms") is None else f", rtt {row['rtt_avg_ms']:.1f} ms"
                print(f"    {status}: {row['speed_kbps']:.1f} kbit/s{loss}, {row['rt_rate']:.1f} round trips/s{rtt}",
                      flush=True)
            rows.append(row)
            with open(os.path.join(out_dir, "results.json"), "w") as f:
                json.dump(rows, f, indent=1)
    except KeyboardInterrupt:
        print("\ninterrupted; writing what ran so far")

    if rows:
        write_csv(os.path.join(out_dir, "results.csv"), rows)
        tx_only_rows = [r for r in rows if r["mode"] == TX_ONLY]
        round_trip_rows = [r for r in rows if r["mode"] == ROUND_TRIP]
        if tx_only_rows:
            print(f"\ntx_only, {args.size}-byte packets.")
            print("nominal = the setting's PHY data rate; rx_kbps and pkt/s = received kbits and packets per second;")
            print("loss = % packets the sender's radio sent that never arrived;")
            print("sent/tx_err = packets the sender's radio sent/refused; crc = corrupted packets at the receiver;")
            print(f"offered = sender's rate; lossy = loss over {args.lossy_pct:g}%; no tx = the sender's radio "
                  "took nothing. All rates in kbit/s.\n")
            print(table(tx_only_rows, TX_ONLY))
        if round_trip_rows:
            print(f"\nround-trip, {args.size}-byte packets, window {args.window}.")
            print("nominal = the setting's PHY data rate; speed = kbits per second, both directions;")
            print("rt/s = completed round trips per second; rtt_avg/rtt_max in ms;")
            print("loss = % packets sent that got no echo back (timeouts + CRC errors + TX errors);")
            print("tx_err = packets the sender's radio refused; crc = corrupted echoes at the sender;")
            print(f"lossy = loss over {args.lossy_pct:g}%; no tx = the sender's radio took nothing. "
                  "All rates in kbit/s.\n")
            print(table(round_trip_rows, ROUND_TRIP))
        print(f"\n>> {out_dir}/results.csv, {out_dir}/results.json, logs in {log_dir}/")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
