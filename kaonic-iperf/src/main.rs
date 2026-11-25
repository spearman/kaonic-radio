use clap::Parser;
use log::{error, warn};
use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::{sync::broadcast, time::timeout_at};
use tokio_util::sync::CancellationToken;

use kaonic_ctrl::{client::Client, protocol::MessageCoder, radio::RadioClient};
use kaonic_frame::frame::Frame;
use radio_common::Modulation;

mod config;

const DEFAULT_COMMD_ADDR: &str = "192.168.10.1:9090";
const MIN_PACKET_SIZE: usize = 24; // MAGIC(4) + SEQ(4) + TIMESTAMP(8) + padding(4) + CRC(4)
const MAX_PACKET_SIZE: usize = 2048;

#[derive(Parser, Debug)]
#[command(name = "kaonic-iperf")]
#[command(about = "Simple RTT and throughput measurement for Kaonic radio")]
struct Args {
    /// Path to kaonic-config.toml
    #[arg(long, short = 'c', default_value = "kaonic-config.toml")]
    config: String,

    /// kaonic-commd address (overrides config file)
    #[arg(long, short = 'a')]
    address: Option<String>,

    /// Run as server (responder)
    #[arg(long, conflicts_with = "client")]
    server: bool,

    /// Run as client (initiator)
    #[arg(long, conflicts_with = "server")]
    client: bool,

    /// Modulation for the iperf module, overriding the config's preset:
    /// ofdm:<mcs 0-6>:<opt 1-4> or qpsk:<kchip 100|200|1000|2000>:<mode 0-4>
    #[arg(long)]
    modulation: Option<String>,

    /// One-way test (overrides [iperf] tx_only)
    #[arg(long)]
    tx_only: bool,

    /// Test duration in seconds (overrides [iperf] duration)
    #[arg(long)]
    duration: Option<u64>,

    /// Packet size in bytes (overrides [iperf] payload_size)
    #[arg(long)]
    payload_size: Option<usize>,

    /// Server: exit after this many seconds without an iperf packet
    #[arg(long, value_name = "SECS")]
    idle_exit: Option<f64>,

    /// Server with --idle-exit: how long to wait for the first packet,
    /// counted from when the radio is configured (default: the idle time)
    #[arg(long, value_name = "SECS", requires = "idle_exit")]
    first_packet_timeout: Option<f64>,

    /// Print a final machine-readable `RESULT {json}` line
    #[arg(long)]
    json: bool,
}

/// Run options that only come from the command line.
struct Options {
    idle_exit: Option<Duration>,
    first_packet_timeout: Option<Duration>,
    json: bool,
}

/// A positive number of seconds from the command line, or exit with an error.
fn secs_arg(name: &str, secs: Option<f64>) -> Option<Duration> {
    match secs {
        Some(s) if s.is_finite() && s > 0.0 => Some(Duration::from_secs_f64(s)),
        Some(_) => {
            eprintln!("Error: {} must be a positive number of seconds", name);
            std::process::exit(2);
        }
        None => None,
    }
}

/// Sets the module's modulation and reads it back. commd replies OK even
/// when the radio rejects a modulation, but only reports one it applied.
async fn apply_modulation(
    radio_client: &mut RadioClient,
    module: usize,
    modulation: Modulation,
) -> Result<(), String> {
    radio_client
        .set_modulation(module, modulation)
        .await
        .map_err(|e| format!("Modulation error: {:?}", e))?;

    let applied = radio_client
        .get_modulation(module)
        .await
        .map_err(|e| format!("Modulation readback error: {:?}", e))?;

    if format!("{:?}", applied) != format!("{:?}", modulation) {
        return Err(format!(
            "the board did not apply modulation {:?} (it reports {:?})",
            modulation, applied
        ));
    }

    Ok(())
}

// Packet structure:
// MAGIC (4) + SEQ (4) + TIMESTAMP (8) + PADDING (N) + CRC32 (4)
// Minimum size: 24 bytes
const MAGIC: [u8; 4] = [0x8B, 0x52, 0x54, 0x54];

fn compute_crc(data: &[u8]) -> u32 {
    kaonic_crc::crc32(data)
}

fn per_sec(n: u64, secs: f64) -> f64 {
    if secs > 0.0 {
        n as f64 / secs
    } else {
        0.0
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn fill_packet(frame: &mut Frame<2048>, seq: u32, size: usize) {
    let size = size.clamp(MIN_PACKET_SIZE, MAX_PACKET_SIZE);
    frame.clear();

    let buffer = frame.alloc_buffer(size).expect("Frame too small");
    let mut pos = 0;

    // Header: MAGIC + SEQ + TIMESTAMP (16 bytes)
    buffer[pos..pos + 4].copy_from_slice(&MAGIC);
    pos += 4;
    buffer[pos..pos + 4].copy_from_slice(&seq.to_le_bytes());
    pos += 4;
    buffer[pos..pos + 8].copy_from_slice(&now_ms().to_le_bytes());
    pos += 8;

    // Padding (fill to size - 4 bytes for CRC)
    while pos < size - 4 {
        buffer[pos] = (pos & 0xFF) as u8;
        pos += 1;
    }

    // CRC32 of everything before it
    let crc = compute_crc(&buffer[..pos]);
    buffer[pos..pos + 4].copy_from_slice(&crc.to_le_bytes());
}

#[derive(Debug)]
enum ParseError {
    TooShort,
    BadMagic,
    CrcMismatch { expected: u32, actual: u32 },
}

/// Returns (seq, timestamp) if packet is valid
fn parse_packet(data: &[u8]) -> Result<(u32, u64), ParseError> {
    if data.len() < MIN_PACKET_SIZE {
        return Err(ParseError::TooShort);
    }

    // Check magic
    if data[0..4] != MAGIC {
        return Err(ParseError::BadMagic);
    }

    // Verify CRC (last 4 bytes)
    let payload_end = data.len() - 4;
    let expected_crc = u32::from_le_bytes([
        data[payload_end],
        data[payload_end + 1],
        data[payload_end + 2],
        data[payload_end + 3],
    ]);
    let actual_crc = compute_crc(&data[..payload_end]);

    if expected_crc != actual_crc {
        return Err(ParseError::CrcMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    // Parse header
    let seq = u32::from_le_bytes([data[4], data[5], data[6], data[7]]);
    let timestamp = u64::from_le_bytes([
        data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
    ]);

    Ok((seq, timestamp))
}

async fn run_server(
    address: &str,
    cfg: &config::Config,
    opts: &Options,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Kaonic RTT Server ===");
    println!("Connecting to {}...", address);

    let server_addr: std::net::SocketAddr = address.parse()?;
    let listen_addr: std::net::SocketAddr = "0.0.0.0:0".parse()?;

    let cancel = CancellationToken::new();

    let client = Client::connect(
        listen_addr,
        server_addr,
        MessageCoder::<1400, 5>::new(),
        cancel.clone(),
    )
    .await
    .map_err(|e| format!("Client connect error: {:?}", e))?;

    let mut radio_client = RadioClient::new(client, cancel.clone())
        .await
        .map_err(|e| format!("RadioClient init error: {:?}", e))?;
    println!("Connected.");

    // Apply radio configuration for the target module only
    if let Some(radio_cfg) = cfg.radios.iter().find(|r| r.module == cfg.iperf.module) {
        println!("Configuring radio module {}...", cfg.iperf.module);
        radio_client
            .set_radio_config(cfg.iperf.module, radio_cfg.config.clone())
            .await
            .map_err(|e| format!("Config error: {:?}", e))?;

        if let Some(modulation) = radio_cfg.modulation {
            apply_modulation(&mut radio_client, cfg.iperf.module, modulation).await?;
            println!("Modulation configured: {:?}", modulation);
        }

        println!("Radio configuration applied.\n");
    } else {
        println!(
            "Warning: no radio config found for module {}\n",
            cfg.iperf.module
        );
    }

    let mut module_rx = radio_client.module_receive();
    let mut count: u64 = 0;
    let mut ignored: u64 = 0;
    let mut crc_errors: u64 = 0;
    let mut bytes_received: u64 = 0;
    let mut rx_packets: u64 = 0;
    let mut start_time: Option<Instant> = None;
    let mut last_rx_time: Option<Instant> = None;

    // One-way (tx_only) mode: seqs seen, to count loss and duplicates
    let tx_only = cfg.iperf.tx_only;
    let mut seen_seqs: HashSet<u32> = HashSet::new();
    let mut max_seq: Option<u32> = None;
    let mut duplicates: u64 = 0;

    if tx_only {
        println!("Mode: one-way (tx_only), not echoing\n");
    }

    // With --idle-exit, stop once no iperf packet has arrived for that long.
    // Before the first packet, wait --first-packet-timeout instead (the
    // client may take a while to start).
    let mut idle_deadline = opts
        .idle_exit
        .map(|idle| tokio::time::Instant::now() + opts.first_packet_timeout.unwrap_or(idle));

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    println!("Listening...");

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                println!("\nShutting down...");
                break;
            }
            _ = async { tokio::time::sleep_until(idle_deadline.unwrap()).await }, if idle_deadline.is_some() => {
                if start_time.is_some() {
                    println!(
                        "\nNo iperf packets for {:.1} s, stopping",
                        opts.idle_exit.unwrap_or_default().as_secs_f64()
                    );
                } else {
                    println!("\nNo iperf packet arrived, stopping");
                }
                break;
            }
            result = module_rx.recv() => {
                match result {
                    Ok(rx_module) => {
                        if rx_module.module != cfg.iperf.module {
                            continue;
                        }

                        let rx_data = rx_module.frame.as_slice();

                        match parse_packet(rx_data) {
                            Ok((seq, _ts)) => {
                                // Track receive stats
                                let packet_size = rx_data.len() as u64;
                                if !tx_only {
                                    println!("[RX] seq={} size={} bytes", seq, packet_size);
                                }
                                let now = Instant::now();
                                if start_time.is_none() {
                                    start_time = Some(now);
                                }
                                last_rx_time = Some(now);
                                if let Some(idle) = opts.idle_exit {
                                    idle_deadline = Some(tokio::time::Instant::now() + idle);
                                }
                                bytes_received += packet_size;
                                rx_packets += 1;

                                // Calculate current receive speed and packet rate
                                let elapsed = start_time
                                    .map(|t| t.elapsed().as_secs_f64())
                                    .unwrap_or(0.0);
                                let speed_kbps = per_sec(bytes_received * 8, elapsed) / 1000.0;
                                let pkt_rate = per_sec(rx_packets, elapsed);

                                if tx_only {
                                    count += 1;
                                    if !seen_seqs.insert(seq) {
                                        duplicates += 1;
                                    }
                                    max_seq = Some(max_seq.map_or(seq, |m| m.max(seq)));
                                    println!(
                                        "[{}] RX seq={} size={}  rx={:.2} kb/s  {:.1} pkt/s",
                                        count, seq, rx_data.len(), speed_kbps, pkt_rate
                                    );
                                    continue;
                                }

                                // Echo back the same packet
                                let mut echo_frame = Frame::<2048>::new();
                                echo_frame.copy_from_slice(rx_data);

                                match radio_client.transmit(cfg.iperf.module, &echo_frame).await {
                                    Ok(_) => {
                                        count += 1;
                                        println!(
                                            "[{}] Echo seq={} size={}  rx={:.2} kb/s  {:.1} pkt/s",
                                            count, seq, rx_data.len(), speed_kbps, pkt_rate
                                        );
                                    }
                                    Err(e) => warn!("Transmit error: {:?}", e),
                                }
                            }
                            Err(ParseError::TooShort) => {
                                ignored += 1;
                            }
                            Err(ParseError::BadMagic) => {
                                ignored += 1;
                            }
                            Err(ParseError::CrcMismatch { expected, actual }) => {
                                crc_errors += 1;
                                warn!(
                                    "CRC mismatch: expected={:#010x} actual={:#010x} size={}",
                                    expected, actual, rx_data.len()
                                );
                            }
                        }
                    }
                    Err(_) => {
                        // Channel closed or lagged
                        break;
                    }
                }
            }
        }
    }

    radio_client.cancel();

    if !tx_only {
        println!("\nTotal packets echoed: {}", count);
    } else if let Some(max_seq) = max_seq {
        // The client numbers packets from 0, so 0..=max_seq should all have arrived
        let expected = u64::from(max_seq) + 1;
        let unique = seen_seqs.len() as u64;
        let loss = (expected.saturating_sub(unique)) as f64 / expected as f64 * 100.0;
        println!(
            "\nPackets received: {} of {} expected (seq 0..={}), loss {:.1}%",
            unique, expected, max_seq, loss
        );
        println!(
            "  (packets lost after seq {} aren't counted; compare with the client's last seq)",
            max_seq
        );
        if duplicates > 0 {
            println!("Duplicates: {}", duplicates);
        }
    } else {
        println!("\nNo iperf packets received");
    }
    if ignored > 0 {
        println!("Ignored (non-iperf): {}", ignored);
    }
    if crc_errors > 0 {
        println!("CRC errors: {}", crc_errors);
    }
    if let (Some(start), Some(last)) = (start_time, last_rx_time) {
        let elapsed = last.duration_since(start).as_secs_f64();
        if elapsed > 0.0 {
            let avg_speed_kbps = (bytes_received as f64 * 8.0) / elapsed / 1000.0;
            println!("Bytes received: {}", bytes_received);
            println!("Avg receive speed: {:.2} kb/s", avg_speed_kbps);
            println!(
                "Avg receive rate: {:.1} pkt/s",
                per_sec(rx_packets, elapsed)
            );
        }
    }

    if opts.json {
        let span_s = match (start_time, last_rx_time) {
            (Some(start), Some(last)) => last.duration_since(start).as_secs_f64(),
            _ => 0.0,
        };
        let result = serde_json::json!({
            "role": "server",
            "tx_only": tx_only,
            "received": if tx_only { seen_seqs.len() as u64 } else { rx_packets },
            "rx_packets": rx_packets,
            "echoed": if tx_only { None } else { Some(count) },
            "duplicates": duplicates,
            "max_seq": max_seq,
            "crc_errors": crc_errors,
            "ignored": ignored,
            "bytes": bytes_received,
            "span_s": span_s,
            "rx_kbps": per_sec(bytes_received * 8, span_s) / 1000.0,
            "rx_pkt_rate": per_sec(rx_packets, span_s),
        });
        println!("RESULT {}", result);
    }
    Ok(())
}

/// One-way test: send packets back-to-back for the test duration without waiting for
/// echoes.
async fn run_client_tx_only(
    radio_client: &mut RadioClient,
    cfg: &config::Config,
    packet_size: usize,
    opts: &Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let test_duration = Duration::from_secs(cfg.iperf.duration);
    let progress_interval = Duration::from_secs(1);
    let mut next_progress = progress_interval;

    let mut seq: u32 = 0;
    let mut sent: u64 = 0;
    let mut tx_errors: u64 = 0;

    let mut tx_frame = Frame::<2048>::new();

    let offered_kbps = |sent: u64, elapsed: f64| {
        if elapsed > 0.0 {
            (sent * packet_size as u64) as f64 * 8.0 / elapsed / 1000.0
        } else {
            0.0
        }
    };

    while start.elapsed() < test_duration {
        fill_packet(&mut tx_frame, seq, packet_size);

        match radio_client.transmit(cfg.iperf.module, &tx_frame).await {
            Ok(_) => sent += 1,
            Err(e) => {
                error!("seq={} transmit error: {:?}", seq, e);
                tx_errors += 1;
            }
        }

        // Seq advances even on error so the server sees the gap
        seq = seq.wrapping_add(1);

        let elapsed = start.elapsed();
        if elapsed >= next_progress {
            let secs = elapsed.as_secs_f64();
            println!(
                "[{:>5.1} s] sent={}  tx_errors={}  offered={:.1} kb/s  {:.1} pkt/s",
                secs,
                sent,
                tx_errors,
                offered_kbps(sent, secs),
                per_sec(sent, secs)
            );
            next_progress += progress_interval;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();

    println!("\n=== Results (one-way) ===");
    println!("Duration:     {:.2} s", elapsed);
    println!("Packet size:  {} bytes", packet_size);
    println!("Packets:      {} sent, {} TX errors", sent, tx_errors);
    if seq > 0 {
        println!("Last seq:     {}", seq - 1);
    }
    println!("Offered:      {:.2} kb/s", offered_kbps(sent, elapsed));
    println!("Packet rate:  {:.1} pkt/s", per_sec(sent, elapsed));

    if opts.json {
        let result = serde_json::json!({
            "role": "client",
            "tx_only": true,
            "packet_size": packet_size,
            "sent": sent,
            "tx_errors": tx_errors,
            "last_seq": seq.checked_sub(1),
            "elapsed_s": elapsed,
            "offered_kbps": offered_kbps(sent, elapsed),
            "pkt_rate": per_sec(sent, elapsed),
        });
        println!("RESULT {}", result);
    }

    Ok(())
}

async fn run_client(
    address: &str,
    cfg: &config::Config,
    opts: &Options,
) -> Result<(), Box<dyn std::error::Error>> {
    let packet_size = cfg
        .iperf
        .payload_size
        .clamp(MIN_PACKET_SIZE, MAX_PACKET_SIZE);

    println!("=== Kaonic RTT Client ===");
    println!("Connecting to {}...", address);

    let server_addr: std::net::SocketAddr = address.parse()?;
    let listen_addr: std::net::SocketAddr = "0.0.0.0:0".parse()?;

    let cancel = CancellationToken::new();

    let client = Client::connect(
        listen_addr,
        server_addr,
        MessageCoder::<1400, 5>::new(),
        cancel.clone(),
    )
    .await
    .map_err(|e| format!("Client connect error: {:?}", e))?;

    let mut radio_client = RadioClient::new(client, cancel.clone())
        .await
        .map_err(|e| format!("RadioClient init error: {:?}", e))?;
    println!("Connected.");

    // Apply radio configuration for the target module only
    if let Some(radio_cfg) = cfg.radios.iter().find(|r| r.module == cfg.iperf.module) {
        println!("Configuring radio module {}...", cfg.iperf.module);
        radio_client
            .set_radio_config(cfg.iperf.module, radio_cfg.config.clone())
            .await
            .map_err(|e| format!("Config error: {:?}", e))?;

        if let Some(modulation) = radio_cfg.modulation {
            apply_modulation(&mut radio_client, cfg.iperf.module, modulation).await?;
            println!("Modulation configured: {:?}", modulation);
        }

        println!("Radio configuration applied.");
    } else {
        println!(
            "Warning: no radio config found for module {}",
            cfg.iperf.module
        );
    }

    if cfg.iperf.tx_only {
        println!("Mode: one-way (tx_only)");
        println!("Packet size: {} bytes", packet_size);
        println!("Duration: {} seconds\n", cfg.iperf.duration);

        let result = run_client_tx_only(&mut radio_client, cfg, packet_size, opts).await;
        radio_client.cancel();
        return result;
    }

    let window = cfg.iperf.window.max(1);

    println!("Packet size: {} bytes", packet_size);
    println!("Window: {} packet(s)", window);
    println!("Response timeout: {} ms", cfg.iperf.response_timeout_ms);
    println!("Duration: {} seconds\n", cfg.iperf.duration);

    // Start receive stream
    let mut module_rx = radio_client.module_receive();

    let start = Instant::now();
    let test_duration = Duration::from_secs(cfg.iperf.duration);
    let response_timeout = Duration::from_millis(cfg.iperf.response_timeout_ms);
    let mut seq: u32 = 0;
    let mut rtt_min: u64 = u64::MAX;
    let mut rtt_max: u64 = 0;
    let mut rtt_sum: u64 = 0;
    let mut rtt_count: u64 = 0;
    let mut bytes_transferred: u64 = 0;
    let mut timeouts: u64 = 0;
    let mut crc_errors: u64 = 0;
    let mut tx_errors: u64 = 0;

    // Packets awaiting their echo, oldest first: (seq, send time)
    let mut in_flight: VecDeque<(u32, Instant)> = VecDeque::with_capacity(window);

    // Pre-allocate reusable packet frame
    let mut tx_frame = Frame::<2048>::new();

    // Keep up to `window` packets in flight until the test duration is over,
    // then drain the remaining echoes. Each packet has its own deadline;
    // frames that are not an in-flight echo (other module, late echoes,
    // non-iperf traffic) are skipped without re-sending or extending it.
    loop {
        while in_flight.len() < window && start.elapsed() < test_duration {
            fill_packet(&mut tx_frame, seq, packet_size);
            let send_time = Instant::now();

            match radio_client.transmit(cfg.iperf.module, &tx_frame).await {
                Ok(_) => in_flight.push_back((seq, send_time)),
                Err(e) => {
                    error!("Transmit error: {:?}", e);
                    tx_errors += 1;
                }
            }

            seq = seq.wrapping_add(1);
        }

        // Test over and every packet accounted for
        let Some(&(_, oldest_send_time)) = in_flight.front() else {
            break;
        };
        let deadline = oldest_send_time + response_timeout;

        match timeout_at(deadline.into(), module_rx.recv()).await {
            Ok(Ok(rx_module)) => {
                if rx_module.module != cfg.iperf.module {
                    continue;
                }

                let rx_data = rx_module.frame.as_slice();

                match parse_packet(rx_data) {
                    Ok((resp_seq, _)) => {
                        // Echoes of packets no longer in flight (already timed
                        // out, or duplicates) are ignored
                        if let Some(pos) = in_flight.iter().position(|&(s, _)| s == resp_seq) {
                            let (_, send_time) = in_flight.remove(pos).unwrap();
                            let rtt = send_time.elapsed().as_millis() as u64;

                            rtt_min = rtt_min.min(rtt);
                            rtt_max = rtt_max.max(rtt);
                            rtt_sum += rtt;
                            rtt_count += 1;
                            bytes_transferred += (packet_size * 2) as u64; // req + resp

                            println!(
                                "seq={:<6} rtt={:<4} ms  size={}",
                                resp_seq,
                                rtt,
                                rx_data.len()
                            );
                        }
                    }
                    Err(ParseError::CrcMismatch { expected, actual }) => {
                        // The seq in a corrupted packet can't be trusted. Echoes
                        // come back in order, so charge it to the oldest packet.
                        if let Some((lost_seq, _)) = in_flight.pop_front() {
                            crc_errors += 1;
                            println!(
                                "seq={:<6} CRC ERROR (expected={:#010x} actual={:#010x})",
                                lost_seq, expected, actual
                            );
                        }
                    }
                    Err(_) => {
                        // Ignore non-iperf packets
                    }
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(skipped))) => {
                warn!("receive stream lagged by {} frames", skipped);
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                error!("receive stream closed, stopping test");
                timeouts += in_flight.len() as u64;
                in_flight.clear();
                break;
            }
            Err(_) => {
                // Expire every packet whose deadline has passed
                let now = Instant::now();
                while let Some(&(lost_seq, send_time)) = in_flight.front() {
                    if now < send_time + response_timeout {
                        break;
                    }
                    in_flight.pop_front();
                    println!("seq={:<6} TIMEOUT", lost_seq);
                    timeouts += 1;
                }
            }
        }
    }

    radio_client.cancel();

    // Print results
    let elapsed = start.elapsed().as_secs_f64();
    let packets_sent = seq as u64;

    println!("\n=== Results ===");
    println!("Duration:     {:.2} s", elapsed);
    println!("Packet size:  {} bytes", packet_size);
    println!(
        "Packets:      {} sent, {} received, {} timeouts, {} CRC errors",
        packets_sent, rtt_count, timeouts, crc_errors
    );
    if tx_errors > 0 {
        println!("TX errors:    {}", tx_errors);
    }

    if rtt_count > 0 {
        let avg_rtt = rtt_sum as f64 / rtt_count as f64;

        println!(
            "RTT:          min={} ms, avg={:.1} ms, max={} ms",
            rtt_min, avg_rtt, rtt_max
        );
    }

    if elapsed > 0.0 {
        let speed_kbps = (bytes_transferred as f64 * 8.0) / elapsed / 1000.0;
        println!("Speed:        {:.2} kb/s", speed_kbps);
        println!(
            "Rate:         {:.1} round trips/s ({:.1} pkt/s sent)",
            per_sec(rtt_count, elapsed),
            per_sec(packets_sent, elapsed)
        );
    }

    if packets_sent > 0 {
        let loss = ((packets_sent - rtt_count) as f64 / packets_sent as f64) * 100.0;
        println!("Packet loss:  {:.1}%", loss);
    }

    if opts.json {
        let result = serde_json::json!({
            "role": "client",
            "tx_only": false,
            "packet_size": packet_size,
            "window": window,
            "sent": packets_sent,
            "received": rtt_count,
            "timeouts": timeouts,
            "crc_errors": crc_errors,
            "tx_errors": tx_errors,
            "rtt_min_ms": (rtt_count > 0).then_some(rtt_min),
            "rtt_avg_ms": (rtt_count > 0).then(|| rtt_sum as f64 / rtt_count as f64),
            "rtt_max_ms": (rtt_count > 0).then_some(rtt_max),
            "elapsed_s": elapsed,
            "speed_kbps": per_sec(bytes_transferred * 8, elapsed) / 1000.0,
            "round_trip_rate": per_sec(rtt_count, elapsed),
        });
        println!("RESULT {}", result);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .parse_default_env()
        .init();

    let args = Args::parse();

    // Load config from specified path (required)
    let mut cfg = match config::load_config(&args.config) {
        Ok(c) => {
            println!(
                "Loaded config from {} with {} radio(s)",
                args.config,
                c.radios.len()
            );
            c
        }
        Err(e) => {
            eprintln!("Error: could not load config file '{}': {}", args.config, e);
            std::process::exit(1);
        }
    };

    if !args.server && !args.client {
        eprintln!("Error: specify --server or --client");
        std::process::exit(1);
    }

    // Command-line overrides of the [iperf] section
    if args.tx_only {
        cfg.iperf.tx_only = true;
    }
    if let Some(duration) = args.duration {
        cfg.iperf.duration = duration;
    }
    if let Some(size) = args.payload_size {
        cfg.iperf.payload_size = size;
    }
    if let Some(spec) = &args.modulation {
        match cfg.override_modulation(spec) {
            Ok(modulation) => println!("Modulation from command line: {:?}", modulation),
            Err(e) => {
                eprintln!("Error: {}", e);
                std::process::exit(2);
            }
        }
    }

    let opts = Options {
        idle_exit: secs_arg("--idle-exit", args.idle_exit),
        first_packet_timeout: secs_arg("--first-packet-timeout", args.first_packet_timeout),
        json: args.json,
    };

    // Determine address: CLI arg takes precedence over config
    let address = if let Some(addr) = &args.address {
        // Parse as IP:PORT or add default port
        if addr.contains(':') {
            addr.clone()
        } else {
            format!("{}:9090", addr)
        }
    } else if let Some(ip) = &cfg.iperf.ip {
        format!("{}:9090", ip)
    } else {
        DEFAULT_COMMD_ADDR.to_string()
    };

    if args.server {
        run_server(&address, &cfg, &opts).await?;
    } else {
        run_client(&address, &cfg, &opts).await?;
    }

    Ok(())
}
