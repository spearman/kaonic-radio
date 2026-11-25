use radio_common::{
    modulation::{
        OfdmBandwidthOption, OfdmMcs, OfdmModulation, QpskChipFrequency, QpskModulation,
        QpskRateMode,
    },
    Hertz, Modulation, RadioConfig, RadioConfigBuilder,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs;

#[derive(Debug)]
pub struct IperfConfig {
    pub duration: u64,
    pub payload_size: usize,
    pub timeout: u64,
    pub ip: Option<String>,
    pub module: usize,
    /// Packets in flight in client round-trip mode (1 = stop-and-wait).
    pub window: usize,
    /// One-way test: the client streams packets and the server only counts them.
    pub tx_only: bool,
    /// Round-trip mode: how long to wait for each echo before counting it lost.
    pub response_timeout_ms: u64,
}

impl Default for IperfConfig {
    fn default() -> Self {
        IperfConfig {
            duration: 10,
            payload_size: 2047,
            timeout: 10,
            ip: None,
            module: 0,
            window: 1,
            tx_only: false,
            response_timeout_ms: 200,
        }
    }
}

#[derive(Debug, Default)]
pub struct Config {
    pub radios: Vec<RadioConfigWithModule>,
    pub iperf: IperfConfig,
    pub modulation: HashMap<String, toml::Value>,
}

#[derive(Debug, Clone)]
pub struct RadioConfigWithModule {
    pub module: usize,
    pub config: RadioConfig,
    pub modulation: Option<Modulation>,
}

#[derive(Deserialize)]
struct IperfPartial {
    duration: Option<u64>,
    payload_size: Option<usize>,
    timeout: Option<u64>,
    ip: Option<String>,
    module: Option<i64>,
    window: Option<usize>,
    tx_only: Option<bool>,
    response_timeout_ms: Option<u64>,
}

/// Loads configuration from the given TOML file path and maps radio-* sections to protobufs.
pub fn load_config(path: &str) -> Result<Config, Box<dyn Error>> {
    let s = fs::read_to_string(path)?;
    let val: toml::Value = toml::from_str(&s)?;
    let table = val.as_table().ok_or("config is not a table")?;

    // Modulation presets (parse first so presets can be applied while creating radios)
    let mut modulation: HashMap<String, toml::Value> = HashMap::new();
    if let Some(mv) = table.get("modulation").and_then(|v| v.as_table()) {
        for (k, v) in mv.iter() {
            modulation.insert(k.clone(), v.clone());
        }
    }

    // Collect radio keys in sorted order for deterministic mapping
    let mut radio_keys: Vec<String> = table
        .keys()
        .filter(|k| k.starts_with("radio-"))
        .cloned()
        .collect();
    radio_keys.sort();

    let mut radios: Vec<RadioConfigWithModule> = Vec::new();
    for (i, key) in radio_keys.iter().enumerate() {
        if let Some(v) = table.get(key) {
            if let Some(rtab) = v.as_table() {
                let mut builder = RadioConfigBuilder::new();

                if let Some(freq) = rtab.get("freq").and_then(|x| x.as_integer()) {
                    builder = builder.freq(Hertz::new(freq as u64));
                }
                if let Some(ch) = rtab.get("channel").and_then(|x| x.as_integer()) {
                    builder = builder.channel(ch as u16);
                }
                if let Some(cs) = rtab.get("channel_spacing").and_then(|x| x.as_integer()) {
                    builder = builder.channel_spacing(Hertz::new(cs as u64));
                }

                let config = builder.build();

                // Parse modulation from preset if specified
                let mod_config =
                    if let Some(mod_name) = rtab.get("modulation").and_then(|x| x.as_str()) {
                        parse_modulation(&modulation, mod_name)
                    } else {
                        None
                    };

                radios.push(RadioConfigWithModule {
                    module: i,
                    config,
                    modulation: mod_config,
                });
            }
        }
    }

    // Parse iperf section
    let iperf = if let Some(v) = table.get("iperf") {
        let mut d = IperfConfig::default();
        if let Ok(partial) = v.clone().try_into::<IperfPartial>() {
            if let Some(x) = partial.duration {
                d.duration = x;
            }
            if let Some(x) = partial.payload_size {
                d.payload_size = x;
            }
            if let Some(x) = partial.timeout {
                d.timeout = x;
            }
            if let Some(x) = partial.ip {
                d.ip = Some(x);
            }
            if let Some(m) = partial.module {
                d.module = m as usize;
            }
            if let Some(w) = partial.window {
                d.window = w.max(1);
            }
            if let Some(b) = partial.tx_only {
                d.tx_only = b;
            }
            if let Some(ms) = partial.response_timeout_ms {
                d.response_timeout_ms = ms.max(1);
            }
        }
        d
    } else {
        IperfConfig::default()
    };

    // Modulation presets
    let mut modulation: HashMap<String, toml::Value> = HashMap::new();
    if let Some(mv) = table.get("modulation").and_then(|v| v.as_table()) {
        for (k, v) in mv.iter() {
            modulation.insert(k.clone(), v.clone());
        }
    }

    Ok(Config {
        radios,
        iperf,
        modulation,
    })
}

fn parse_modulation(presets: &HashMap<String, toml::Value>, name: &str) -> Option<Modulation> {
    let preset = presets.get(name)?;
    let mod_type = preset.get("type")?.as_str()?;

    match mod_type {
        "ofdm" => {
            let mcs_val = preset.get("mcs")?.as_integer()? as u8;
            let opt_val = preset.get("opt")?.as_integer()? as u8;
            let tx_power = preset
                .get("tx_power")
                .and_then(|v| v.as_integer())
                .unwrap_or(10) as u8;

            let mcs = match mcs_val {
                0 => OfdmMcs::BpskC1_2_4x,
                1 => OfdmMcs::BpskC1_2_2x,
                2 => OfdmMcs::QpskC1_2_2x,
                3 => OfdmMcs::QpskC1_2,
                4 => OfdmMcs::QpskC3_4,
                5 => OfdmMcs::QamC1_2,
                6 => OfdmMcs::QamC3_4,
                _ => return None,
            };

            let opt = match opt_val {
                0 => OfdmBandwidthOption::Option1,
                1 => OfdmBandwidthOption::Option2,
                2 => OfdmBandwidthOption::Option3,
                3 => OfdmBandwidthOption::Option4,
                _ => return None,
            };

            Some(Modulation::Ofdm(OfdmModulation {
                mcs,
                opt,
                tx_power,
            }))
        }
        "qpsk" => {
            let chip_val = preset.get("chip_freq")?.as_integer()? as u32;
            let rate_val = preset.get("rate")?.as_integer()? as u8;
            let tx_power = preset
                .get("tx_power")
                .and_then(|v| v.as_integer())
                .unwrap_or(10) as u8;

            let fchip = match chip_val {
                100 => QpskChipFrequency::Fchip100,
                200 => QpskChipFrequency::Fchip200,
                1000 => QpskChipFrequency::Fchip1000,
                2000 => QpskChipFrequency::Fchip2000,
                _ => return None,
            };

            let mode = match rate_val {
                0 => QpskRateMode::RateMode0,
                1 => QpskRateMode::RateMode1,
                2 => QpskRateMode::RateMode2,
                3 => QpskRateMode::RateMode3,
                4 => QpskRateMode::RateMode4,
                _ => return None,
            };

            Some(Modulation::Qpsk(QpskModulation {
                fchip,
                mode,
                tx_power,
            }))
        }
        _ => None,
    }
}

/// Parses a modulation spec from the command line:
/// - `ofdm:<mcs>:<opt>`: MCS 0-6, bandwidth option 1-4 (datasheet numbering,
///   so option 1 is the widest). Option 3 needs MCS >= 1 and option 4 MCS >= 2.
/// - `qpsk:<kchip>:<mode>`: chip rate 100/200/1000/2000 kchip/s, rate mode
///   0-4. Rate mode 4 exists only at 2000 kchip/s.
pub fn parse_modulation_spec(spec: &str, tx_power: u8) -> Result<Modulation, String> {
    let bad = || {
        format!(
            "bad modulation {spec:?}: expected ofdm:<mcs 0-6>:<opt 1-4> or qpsk:<kchip>:<mode 0-4>"
        )
    };
    let parts: Vec<&str> = spec.split(':').collect();
    let [kind, a, b] = parts[..] else {
        return Err(bad());
    };
    let a: u32 = a.parse().map_err(|_| bad())?;
    let b: u32 = b.parse().map_err(|_| bad())?;

    match kind {
        "ofdm" => {
            let (mcs, opt) = (a, b);
            let min_mcs = match opt {
                1 | 2 => 0,
                3 => 1,
                4 => 2,
                _ => return Err(format!("bad modulation {spec:?}: OFDM option must be 1-4")),
            };
            if !(min_mcs..=6).contains(&mcs) {
                return Err(format!(
                    "bad modulation {spec:?}: OFDM option {opt} needs MCS {min_mcs}-6"
                ));
            }
            let mcs = match mcs {
                0 => OfdmMcs::BpskC1_2_4x,
                1 => OfdmMcs::BpskC1_2_2x,
                2 => OfdmMcs::QpskC1_2_2x,
                3 => OfdmMcs::QpskC1_2,
                4 => OfdmMcs::QpskC3_4,
                5 => OfdmMcs::QamC1_2,
                _ => OfdmMcs::QamC3_4,
            };
            let opt = match opt {
                1 => OfdmBandwidthOption::Option1,
                2 => OfdmBandwidthOption::Option2,
                3 => OfdmBandwidthOption::Option3,
                _ => OfdmBandwidthOption::Option4,
            };
            Ok(Modulation::Ofdm(OfdmModulation { mcs, opt, tx_power }))
        }
        "qpsk" => {
            let (kchip, mode) = (a, b);
            let fchip = match kchip {
                100 => QpskChipFrequency::Fchip100,
                200 => QpskChipFrequency::Fchip200,
                1000 => QpskChipFrequency::Fchip1000,
                2000 => QpskChipFrequency::Fchip2000,
                _ => {
                    return Err(format!(
                        "bad modulation {spec:?}: QPSK chip rate must be 100, 200, 1000 or 2000"
                    ))
                }
            };
            let mode = match mode {
                0 => QpskRateMode::RateMode0,
                1 => QpskRateMode::RateMode1,
                2 => QpskRateMode::RateMode2,
                3 => QpskRateMode::RateMode3,
                4 if kchip == 2000 => QpskRateMode::RateMode4,
                4 => {
                    return Err(format!(
                        "bad modulation {spec:?}: QPSK rate mode 4 needs 2000 kchip/s"
                    ))
                }
                _ => {
                    return Err(format!(
                        "bad modulation {spec:?}: QPSK rate mode must be 0-4"
                    ))
                }
            };
            Ok(Modulation::Qpsk(QpskModulation {
                fchip,
                mode,
                tx_power,
            }))
        }
        _ => Err(bad()),
    }
}

impl Config {
    /// Replaces the iperf module's modulation with `spec` (see
    /// [`parse_modulation_spec`]). TX power is kept from the module's preset,
    /// or 10 (the preset default) if it has none.
    pub fn override_modulation(&mut self, spec: &str) -> Result<Modulation, String> {
        let module = self.iperf.module;
        let radio = self
            .radios
            .iter_mut()
            .find(|r| r.module == module)
            .ok_or_else(|| {
                format!("no [radio-*] section for iperf module {module} in the config")
            })?;
        let tx_power = radio.modulation.map(|m| m.tx_power()).unwrap_or(10);
        let modulation = parse_modulation_spec(spec, tx_power)?;
        radio.modulation = Some(modulation);
        Ok(modulation)
    }
}
