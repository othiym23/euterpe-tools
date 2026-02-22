use std::fs;
use std::path::Path;

/// Top-level configuration document.
#[derive(Debug, knuffel::Decode)]
pub struct Config {
    #[knuffel(child)]
    pub global: Option<Global>,
    #[knuffel(children(name = "scan"))]
    pub scans: Vec<Scan>,
}

/// Global settings shared across all scans.
#[derive(Debug, knuffel::Decode)]
pub struct Global {
    #[knuffel(child, unwrap(argument))]
    pub scanner: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub tree: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub home_base: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub trees_path: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub csvs_path: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub state_path: Option<String>,
}

/// A single scan target.
#[derive(Debug, knuffel::Decode)]
pub struct Scan {
    #[knuffel(argument)]
    pub name: String,
    #[knuffel(child, unwrap(argument))]
    pub mode: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub disk: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub desc: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub header: Option<String>,
}

/// Load and parse a KDL config file.
pub fn load_config(path: &Path) -> Result<Config, ConfigError> {
    let text = fs::read_to_string(path).map_err(ConfigError::Io)?;
    parse_config(&text, path.to_string_lossy().as_ref())
}

/// Parse a KDL config string.
pub fn parse_config(text: &str, filename: &str) -> Result<Config, ConfigError> {
    knuffel::parse(filename, text).map_err(ConfigError::Parse)
}

#[derive(Debug)]
pub enum ConfigError {
    Io(std::io::Error),
    Parse(knuffel::Error),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Io(e) => write!(f, "config I/O error: {e}"),
            ConfigError::Parse(e) => write!(f, "config parse error: {e}"),
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_config() {
        let kdl = r#"
global {
    scanner "$HOME/bin/etp-csv"
    tree "$HOME/bin/etp-tree"
    home-base "/volume1/data/downloads/(music)"
    trees-path "{home-base}/catalogs/trees"
    csvs-path "{trees-path}/csv"
    state-path "{trees-path}/state"
}

scan "music" {
    mode "subs"
    disk "/volume1/music"
    desc "euterpe music (NAS volume)"
    header "Synology NAS //music"
}

scan "television" {
    mode "df"
    disk "/volume1/data/video/Television"
    desc "euterpe television (NAS directory)"
    header "Synology NAS //data/video/Television share"
}
"#;
        let config = parse_config(kdl, "test.kdl").unwrap();

        let global = config.global.unwrap();
        assert_eq!(global.scanner.as_deref(), Some("$HOME/bin/etp-csv"));
        assert_eq!(
            global.home_base.as_deref(),
            Some("/volume1/data/downloads/(music)")
        );

        assert_eq!(config.scans.len(), 2);
        assert_eq!(config.scans[0].name, "music");
        assert_eq!(config.scans[0].mode.as_deref(), Some("subs"));
        assert_eq!(config.scans[0].disk.as_deref(), Some("/volume1/music"));
        assert_eq!(config.scans[1].name, "television");
        assert_eq!(config.scans[1].mode.as_deref(), Some("df"));
    }

    #[test]
    fn parse_empty_config() {
        let config = parse_config("", "empty.kdl").unwrap();
        assert!(config.global.is_none());
        assert!(config.scans.is_empty());
    }

    #[test]
    fn parse_config_no_global() {
        let kdl = r#"
scan "music" {
    disk "/volume1/music"
}
"#;
        let config = parse_config(kdl, "test.kdl").unwrap();
        assert!(config.global.is_none());
        assert_eq!(config.scans.len(), 1);
    }

    #[test]
    fn parse_config_minimal_scan() {
        let kdl = r#"scan "test" {}"#;
        let config = parse_config(kdl, "test.kdl").unwrap();
        assert_eq!(config.scans.len(), 1);
        assert_eq!(config.scans[0].name, "test");
        assert!(config.scans[0].mode.is_none());
        assert!(config.scans[0].disk.is_none());
    }

    #[test]
    fn parse_invalid_config_returns_error() {
        let result = parse_config("not { valid } kdl [[[", "bad.kdl");
        assert!(result.is_err());
    }

    #[test]
    fn slashdash_comments_out_scan() {
        let kdl = r#"
scan "active" {
    disk "/volume1/music"
}

/- scan "disabled" {
    disk "/volume1/old"
}
"#;
        let config = parse_config(kdl, "test.kdl").unwrap();
        assert_eq!(config.scans.len(), 1);
        assert_eq!(config.scans[0].name, "active");
    }
}
