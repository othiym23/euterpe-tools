use crate::ops;
use crate::paths;
use std::fs;
use std::path::{Path, PathBuf};

/// Top-level catalog configuration document (catalog.kdl).
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
    pub home_base: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub trees_path: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub csvs_path: Option<String>,
    #[knuffel(child, unwrap(argument))]
    pub db_path: Option<String>,
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

// --- Runtime configuration (config.kdl) ---

/// KDL document for runtime config (separate from catalog.kdl).
#[derive(Debug, knuffel::Decode)]
struct RawRuntimeConfig {
    #[knuffel(child, unwrap(argument))]
    default_database: Option<String>,
    #[knuffel(child, unwrap(argument))]
    cas_dir: Option<String>,
    #[knuffel(child)]
    system_files: Option<PatternBlock>,
    #[knuffel(child)]
    user_excludes: Option<PatternBlock>,
    #[knuffel(children(name = "database"))]
    databases: Vec<RawDatabaseEntry>,
}

/// A block containing repeated `pattern "value"` children.
#[derive(Debug, knuffel::Decode)]
struct PatternBlock {
    #[knuffel(children(name = "pattern"))]
    patterns: Vec<PatternNode>,
}

/// A single `pattern "value"` node.
#[derive(Debug, knuffel::Decode)]
struct PatternNode {
    #[knuffel(argument)]
    value: String,
}

/// A `database "name" { root "..." db "..." }` block.
#[derive(Debug, knuffel::Decode)]
struct RawDatabaseEntry {
    #[knuffel(argument)]
    name: String,
    #[knuffel(child, unwrap(argument))]
    root: Option<String>,
    #[knuffel(child, unwrap(argument))]
    db: Option<String>,
}

/// Resolved runtime configuration used by commands at startup.
#[derive(Debug)]
pub struct RuntimeConfig {
    pub default_database: Option<String>,
    pub cas_dir: Option<PathBuf>,
    pub system_patterns: Vec<String>,
    pub user_excludes: Vec<String>,
    pub databases: Vec<DatabaseEntry>,
}

/// A named database with root directory and DB file path.
#[derive(Debug, Clone)]
pub struct DatabaseEntry {
    pub name: String,
    pub root: PathBuf,
    pub db: PathBuf,
}

impl RuntimeConfig {
    /// Build with hardcoded defaults (no config file).
    pub fn defaults() -> Self {
        Self {
            default_database: None,
            cas_dir: None,
            system_patterns: ops::DEFAULT_SYSTEM_PATTERNS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            user_excludes: ops::DEFAULT_USER_EXCLUDES
                .iter()
                .map(|s| s.to_string())
                .collect(),
            databases: Vec::new(),
        }
    }

    /// Look up a database entry by nickname.
    pub fn resolve_database(&self, name: &str) -> Option<&DatabaseEntry> {
        self.databases.iter().find(|d| d.name == name)
    }
}

/// Load runtime config from `config.kdl`. Returns defaults if the file
/// doesn't exist. Returns an error only if the file exists but can't be parsed.
pub fn load_runtime_config() -> Result<RuntimeConfig, ConfigError> {
    let path = match paths::config_file() {
        Ok(p) => p,
        Err(_) => return Ok(RuntimeConfig::defaults()),
    };

    if !path.exists() {
        return Ok(RuntimeConfig::defaults());
    }

    let text = fs::read_to_string(&path).map_err(ConfigError::Io)?;
    let raw: RawRuntimeConfig =
        knuffel::parse(path.to_string_lossy().as_ref(), &text).map_err(ConfigError::Parse)?;

    let system_patterns = match raw.system_files {
        Some(block) => block.patterns.into_iter().map(|p| p.value).collect(),
        None => ops::DEFAULT_SYSTEM_PATTERNS
            .iter()
            .map(|s| s.to_string())
            .collect(),
    };

    let user_excludes = match raw.user_excludes {
        Some(block) => block.patterns.into_iter().map(|p| p.value).collect(),
        None => ops::DEFAULT_USER_EXCLUDES
            .iter()
            .map(|s| s.to_string())
            .collect(),
    };

    let databases = raw
        .databases
        .into_iter()
        .filter_map(|d| {
            let root = d.root?;
            let db = d.db?;
            Some(DatabaseEntry {
                name: d.name,
                root: PathBuf::from(root),
                db: PathBuf::from(db),
            })
        })
        .collect();

    Ok(RuntimeConfig {
        default_database: raw.default_database,
        cas_dir: raw.cas_dir.map(PathBuf::from),
        system_patterns,
        user_excludes,
        databases,
    })
}

/// Parse a runtime config from a KDL string (for testing).
pub fn parse_runtime_config(text: &str) -> Result<RuntimeConfig, ConfigError> {
    let raw: RawRuntimeConfig = knuffel::parse("test.kdl", text).map_err(ConfigError::Parse)?;

    let system_patterns = match raw.system_files {
        Some(block) => block.patterns.into_iter().map(|p| p.value).collect(),
        None => ops::DEFAULT_SYSTEM_PATTERNS
            .iter()
            .map(|s| s.to_string())
            .collect(),
    };

    let user_excludes = match raw.user_excludes {
        Some(block) => block.patterns.into_iter().map(|p| p.value).collect(),
        None => ops::DEFAULT_USER_EXCLUDES
            .iter()
            .map(|s| s.to_string())
            .collect(),
    };

    let databases = raw
        .databases
        .into_iter()
        .filter_map(|d| {
            let root = d.root?;
            let db = d.db?;
            Some(DatabaseEntry {
                name: d.name,
                root: PathBuf::from(root),
                db: PathBuf::from(db),
            })
        })
        .collect();

    Ok(RuntimeConfig {
        default_database: raw.default_database,
        cas_dir: raw.cas_dir.map(PathBuf::from),
        system_patterns,
        user_excludes,
        databases,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full_config() {
        let kdl = r#"
global {
    home-base "/volume1/data/downloads/(music)"
    trees-path "{home-base}/catalogs/trees"
    csvs-path "{trees-path}/csv"
    db-path "{trees-path}/db"
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
        assert_eq!(
            global.home_base.as_deref(),
            Some("/volume1/data/downloads/(music)")
        );
        assert_eq!(global.db_path.as_deref(), Some("{trees-path}/db"));

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

    // --- RuntimeConfig tests ---

    #[test]
    fn runtime_config_empty_uses_defaults() {
        let config = parse_runtime_config("").unwrap();
        assert!(config.default_database.is_none());
        assert!(config.cas_dir.is_none());
        assert!(!config.system_patterns.is_empty());
        assert!(config.system_patterns.contains(&"@eaDir".to_string()));
        assert!(config.databases.is_empty());
    }

    #[test]
    fn runtime_config_full() {
        let kdl = r#"
default-database "music"
cas-dir "/volume1/data/etp/assets"

system-files {
    pattern "@eaDir"
    pattern "@custom"
}

user-excludes {
    pattern "*.bak"
}

database "music" {
    root "/volume1/music"
    db "/data/music.db"
}

database "tv" {
    root "/volume1/video/Television"
    db "/data/tv.db"
}
"#;
        let config = parse_runtime_config(kdl).unwrap();

        assert_eq!(config.default_database.as_deref(), Some("music"));
        assert_eq!(
            config.cas_dir.as_deref(),
            Some(Path::new("/volume1/data/etp/assets"))
        );
        assert_eq!(config.system_patterns, vec!["@eaDir", "@custom"]);
        assert_eq!(config.user_excludes, vec!["*.bak"]);
        assert_eq!(config.databases.len(), 2);
        assert_eq!(config.databases[0].name, "music");
        assert_eq!(config.databases[0].root, Path::new("/volume1/music"));
        assert_eq!(config.databases[0].db, Path::new("/data/music.db"));
        assert_eq!(config.databases[1].name, "tv");
    }

    #[test]
    fn runtime_config_system_files_overrides_defaults() {
        let kdl = r#"
system-files {
    pattern "only-this"
}
"#;
        let config = parse_runtime_config(kdl).unwrap();
        assert_eq!(config.system_patterns, vec!["only-this"]);
    }

    #[test]
    fn runtime_config_resolve_database() {
        let kdl = r#"
database "music" {
    root "/volume1/music"
    db "/data/music.db"
}
"#;
        let config = parse_runtime_config(kdl).unwrap();
        let entry = config.resolve_database("music").unwrap();
        assert_eq!(entry.root, Path::new("/volume1/music"));
        assert!(config.resolve_database("nonexistent").is_none());
    }

    #[test]
    fn runtime_config_database_missing_fields_skipped() {
        let kdl = r#"
database "incomplete" {
    root "/volume1/music"
}
"#;
        let config = parse_runtime_config(kdl).unwrap();
        assert!(
            config.databases.is_empty(),
            "database without db field should be skipped"
        );
    }

    #[test]
    fn runtime_config_defaults_returns_hardcoded() {
        let config = RuntimeConfig::defaults();
        assert!(config.system_patterns.contains(&"@eaDir".to_string()));
        assert!(config.system_patterns.contains(&".etp.db".to_string()));
        assert!(config.databases.is_empty());
    }
}
