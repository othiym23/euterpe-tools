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
    let text = fs::read_to_string(path)?;
    parse_config(&text, path.to_string_lossy().as_ref())
}

/// Parse a KDL config string.
pub fn parse_config(text: &str, filename: &str) -> Result<Config, ConfigError> {
    Ok(knuffel::parse(filename, text)?)
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("config I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("config parse error: {0}")]
    Parse(#[from] knuffel::Error),
    #[error("config error: {0}")]
    Validation(String),
}

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
    pub system_patterns: std::collections::HashSet<String>,
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

    /// Load from config file, falling back to defaults on missing file or error.
    pub fn load_or_default() -> Self {
        load_runtime_config().unwrap_or_else(|e| {
            eprintln!("warning: failed to load config: {e}");
            Self::defaults()
        })
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

    let text = fs::read_to_string(&path)?;
    let raw: RawRuntimeConfig = knuffel::parse(path.to_string_lossy().as_ref(), &text)?;
    resolve_raw_config(raw)
}

/// Parse a runtime config from a KDL string (for testing).
pub fn parse_runtime_config(text: &str) -> Result<RuntimeConfig, ConfigError> {
    let raw: RawRuntimeConfig = knuffel::parse("test.kdl", text)?;
    resolve_raw_config(raw)
}

fn resolve_raw_config(raw: RawRuntimeConfig) -> Result<RuntimeConfig, ConfigError> {
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

    let databases: Vec<DatabaseEntry> = raw
        .databases
        .into_iter()
        .filter_map(|d| match (d.root, d.db) {
            (Some(root), Some(db)) => Some(DatabaseEntry {
                name: d.name,
                root: PathBuf::from(root),
                db: PathBuf::from(db),
            }),
            (None, _) => {
                eprintln!(
                    "warning: database \"{}\" missing 'root' field, skipping",
                    d.name
                );
                None
            }
            (_, None) => {
                eprintln!(
                    "warning: database \"{}\" missing 'db' field, skipping",
                    d.name
                );
                None
            }
        })
        .collect();

    if let Some(ref name) = raw.default_database
        && !databases.iter().any(|d| d.name == *name)
    {
        return Err(ConfigError::Validation(format!(
            "default-database \"{name}\" does not match any configured database nickname"
        )));
    }

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
    use std::collections::HashSet;

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
        assert_eq!(
            config.system_patterns,
            HashSet::from(["@eaDir".to_string(), "@custom".to_string()])
        );
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
        assert_eq!(
            config.system_patterns,
            HashSet::from(["only-this".to_string()])
        );
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
    fn runtime_config_parses_init_template() {
        // The etp-init template must parse as valid RuntimeConfig.
        // This catches KDL syntax errors in the template.
        let template = include_str!("../../../cmd/etp-init/src/main.rs");
        let start = template.find("r##\"").unwrap() + 4;
        let end = template.find("\"##;").unwrap();
        let kdl = &template[start..end];
        let config = parse_runtime_config(kdl).unwrap();
        assert!(
            config.system_patterns.contains(&"@eaDir".to_string()),
            "template should include @eaDir"
        );
        assert!(
            config.databases.is_empty(),
            "template databases are commented out"
        );
    }

    #[test]
    fn runtime_config_defaults_returns_hardcoded() {
        let config = RuntimeConfig::defaults();
        assert!(config.system_patterns.contains(&"@eaDir".to_string()));
        assert!(config.system_patterns.contains(&".etp.db".to_string()));
        assert!(config.databases.is_empty());
    }

    #[test]
    fn runtime_config_default_database_must_exist() {
        let kdl = r#"
default-database "nonexistent"

database "music" {
    root "/volume1/music"
    db "/data/music.db"
}
"#;
        let result = parse_runtime_config(kdl);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("nonexistent"),
            "error should mention the bad nickname: {err}"
        );
    }

    #[test]
    fn runtime_config_default_database_valid() {
        let kdl = r#"
default-database "music"

database "music" {
    root "/volume1/music"
    db "/data/music.db"
}
"#;
        let config = parse_runtime_config(kdl).unwrap();
        assert_eq!(config.default_database.as_deref(), Some("music"));
    }
}
