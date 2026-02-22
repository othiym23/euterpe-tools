use etcetera::AppStrategy;
use etcetera::AppStrategyArgs;
use etcetera::choose_app_strategy;
use std::path::PathBuf;

const APP_NAME: &str = "euterpe-tools";

fn strategy() -> impl AppStrategy {
    choose_app_strategy(AppStrategyArgs {
        top_level_domain: "net".to_string(),
        author: "aoaioxxysz".to_string(),
        app_name: APP_NAME.to_string(),
    })
    .expect("failed to determine app directories")
}

/// Config directory: `~/Library/Application Support/net.aoaioxxysz.etp/` on macOS,
/// `$XDG_CONFIG_HOME/euterpe-tools/` on Linux.
pub fn config_dir() -> PathBuf {
    strategy().config_dir()
}

/// Config file path: `config.kdl` inside the config directory.
pub fn config_file() -> PathBuf {
    config_dir().join("config.kdl")
}

/// Data directory: `~/Library/Application Support/net.aoaioxxysz.etp/` on macOS,
/// `$XDG_DATA_HOME/euterpe-tools/` on Linux.
pub fn data_dir() -> PathBuf {
    strategy().data_dir()
}

/// Database file path: `metadata.sqlite` inside the data directory.
pub fn db_path() -> PathBuf {
    data_dir().join("metadata.sqlite")
}

/// CAS blob storage root: `assets/` inside the data directory.
pub fn cas_dir() -> PathBuf {
    data_dir().join("assets")
}

/// CAS blob path for a given BLAKE3 hex hash: `assets/{first2}/{full_hash}`.
pub fn cas_blob_path(hash: &str) -> PathBuf {
    let prefix = &hash[..2.min(hash.len())];
    cas_dir().join(prefix).join(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_dir_is_not_empty() {
        let dir = config_dir();
        assert!(!dir.as_os_str().is_empty());
    }

    #[test]
    fn data_dir_is_not_empty() {
        let dir = data_dir();
        assert!(!dir.as_os_str().is_empty());
    }

    #[test]
    fn config_file_ends_with_kdl() {
        let path = config_file();
        assert_eq!(path.file_name().unwrap(), "config.kdl");
    }

    #[test]
    fn db_path_ends_with_sqlite() {
        let path = db_path();
        assert_eq!(path.file_name().unwrap(), "metadata.sqlite");
    }

    #[test]
    fn cas_blob_path_uses_prefix_directory() {
        let path = cas_blob_path("abcdef1234567890");
        assert!(path.ends_with("ab/abcdef1234567890"));
    }

    #[test]
    fn cas_dir_inside_data_dir() {
        let cas = cas_dir();
        let data = data_dir();
        assert!(cas.starts_with(&data));
        assert_eq!(cas.file_name().unwrap(), "assets");
    }

    #[test]
    fn paths_contain_app_name_or_bundle_id() {
        let dir = config_dir();
        let dir_str = dir.to_string_lossy();
        // Depending on platform and environment, the path may use the app name
        // (XDG/Linux) or bundle ID (Apple/macOS native). Either is valid.
        assert!(
            dir_str.contains("euterpe-tools") || dir_str.contains("net.aoaioxxysz.etp"),
            "expected app identifier in path: {dir_str}"
        );
    }
}
