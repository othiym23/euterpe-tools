use crate::db::dao;
use crate::finder::FindMatch;
use glob::Pattern;
use icu_collator::CollatorBorrowed;
use icu_collator::options::{AlternateHandling, CollatorOptions, Strength};
use sqlx::SqlitePool;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Escape non-printable and non-ASCII bytes to '?' (matching tree's default behavior).
/// With -N, returns the name unchanged.
fn maybe_escape(name: &str, no_escape: bool) -> String {
    if no_escape {
        return name.to_string();
    }
    name.chars()
        .map(|c| {
            if c.is_ascii_graphic() || c == ' ' {
                c
            } else {
                '?'
            }
        })
        .collect()
}

/// Shared context for recursive tree rendering.
struct TreeContext<'a> {
    files_by_dir: HashMap<PathBuf, Vec<String>>,
    children: BTreeMap<PathBuf, BTreeSet<String>>,
    patterns: &'a [Pattern],
    collator: CollatorBorrowed<'static>,
    no_escape: bool,
    show_hidden: bool,
}

fn make_collator() -> io::Result<CollatorBorrowed<'static>> {
    let mut options = CollatorOptions::default();
    options.strength = Some(Strength::Quaternary);
    options.alternate_handling = Some(AlternateHandling::Shifted);
    CollatorBorrowed::try_new(Default::default(), options)
        .map_err(|e| io::Error::other(format!("collator initialization failed: {e}")))
}

#[cfg_attr(
    feature = "profiling",
    tracing::instrument(name = "tree_render_tree_from_db", skip_all)
)]
pub async fn render_tree_from_db(
    pool: &SqlitePool,
    scan_id: i64,
    root: &Path,
    patterns: &[Pattern],
    no_escape: bool,
    show_hidden: bool,
) -> io::Result<(usize, usize)> {
    let dir_paths = dao::list_directory_paths(pool, scan_id)
        .await
        .map_err(io::Error::other)?;
    let file_records = dao::list_files(pool, scan_id)
        .await
        .map_err(io::Error::other)?;

    // Build child-directory map from full dir paths
    let mut children: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    for dir_path_str in &dir_paths {
        let dir_path = Path::new(dir_path_str);
        if let Some(parent) = dir_path.parent()
            && let Some(name) = dir_path.file_name()
        {
            children
                .entry(parent.to_path_buf())
                .or_default()
                .insert(name.to_string_lossy().into_owned());
        }
    }

    // Build files_by_dir from file records
    let mut files_by_dir: HashMap<PathBuf, Vec<String>> = HashMap::new();
    for f in &file_records {
        files_by_dir
            .entry(PathBuf::from(&f.dir_path))
            .or_default()
            .push(f.filename.clone());
    }

    let ctx = TreeContext {
        files_by_dir,
        children,
        patterns,
        collator: make_collator()?,
        no_escape,
        show_hidden,
    };

    let mut out = io::stdout();
    writeln!(out, "{}", root.display())?;

    let mut dir_count = 1;
    let mut file_count = 0;
    render_dir(&ctx, root, "", &mut dir_count, &mut file_count, &mut out)?;
    Ok((dir_count, file_count))
}

/// Build a tree from a list of `FindMatch` values and render it to `writer`.
/// Returns (dir_count, file_count).
pub fn render_tree_from_paths(
    matches: &[FindMatch],
    root: &Path,
    writer: &mut dyn Write,
) -> io::Result<(usize, usize)> {
    // Build the same data structures as render_tree_from_db
    let mut children: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    let mut files_by_dir: HashMap<PathBuf, Vec<String>> = HashMap::new();

    // Collect all directory paths that contain matched files
    let mut all_dirs: BTreeSet<PathBuf> = BTreeSet::new();
    for m in matches {
        let full = Path::new(&m.full_path);
        if let Some(parent) = full.parent() {
            let filename = match full.file_name() {
                Some(name) => name.to_string_lossy().into_owned(),
                None => continue,
            };
            files_by_dir
                .entry(parent.to_path_buf())
                .or_default()
                .push(filename);

            // Register all ancestor directories
            let mut ancestor = parent.to_path_buf();
            while ancestor != root {
                all_dirs.insert(ancestor.clone());
                match ancestor.parent() {
                    Some(p) => ancestor = p.to_path_buf(),
                    None => break,
                }
            }
        }
    }

    // Build children map from all_dirs
    for dir in &all_dirs {
        if let Some(parent) = dir.parent()
            && let Some(name) = dir.file_name()
        {
            children
                .entry(parent.to_path_buf())
                .or_default()
                .insert(name.to_string_lossy().into_owned());
        }
    }

    let no_patterns: Vec<Pattern> = Vec::new();
    let ctx = TreeContext {
        files_by_dir,
        children,
        patterns: &no_patterns,
        collator: make_collator()?,
        no_escape: true,
        show_hidden: true,
    };

    writeln!(writer, "{}", root.display())?;

    let mut dir_count = 1;
    let mut file_count = 0;
    render_dir(&ctx, root, "", &mut dir_count, &mut file_count, writer)?;
    Ok((dir_count, file_count))
}

/// Entry in the merged directory listing — either a file or subdirectory.
enum Entry {
    File(String),
    Dir(String),
}

impl Entry {
    fn name(&self) -> &str {
        match self {
            Entry::File(n) | Entry::Dir(n) => n,
        }
    }
}

fn merge_entries(files: &[String], child_dirs: &BTreeSet<String>, ctx: &TreeContext) -> Vec<Entry> {
    let mut entries: Vec<Entry> = files
        .iter()
        .map(|f| Entry::File(f.clone()))
        .chain(child_dirs.iter().map(|d| Entry::Dir(d.clone())))
        .filter(|e| {
            let n = e.name();
            if !ctx.show_hidden && n.starts_with('.') {
                return false;
            }
            !ctx.patterns.iter().any(|p| p.matches(n))
        })
        .collect();

    entries.sort_by(|a, b| ctx.collator.compare(a.name(), b.name()));
    entries
}

fn render_dir(
    ctx: &TreeContext,
    dir_path: &Path,
    prefix: &str,
    dir_count: &mut usize,
    file_count: &mut usize,
    writer: &mut dyn Write,
) -> io::Result<()> {
    let files: Vec<String> = ctx.files_by_dir.get(dir_path).cloned().unwrap_or_default();
    let empty = BTreeSet::new();
    let child_dirs = ctx.children.get(dir_path).unwrap_or(&empty);

    let entries = merge_entries(&files, child_dirs, ctx);
    let total = entries.len();
    for (i, entry) in entries.iter().enumerate() {
        let is_last = i + 1 == total;
        let connector = if is_last { "└── " } else { "├── " };
        let child_prefix = if is_last { "    " } else { "│\u{a0}\u{a0} " };

        match entry {
            Entry::File(name) => {
                writeln!(
                    writer,
                    "{}{}{}",
                    prefix,
                    connector,
                    maybe_escape(name, ctx.no_escape)
                )?;
                *file_count += 1;
            }
            Entry::Dir(name) => {
                writeln!(
                    writer,
                    "{}{}{}",
                    prefix,
                    connector,
                    maybe_escape(name, ctx.no_escape)
                )?;
                *dir_count += 1;
                let child_path = dir_path.join(name);
                render_dir(
                    ctx,
                    &child_path,
                    &format!("{}{}", prefix, child_prefix),
                    dir_count,
                    file_count,
                    writer,
                )?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finder::FindMatch;

    fn make_match(full_path: &str) -> FindMatch {
        FindMatch {
            full_path: full_path.to_string(),
            size: 100,
            ctime: 1000,
            mtime: 2000,
        }
    }

    /// Regression: when root_path in the DB has a trailing slash (e.g.
    /// "/data/music/"), FindMatch paths contain a double slash
    /// ("/data/music//sub/file.txt"). The tree must still render files,
    /// not just directories.
    #[test]
    fn render_tree_from_paths_double_slash() {
        let root = Path::new("/data/music/");
        let matches = vec![
            make_match("/data/music//sub/a.txt"),
            make_match("/data/music//sub/deep/b.txt"),
            make_match("/data/music//top.txt"),
        ];

        let mut buf = Vec::new();
        let (dir_count, file_count) = render_tree_from_paths(&matches, root, &mut buf).unwrap();

        let output = String::from_utf8(buf).unwrap();
        assert!(
            output.contains("a.txt"),
            "file a.txt missing from tree:\n{output}"
        );
        assert!(
            output.contains("b.txt"),
            "file b.txt missing from tree:\n{output}"
        );
        assert!(
            output.contains("top.txt"),
            "file top.txt missing from tree:\n{output}"
        );
        assert_eq!(file_count, 3, "expected 3 files, got {file_count}");
        assert_eq!(
            dir_count, 3,
            "expected 3 dirs (root + sub + deep), got {dir_count}"
        );
    }
}
