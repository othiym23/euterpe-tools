use crate::state::ScanState;
use glob::Pattern;
use icu_collator::CollatorBorrowed;
use icu_collator::options::{AlternateHandling, CollatorOptions, Strength};
use std::collections::{BTreeMap, BTreeSet};
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
    state: &'a ScanState,
    children: BTreeMap<PathBuf, BTreeSet<String>>,
    patterns: &'a [Pattern],
    collator: CollatorBorrowed<'static>,
    no_escape: bool,
    show_hidden: bool,
}

/// Render a tree view of the scan state, printing to stdout.
/// Returns `(dir_count, file_count)`.
pub fn render_tree(
    state: &ScanState,
    root: &Path,
    patterns: &[Pattern],
    no_escape: bool,
    show_hidden: bool,
) -> (usize, usize) {
    // Build child-directory map: for each dir in state, register it as a child of its parent
    let mut children: BTreeMap<PathBuf, BTreeSet<String>> = BTreeMap::new();
    for dir_key in state.dirs.keys() {
        let dir_path = Path::new(dir_key);
        if let Some(parent) = dir_path.parent()
            && let Some(name) = dir_path.file_name()
        {
            children
                .entry(parent.to_path_buf())
                .or_default()
                .insert(name.to_string_lossy().into_owned());
        }
    }

    let mut options = CollatorOptions::default();
    options.strength = Some(Strength::Quaternary);
    options.alternate_handling = Some(AlternateHandling::Shifted);
    let collator = CollatorBorrowed::try_new(Default::default(), options).unwrap();

    let ctx = TreeContext {
        state,
        children,
        patterns,
        collator,
        no_escape,
        show_hidden,
    };

    println!("{}", root.display());

    let mut dir_count = 1; // count the root directory itself, matching tree's behavior
    let mut file_count = 0;
    render_dir(&ctx, root, "", &mut dir_count, &mut file_count);
    (dir_count, file_count)
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
) {
    let dir_key = dir_path.to_string_lossy();
    let files: Vec<String> = ctx
        .state
        .dirs
        .get(dir_key.as_ref())
        .map(|d| d.files.iter().map(|f| f.filename.clone()).collect())
        .unwrap_or_default();
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
                println!(
                    "{}{}{}",
                    prefix,
                    connector,
                    maybe_escape(name, ctx.no_escape)
                );
                *file_count += 1;
            }
            Entry::Dir(name) => {
                println!(
                    "{}{}{}",
                    prefix,
                    connector,
                    maybe_escape(name, ctx.no_escape)
                );
                *dir_count += 1;
                let child_path = dir_path.join(name);
                render_dir(
                    ctx,
                    &child_path,
                    &format!("{}{}", prefix, child_prefix),
                    dir_count,
                    file_count,
                );
            }
        }
    }
}
