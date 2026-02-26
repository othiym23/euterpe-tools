# TODO.md – extra stuff I came up with along the way

This file is a place to take notes on additional plans (feature requests,
functionality changes, bug fixes) for existing plans. The items are broken out
by project or subproject, with dependencies between tasks indicated by nesting.
Tasks should be marked as done when they are incorporated into the plan, and
then removed once it has been verified that they have been completely
implemented.

## SP 1.5

- [x] add an `etp-find` command that takes a pattern and finds all paths
      matching that pattern
  - [x] extend `etp-find` to take `--tree=outfile.tree` and `--csv=outfile.csv`
        command-line switches to produce output in tree and / or csv formats;
        support using `-` for the file path to put the output on stdout
  - [x] extend `etp-find` to take a `--size` parameter to provide a
        human-readable summary (KiB, MiB, GiB, etc; two significant digit
        precision) of the sizes of all of the matched paths at the end (if
        --tree=- and/or --csv=- are invoked) of etp-find output
- [x] etp-find bug: `--tree` should print the files matched and not just the
      directories (also the number of files should be included in the count)
- [x] etp-find feature: don't do a scan unless the database doesn't exist
- [ ] etp-find feature: add '-I' / '--insensitive' for case-insensitive matching
- [ ] etp-find feature: don't require a directory parameter (match against all
      paths in the db)
- [ ] etp-csv, etp-tree: add a `--find` option to both `etp-tree` and `etp-csv`
      that uses the same search logic as `etp-find` – just a different way to
      invoke the same functionality

## SP 1.6

- [ ] why do we need both `etp/` and `scripts/`?
- [ ] refactor plumbing binaries to go into .local/share/libexec
- [ ] etp-scan: add a new plumbing command that extracts the scanning portion
      into its own command, and refactor porcelain to manage scanning before
      calling etp-tree and etp-csv unless `--no-scan` is passed to the
      porcelain. Remove `--no-scan` option from `etp-csv` and `etp-tree` and
      clean up those tools' source.
- [ ] create README with description of all porcelain commands and with
      installation instructions

## SP 2

- [ ] add change and rename detection to file scanner as part of metadata
      scanning. use inode changes to trace renames and Btrfs transid to
      double-check whether file contents have changed before scanning
- [ ] write a utility to truncate media files for various formats to just
      include the metadata blocks and enough frame data to be a valid media file
      for that encoding. This will be useful for gathering test cases to for
      metadata tag reading and updating.
- [ ] write a function to fingerprint the metadata blocks without reading the
      whole media file
