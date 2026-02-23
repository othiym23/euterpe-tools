# TODO.md – extra stuff I came up with along the way

This file is a place to take notes on additional plans (feature requests,
functionality changes, bug fixes) for existing plans. The items are broken out
by project or subproject, with dependencies between tasks indicated by nesting.
Tasks should be marked as done when they are incorporated into the plan, and
then removed once it has been verified that they have been completely
implemented.

## SP 1.4

- [x] Migrate away from using du to sum up the sizes of the dir trees in
      catalog-nas.py. Add a switch to `etp-tree` to append the two kinds of du
      output (total for the root path, and a path for each subdirectory of the
      root path) to the tree output. The output of the replacement function
      should match the output of the catalog-nas.py / etp-catalog / etp catalog
      tree output.
- [x] Because the cataloging process runs both `etp-csv` and `etp-tree` in quick
      succession, it's not necessary to run the dirtree scanner for both
      processes. Add a (hidden) plumbing-only flag (`--no-scan`, unless there's
      a name that better suggests its purpose) to `etp-csv` and `etp-tree` to
      skip the scanning process and just use the cached values to speed up
      etp-catalog.
- [ ] add an `etp-find` command that takes a pattern and finds all paths
      matching that pattern
  - [ ] extend `etp-find` to take `--tree=outfile.tree` and `--csv=outfile.csv`
        command-line switches to produce output in tree and / or csv formats;
        support using `-` for the file path to put the output on stdout
  - [ ] extend `etp-find` to take a `--size` parameter to provide a
        human-readable summary (KiB, MiB, GiB, etc; two significant digit
        precision) of the sizes of all of the matched paths at the end (if
        --tree=- and/or --csv=- are invoked) of etp-find output

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
