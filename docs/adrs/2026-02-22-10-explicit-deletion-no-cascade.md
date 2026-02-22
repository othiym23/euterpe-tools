# 10. Explicit Deletion Over ON DELETE CASCADE

Date: 2026-02-22

## Status

Accepted

## Context

SQLite (and PostgreSQL) support `ON DELETE CASCADE` on foreign key constraints,
which automatically deletes child rows when a parent row is deleted. This is
convenient but hides data destruction behind implicit behavior — a single
`DELETE FROM directories` silently removes all associated files without any
explicit code path for that deletion.

In a media management tool where the database tracks hundreds of thousands of
files, silent data loss is unacceptable. Every deletion should be deliberate,
visible in the code, and testable. Implicit cascades make it harder to reason
about what a query will do, harder to add logging or confirmation steps, and
harder to catch bugs where a parent is deleted unintentionally.

## Decision

All foreign key constraints use `ON DELETE RESTRICT` (or omit the `ON DELETE`
clause, since `RESTRICT` is the default). Application code must explicitly
delete child rows before deleting parent rows. This applies to all current and
future tables.

## Consequences

- Every deletion path is explicit in the DAO layer and can be reviewed, logged,
  and tested independently.
- Attempting to delete a parent row that still has children produces a foreign
  key violation error, which surfaces bugs rather than hiding them.
- Deletion code is slightly more verbose — child rows must be deleted before
  parent rows — but the intent is always clear.
- Tests can verify that the database rejects orphaning deletions, confirming the
  constraint is enforced.
