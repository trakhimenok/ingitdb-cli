# 🔹 FK-Filtered Views

> **Status:** ✅ Implemented

When a collection has both a `default_view` block and one or more columns with a `foreign_key`, the
materializer generates one filtered output file per unique FK value found in the collection's records,
placed under a `$fk/{referring_collection}/{field}/` subdirectory of the *referred* collection's
`$ingitdb` output tree.

## What

When a collection has both a `default_view` block and one or more columns with a `foreign_key`, the
materializer generates one filtered output file per unique FK value found in the collection's records,
placed under a `$fk/{referring_collection}/{field}/` subdirectory of the *referred* collection's
`$ingitdb` output tree.

## Why

Web apps frequently need "all records for a given FK value" (e.g. all companies in a country) in a
single HTTP request; pre-materializing these slices as static files makes that possible without a
query engine.

## Acceptance criteria

- For every collection that has **both** `default_view` set **and** at least one column with
  `foreign_key`, after calling `BuildViews` the following files exist for each unique FK value `V`
  encountered in the records:
  ```
  {outputRoot}/$ingitdb/{referredRelColPath}/$fk/{col.ID}/{colName}/{V}.{ext}
  ```
  where `outputRoot` is `repoRoot` (or `dbPath` when `repoRoot` is empty), `referredRelColPath` is
  the referred collection's `DirPath` (looked up via `def.Collections[colDef.ForeignKey]`) relative
  to `outputRoot`, `col.ID` is the referring collection's ID, `colName` is the FK field name, and
  `ext` is derived from `default_view.format` via `defaultViewFormatExtension`.
- Each FK view file contains **only** the records whose FK column equals `V`; records with a `nil`
  or empty-string FK value are **not** written to any FK view file.
- A collection with **multiple FK columns** produces independent subtrees under each referred
  collection's `$fk/{col.ID}/` directory, one subdirectory per FK field name.
- FK view files use **the same column set** (determined by `determineColumns(col, view)`) and the
  **same format** as the collection's `default_view`, **with the FK column itself excluded** — its
  value is identical for every record in the file, so including it wastes space and bandwidth.
  Example: `countries/$fk/companies/country/gb.ingr` will contain `$ID:string, name:string` but
  **not** `country:string`.
- FK view files include the same INGR header with column-type annotations (i.e. `WithColumnTypes(col)`
  is applied) as the regular default view; `WithRecordsDelimiter` and `WithHash` are applied using
  the same cascade logic as `buildDefaultView`.
- The regular (unfiltered) default view file continues to be generated alongside the FK views —
  existing behaviour is unchanged.
- `BuildViews` counters (`FilesCreated`, `FilesUpdated`, `FilesUnchanged`) include FK view files;
  write-skips (content unchanged) are counted as `FilesUnchanged`.
- Missing parent directories are created automatically (same `os.MkdirAll` pattern as
  `buildDefaultView`).
- If a collection has `default_view` but **no** FK columns, no `$fk/` directories or files are
  written.
- If a collection has FK columns but **no** `default_view` block, no FK views are generated.
- `max_batch_size` is **not** applied to FK view files; each FK file always contains all records for
  that FK value in one file.
- Errors writing individual FK view files are collected into `errs` and do not abort processing of
  other FK values or other FK columns (same error-accumulation pattern as `buildDefaultView`).

## Implementation notes

- Add a new unexported function `buildFKViews` in `pkg/ingitdb/materializer/view_builder.go`
  alongside `buildDefaultView`. Call it from `BuildViews` (and `BuildView`) immediately after the
  `buildDefaultView` call, guarded by the view being a default view and at least one FK column
  existing.
- Signature mirrors `buildDefaultView`:
  ```go
  func buildFKViews(
      dbPath string, repoRoot string,
      col *ingitdb.CollectionDef, def *ingitdb.Definition,
      view *ingitdb.ViewDef, records []ingitdb.IRecordEntry,
      logf func(string, ...any),
  ) (created, updated, unchanged int, errs []error)
  ```
- Find FK columns without nested calls:
  ```go
  columns := col.Columns
  for colName, colDef := range columns {
      if colDef.ForeignKey == "" {
          continue
      }
      // group records by colDef.ForeignKey value, then write files
  }
  ```
- Group records by FK value into `map[string][]ingitdb.IRecordEntry` before writing any file.
  Treat `nil` or empty-string values as skip.
- Output path (no nested calls):
  ```go
  outputRoot := repoRoot
  if outputRoot == "" {
      outputRoot = dbPath
  }
  referredColDef, ok := def.Collections[colDef.ForeignKey]
  // if !ok: append error and continue
  referredRelColPath, _ := filepath.Rel(outputRoot, referredColDef.DirPath)
  outPath := filepath.Join(outputRoot, ingitdb.IngitdbDir, referredRelColPath, "$fk", col.ID, colName, fkValue+"."+ext)
  ```
  The materializer must look up the referred collection's `DirPath` from
  `def.Collections[colDef.ForeignKey]`; if the key is not found, append an error and skip that FK
  column.
- Reuse `determineColumns`, `defaultViewFormatExtension`, `formatExportBatch`, and
  `WithColumnTypes(col)` exactly as in `buildDefaultView`; apply `RecordsDelimiter` and
  `IncludeHash` with the same cascade logic.
- **Exclude the FK column from the export columns** — strip `colName` from the column list before
  serializing, since every record in the file shares the same value:
  ```go
  fkExportColumns := make([]string, 0, len(exportColumns))
  for _, c := range exportColumns {
      if c != colName {
          fkExportColumns = append(fkExportColumns, c)
      }
  }
  ```
- The `viewName` argument passed to `formatExportBatch`:
  `colDef.ForeignKey + "/$fk/" + col.ID + "/" + colName + "/" + fkValue`.
- Idempotency: read existing file before writing; skip and increment `unchanged` when content is
  byte-identical (same `bytes.Equal` pattern as `buildDefaultView`).

## Test cases

- **Happy path — single FK column, two values:** collection `companies` with
  `country.ForeignKey = "countries"`, records
  `[{id:"acme",country:"gb"}, {id:"shopify",country:"ca"}, {id:"bmo",country:"ca"}]` →
  `countries/$fk/companies/country/gb.ingr` (1 record),
  `countries/$fk/companies/country/ca.ingr` (2 records), `created == 2`.
- **FK column excluded from output:** `countries/$fk/companies/country/gb.ingr` contains
  `$ID:string, name:string` in the header but **not** `country:string`.
- **Null/empty FK value is skipped:** records `[{country:"us"}, {country:""}, {country:nil}]` →
  only `us.ingr` written, `created == 1`.
- **Multiple FK columns produce independent subtrees:** collection with both
  `country.ForeignKey = "countries"` and `department.ForeignKey = "departments"` → both
  `countries/$fk/{col.ID}/country/` and `departments/$fk/{col.ID}/department/` subtrees populated
  independently under their respective referred collections.
- **Idempotency:** second run with identical records yields `unchanged == N`, `created == 0`,
  `updated == 0`.
- **No `default_view` → no FK files written.**
- **No FK columns → no `$fk/` directories written.**
- **Error accumulation:** a write failure for one FK value does not abort other FK values; all
  other files are written and the error is returned in `errs`.

## Out of scope

- `max_batch_size` support for FK view files (deferred).
- Deleting stale FK view files when a FK value disappears (deferred; pair with the cleanup pass in
  P1-3).
- `order_by` inherited from `default_view` (deferred; records appear in read order).
- A CLI flag to disable FK view generation.
- Cross-collection fanout (e.g. generating a view under the referenced collection's `$ingitdb/`
  tree).

## See also

- [Default Collection View](default-collection-view.md) — the unfiltered view that FK-filtered
  views are generated alongside.
- [`buildFKViews` implementation](../../pkg/ingitdb/materializer/view_builder.go) — the unexported
  function in `view_builder.go` that implements this feature.
