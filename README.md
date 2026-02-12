## Dataplane: self-describing Parquet files and Arrow datasets

Dataplane is a small set of utilities for making tabular data files easier to understand *later*—by you or anyone else—without hunting for a separate data dictionary.

It focuses on three practical problems that come up all the time in real workflows:

- **Column meaning**: What does `temp` represent? Air temperature at 2 m? Surface temperature? Something else?
- **Units**: Is `wind` in m/s or mph? Is `wbgt` in °C or °F?
- **Storage quirks**: Were decimals stored normally, or stored compactly as integers with a scale factor?

Dataplane stores this information as lightweight metadata alongside your data, so files remain “self-describing” when they move between projects, machines, teams, and time.

---

<<<<<<< HEAD
## Quick definitions (no background assumed)

- **Table**: rows × columns (like a spreadsheet).
- **Parquet**: a fast, compact file format for storing tables.
- **Dataset folder (Arrow Dataset)**: a directory containing many Parquet files that behave like one large table.
- **Spec**: a small “data dictionary” table describing columns (meaning, units, and optional scaling intent).
- **Unit attributes**: optional R-native column attributes like `attr(x$temp, "units") <- "degC"`.
- **Scaling (optional)**: storing values like `25.12` as `2512` (integer) plus metadata that says “divide by 100 to recover.”

---

## What Dataplane helps you do

### 1) Create and maintain a spec (data dictionary)
Start from your data, generate a draft spec, then edit it:
- `dp_spec_default()` (or `dp_spec()`) to create a starter spec
- `dp_set_units()` to document units
- `dp_set_scale()` to document optional scaling intent

### 2) Add/check unit attributes in R (optional, but helpful)
- `dp_tag_units()` can attach missing unit attributes to a copy of your data
- `dp_check_units()` audits what’s present vs what the spec expects (warns by default)

### 3) Write/read Parquet with metadata embedded
- `dp_write()` writes Parquet and stores Dataplane metadata in the file schema
- `dp_read()` reads it back and can optionally:
  - decode scaled storage columns
  - re-attach unit attributes

### 4) Work with dataset folders (multiple Parquet files)
If your data is split across many Parquet files (e.g., partitioned by date):
- `dp_write_dataset()` writes a dataset directory and a small sidecar metadata file
- `dp_open()` opens the dataset and reads the sidecar metadata reliably

### 5) Detect Dataplane metadata quickly
- `dp_detect()` checks whether a file/folder likely contains Dataplane metadata
- `dp_print_detect()` prints a human-readable summary

---

## What Dataplane does *not* try to be

- Not a full unit conversion framework (it documents/validates units; conversions are separate decisions).
- Not a required schema enforcement system (it aims to reduce mistakes, not block work).
- Not cloud-dependent (everything works locally; cloud connectors are optional extras).

---

## Recommended learning path (keeps complexity under control)

1. **Start local**: create a small table → `dp_spec_default()` → `dp_set_units()`
2. **Write one file**: `dp_write()` → `dp_read()` → inspect only the *few* metadata pieces you care about
3. **Add scaling only if needed**: introduce `dp_set_scale()` and `scale = TRUE` during writes
4. **Move to dataset folders** only after single-file Parquet feels comfortable
5. Treat cloud/storage integrations as **optional** and separate from the core workflow

---

## Optional: cloud/platform integrations (skip unless you need them)

Some workflows store Parquet files in cloud storage or hosted platforms.

- **S3**: Amazon’s object storage (think “files in the cloud”).
- **Synapse**: a hosted platform some research groups use for managing and sharing datasets.

These are useful in production pipelines, but they are not required to understand Dataplane or use it locally.
=======
``` r
devtools::install_github("jclark50/dataplane")
# OR
pak::pak("jclark50/dataplane")
```

````markdown
# dataplane

**Data architecture and processing helpers with repeatable metadata workflows.**

`dataplane` is a lightweight set of R helpers for writing Parquet files (and Arrow datasets) with **self-describing metadata**:

- A **spec** (`dp_spec`) that documents columns, concepts, units, and optional scaling intent
- An **audit** table that records what was written (and what was scaled/tagged)
- Portable **Parquet schema key/value metadata** (and a dataset-level **sidecar** metadata file for Arrow datasets)

It is designed for repeatable pipelines where “what is this column / what units / how was this stored?” should be answerable from the file itself.

---

## Installation

### From GitHub

```r
# install.packages("remotes")
remotes::install_github("jclark50/dataplane")
````

### Build vignettes (optional)

If you want to build the R Markdown vignettes during install:

```r
remotes::install_github("jclark50/dataplane", build_vignettes = TRUE)
```

> **Note (Windows):** Building R Markdown vignettes requires **Pandoc**. If you see an error like “Pandoc is required … but not available”, install Pandoc (or install RStudio, which bundles Pandoc), or skip vignette building.

---

## Core idea

A typical Parquet file has a schema and data, but not necessarily a clear, standard place to store:

* declared units,
* what those units mean (concept),
* or how values were stored (e.g., scaled integers).

`dataplane` stores a **spec** and **audit** inside Parquet metadata (compressed + base64), so a file can “explain itself” later.

---

## Quick start (5 minutes)

```r
library(data.table)
library(dataplane)

dt <- data.table(
  temp = c(25.12, 25.44),
  rh   = c(55, 52),
  wind = c(2.1, 3.0)
)

# 1) Create a spec from the table (metric or imperial)
sp <- dp_spec_default(dt, declared_system = "metric")
sp
sp$fields[]

# 2) (Optional) Set explicit units and scaling intent
sp <- dp_set_units(sp, "wind", metric_units = "m/s", imperial_units = "mph")
sp <- dp_set_scale(sp, "temp", 100)  # keep 2 decimals as integer storage

# 3) Write a parquet file embedding metadata (and optionally scale at write time)
out <- tempfile(fileext = ".parquet")
dp_write(dt, out, spec = sp, scale = TRUE)

# 4) Read it back + decode scaled columns + attach units
res <- dp_read(out, decode_scaled = TRUE, attach_units = "declared")
res$dt[]
res$meta$spec_dt[]
res$meta$audit_dt[]
```

---

## Full walkthrough (README version of the vignette)

### Dependencies

This package leans on:

* **arrow** for Parquet and Dataset I/O
* **data.table** for fast tabular operations

Examples below assume:

```r
library(dataplane)
library(data.table)
```

---

## 1) Specs (`dp_spec`): documenting columns, units, and scaling intent

A `dp_spec` is the “contract” that describes your table:

* column names
* optional concept labels
* metric/imperial units
* declared units (resolved from your declared system, unless you override)
* optional scaling intent (`writer_scale`)

### 1.1 Create a default spec from a table

```r
dt <- data.table(
  temp = c(25.12, 25.44),
  rh   = c(55, 52),
  wind = c(2.1, 3.0)
)

sp <- dp_spec_default(dt, declared_system = "metric")
sp
sp$fields[]
```

`dp_spec_default()` uses a small internal synonym catalog to auto-recognize common columns (e.g., `temp`, `rh`) when possible. Unmatched columns remain present with `NA` concept/units by design.

### 1.2 Alias: `dp_spec()`

`dp_spec()` is an alias for `dp_spec_default()`:

```r
sp <- dp_spec(dt, declared_system = "metric")
```

### 1.3 Edit units in a spec: `dp_set_units()`

```r
sp <- dp_set_units(
  sp,
  columns = "wind",
  metric_units = "m/s",
  imperial_units = "mph"
)
```

You can set `declared_units` directly if you want a hard override.

### 1.4 Record scaling intent: `dp_set_scale()`

Scaling intent means: “if stored as integers, what factor preserves precision?”

```r
sp <- dp_set_scale(sp, columns = "temp", writer_scale = 100)
```

This does **not** change your data by itself. It records intent for:

* `dp_scale_encode()` (in-memory)
* `dp_write(..., scale = TRUE)` (at write time)

---

## 2) Units: tagging and checking

### 2.1 Tag units onto an in-memory table: `dp_tag_units()`

This adds a unit attribute (default attribute name: `"units"`) to each column using your spec.

```r
tag <- dp_tag_units(dt, sp, system = "declared", unit_attr = "units")
dt2 <- tag$dt

attr(dt2$temp, "units")
attr(dt2$wind, "units")
```

By default `only_missing = TRUE`, so it won’t overwrite existing unit attributes unless you ask it to.

### 2.2 Validate units against a spec: `dp_check_units()`

`dp_check_units()` compares column unit attributes to expected units. It warns by default and returns an audit table invisibly.

```r
dp_check_units(dt2, sp, system = "declared")
```

If you want it to stop on any problem:

```r
dp_check_units(dt2, sp, stop_on_any = TRUE)
```

---

## 3) Scaling: stable integer storage with round-trip decode

Scaling is useful when:

* you want compact integer storage,
* you want predictable decimals,
* and you want a stable round-trip back to numeric.

### 3.1 Encode: `dp_scale_encode()`

```r
sp <- dp_set_scale(sp, "temp", 100)

enc <- dp_scale_encode(dt, sp, suffix = "_i")
enc$dt[]
enc$scale_map[]
```

By default, this creates a new integer storage column (e.g., `temp_i`), leaving the original numeric column intact.

### 3.2 Decode: `dp_scale_decode()`

Decoding uses the audit/plan table (usually read from file metadata):

```r
dt_int <- data.table(temp_i = c(2512L, 2544L))
audit  <- data.table(column="temp", writer_scale=100, storage_column="temp_i", did_scale=TRUE)

dp_scale_decode(dt_int, audit, keep_storage = TRUE)[]
```

---

## 4) File I/O: `dp_write()` and `dp_read()`

### 4.1 Write a Parquet file with embedded Dataplane metadata: `dp_write()`

`dp_write()` embeds a compressed payload into the Parquet schema metadata:

* spec table (serialized, gzip, base64)
* audit table (serialized, gzip, base64)
* a few scalar keys (writer, timestamp, version, etc.)

```r
out <- tempfile(fileext = ".parquet")

dp_write(
  x = dt,
  path = out,
  spec = sp,
  mode = "validate",   # optionally run dp_check_units() before writing
  scale = TRUE,        # optionally apply dp_scale_encode() at write time
  suffix = "_i",
  compression = "zstd"
)
```

If you prefer a simple default spec without manually constructing one:

```r
dp_write(dt, out, spec = "metric")   # or "imperial"
```

### 4.2 Read back data + metadata: `dp_read()`

`dp_read()` returns a list:

* `dt`: a data.table
* `meta`: parsed metadata (spec_dt and audit_dt, plus raw key/values)

```r
res <- dp_read(
  path = out,
  decode_scaled = TRUE,
  keep_storage = TRUE,
  attach_units = "declared"
)

res$dt[]
res$meta$spec_dt[]
res$meta$audit_dt[]
names(res$meta$kv)
```

### 4.3 Read metadata only: `dp_read_meta()`

```r
m <- dp_read_meta(out)
m$spec_dt[]
m$audit_dt[]
```

---

## 5) Dataset workflows (Arrow Datasets): sidecar metadata

Arrow datasets are directories of Parquet files, often partitioned. Dataset-level metadata is best stored in a **sidecar** file to avoid scanning all data files.

`dataplane` supports:

* writing dataset data with `arrow::write_dataset()` plus a **sidecar** metadata parquet
* opening a dataset and reading sidecar metadata in one call

### 5.1 Write dataset + sidecar: `dp_write_dataset()`

`dp_write_dataset()`:

* infers dataset columns without collecting data
* writes the dataset (directory of Parquet files)
* writes a sidecar file like `dp_meta.parquet` storing spec + audit in schema metadata

```r
ds_dir <- tempfile(pattern = "dp_dataset_")
dir.create(ds_dir)

# x can be an Arrow Table, RecordBatchReader, Dataset, or other arrow-compatible input
library(arrow)
tbl <- arrow::Table$create(dt)

dp_write_dataset(
  x = tbl,
  path = ds_dir,
  spec = sp,
  scale_document = TRUE,   # documents intended storage columns (does not transform dataset files)
  suffix = "_i"
)

list.files(ds_dir)
```

### 5.2 Read dataset sidecar metadata: `dp_read_dataset_meta()`

```r
meta_ds <- dp_read_dataset_meta(ds_dir)
meta_ds$spec_dt[]
meta_ds$audit_dt[]
meta_ds$sidecar_path
```

### 5.3 Open dataset + optionally load sidecar metadata: `dp_open()`

```r
opened <- dp_open(ds_dir, metadata = "sidecar")
opened$ds
opened$meta$spec_dt[]
```

If you only want the Arrow Dataset object:

```r
ds <- dp_open(ds_dir, metadata = "none")
ds
```

---

## 6) Detection helpers: “does this file/dataset look like Dataplane?”

### 6.1 Detect: `dp_detect()`

Works on:

* a single Parquet file
* or a dataset directory (prefers sidecar if present; otherwise samples a few Parquet files)

```r
dp_detect(out)
dp_detect(ds_dir)
```

### 6.2 Print a detection result: `dp_print_detect()`

```r
x <- dp_detect(out)
dp_print_detect(x)
```

---

## 7) GeoParquet helpers (independent of Dataplane metadata)

These `gp_*` helpers focus on GeoParquet metadata conventions (the `"geo"` JSON schema metadata key).

### 7.1 Build GeoParquet metadata: `gp_geoparquet_build_geo_meta()`

```r
geo_meta <- gp_geoparquet_build_geo_meta(
  geometry_columns = "geometry",
  primary_column = "geometry",
  encoding = "WKB",
  geometry_types = c("Point"),
  crs = 4326
)

gp_geoparquet_geo_json(geo_meta)
```

### 7.2 Attach GeoParquet metadata to an Arrow Table: `gp_geoparquet_attach_geo_to_table()`

```r
library(arrow)

tbl <- arrow::Table$create(data.table(id=1:2, x=c(0,1), y=c(0,1)))
# In real usage, your table would already contain a WKB geometry column.
# This function is mainly about metadata attachment mechanics.

tbl2 <- gp_geoparquet_attach_geo_to_table(tbl, geo_meta)
```

### 7.3 Write GeoParquet with metadata: `gp_geoparquet_write_parquet()`

```r
gp_out <- tempfile(fileext = ".parquet")
gp_geoparquet_write_parquet(
  x = data.table(id=1:2),
  path = gp_out,
  geo_meta = geo_meta
)
```

### 7.4 Read GeoParquet metadata / test GeoParquet: `gp_geoparquet_read_geo()`, `gp_geoparquet_is_geoparquet()`

```r
gp_geoparquet_read_geo(gp_out)
gp_geoparquet_is_geoparquet(gp_out)
```

### 7.5 Infer GeoParquet metadata from `sf`: `gp_geoparquet_infer_from_sf()`

```r
# install.packages("sf")
library(sf)

x <- st_as_sf(
  data.frame(id=1:2, x=c(0,1), y=c(0,1)),
  coords = c("x","y"),
  crs = 4326
)

meta <- gp_geoparquet_infer_from_sf(x)
meta
```

---

## Recommended project conventions

### Prefix strategy

Dataplane functions are intentionally prefixed to avoid conflicts with `arrow` and other packages:

* `dp_*` for dataplane metadata + I/O helpers
* `gp_*` for GeoParquet helpers

### Where metadata lives

* **Single Parquet files:** stored in schema key/value metadata
* **Arrow datasets:** stored in a small **sidecar parquet**, e.g. `dp_meta.parquet`

---

## Common troubleshooting

### “Vignette building fails because Pandoc is missing”

* Install **Pandoc**, or install **RStudio** (bundles Pandoc), or skip `build_vignettes = TRUE`.

### “pkgdown reference topic not found”

* `_pkgdown.yml` must list actual **topic names** in `man/*.Rd`.
* Prefer selectors like `starts_with("dp_")` / `starts_with("gp_")` to avoid drift during renames.

---

## License

MIT (see `LICENSE`).

---

## Links

* GitHub repo: [https://github.com/jclark50/dataplane](https://github.com/jclark50/dataplane)
* Website: [https://jclark50.github.io/dataplane/](https://jclark50.github.io/dataplane/)

```

If you want, paste your current vignette (`dataplane-parquet-metadata.Rmd`) header + section titles and I’ll align the README headings and narrative structure *exactly* (and ensure every function mentioned is exported and documented so pkgdown doesn’t break).
::contentReference[oaicite:0]{index=0}
```
>>>>>>> 53b099521d79bb7e3cdfecef1d590cc419f8c4c5
