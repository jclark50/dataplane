# dataplane

**dataplane** is a general-purpose toolkit for data architecture and reproducible data workflows in R.  
It targets data scientists, climate practitioners, and anyone working with data pipelines.

The package is intentionally **general**, while also being leveraged by other Klimo packages.

## Key capabilities

### Parquet I/O with embedded metadata (“Klimo Parquet”)

- Write Parquet with a **field specification** (declared units, suggested scaling, and column concepts).
- Optionally write **scaled integer columns** for compact storage.
- Store a **spec** and an **audit record** in Parquet schema key-value metadata.
- Read Parquet and reconstruct metadata (spec + audit), optionally applying:
  - decoding of scaled integer columns, and/or
  - attaching unit attributes back onto columns.

### Synapse REST utilities (pure R)

- A lightweight Synapse client implemented in R (via `httr2`) for:
  - resolving tokens,
  - folder creation,
  - file upload/download,
  - annotations (get/set),
  - entity lookup by path.

### Small utilities

- `listS3files()` helper for listing S3 keys into a consistent tabular form.
- `requireRAM()` and `startEnvironment()` helpers for long-running jobs that need to manage memory/parallel workers.
- `unit()` helper for tagging vectors with unit attributes (and, where configured, converting units).

## Installation

### GitHub (public repo)

``` r
install.packages("pak")
pak::pak("jclark50/dataplane")
```

Alternative:

``` r
install.packages("remotes")
remotes::install_github("jclark50/dataplane")
```

## Quick start: write Parquet + read metadata back

``` r
library(data.table)
library(dataplane)

dt <- data.table(
  site_id = c("A", "A", "A", "B", "B"),
  ts_utc  = as.POSIXct(
    c("2026-01-10 12:00:00", "2026-01-10 13:00:00", "2026-01-10 14:00:00",
      "2026-01-10 12:00:00", "2026-01-10 13:00:00"),
    tz = "UTC"
  ),
  ta      = c( 1.3,  2.1,  2.8, -0.4,  0.2),
  td      = c(-1.2, -0.4,  0.1, -2.3, -1.8),
  rh      = c(80, 75, 70, 85, 83),
  wind10m = c(2.5, 3.1, 1.7, 4.2, 2.9),
  wdir10m = c(220, 235, 210, 190, 205)
)

unit(dt$ta)      <- "degC"
unit(dt$td)      <- "degC"
unit(dt$rh)      <- "percent"
unit(dt$wind10m) <- "m/s"
unit(dt$wdir10m) <- "deg"

spec <- klimo_spec_default(dt, system = "metric")
klimo_spec_preview(spec)

out_file <- file.path(tempdir(), "example_klimo.parquet")
klimo_write_parquet(
  dt,
  path = out_file,
  field_spec = spec,
  declared_system = "metric",
  unit_mode = "validate",
  scaled = TRUE,
  drop_original = FALSE
)

meta <- klimo_read_parquet_meta(out_file)
meta$kv_dt
meta$spec_dt
meta$audit_dt

res <- klimo_read_parquet(out_file, metadata = "apply")
dt2 <- res$dt

str(dt2)
```

## Reference: main user-facing functions

### Parquet / Klimo metadata

- `klimo_write_parquet()`
- `klimo_spec_default()`, `klimo_spec_preview()`
- `klimo_spec_set_units()`, `klimo_spec_set_scale()`
- `klimo_read_parquet()`, `klimo_read_parquet_meta()`
- `klimo_meta_summary()`
- `klimo_decode_scaled()`, `klimo_decode_encoded()`
- `klimo_open_dataset_with_meta()`

### Synapse REST

- `syn_resolve_token()`, `syn_request()`
- `syn_resolve_entity_by_path()`
- `syn_ensure_folder_path()`, `syn_create_folder()`
- `syn_upload_file_path()`, `syn_download_file_path()`
- `syn_get_annotations()`, `syn_set_annotations()`

### Utilities

- `unit()`
- `listS3files()`
- `requireRAM()`
- `startEnvironment()`

## License

License: MIT + file LICENSE
