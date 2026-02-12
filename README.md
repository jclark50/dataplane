## Dataplane: self-describing Parquet files and Arrow datasets

Dataplane is a small set of utilities for making tabular data files easier to understand *later*—by you or anyone else—without hunting for a separate data dictionary.

It focuses on three practical problems that come up all the time in real workflows:

- **Column meaning**: What does `temp` represent? Air temperature at 2 m? Surface temperature? Something else?
- **Units**: Is `wind` in m/s or mph? Is `wbgt` in °C or °F?
- **Storage quirks**: Were decimals stored normally, or stored compactly as integers with a scale factor?

Dataplane stores this information as lightweight metadata alongside your data, so files remain “self-describing” when they move between projects, machines, teams, and time.

---

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
