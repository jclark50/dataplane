# dataplane 1.0.3

* Prepared the package for CRAN submission by removing an unavailable
  dependency, correcting documentation and namespace issues, and making
  process, memory, and dataset helpers portable and safely scoped.
* Added CRAN release metadata and a reproducible release-check workflow.

# dataplane 1.0.2

* Added `dp_delta_setup()` to create, validate, and remember an isolated
  PyArrow environment with one command.
* Delta backend discovery now checks the saved managed environment and verifies
  explicit Parquet encoding support.

# dataplane 1.0.1

* Added optional explicit delta-encoded Parquet writing through PyArrow with
  `dp_write_parquet_delta()`, `dp_delta_check()`, and
  `dp_write(parquet_encoding = "delta")`.
* Updated Parquet metadata reads for compatibility with current Arrow R.

# dataplane 0.1.0

* Initial CRAN submission.
