# dataplane 1.0.1

* Added optional explicit delta-encoded Parquet writing through PyArrow with
  `dp_write_parquet_delta()`, `dp_delta_check()`, and
  `dp_write(parquet_encoding = "delta")`.
* Updated Parquet metadata reads for compatibility with current Arrow R.

# dataplane 0.1.0

* Initial CRAN submission.
