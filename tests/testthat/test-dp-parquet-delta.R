delta_backend_or_skip <- function() {
  check <- dataplane::dp_delta_check()
  testthat::skip_if_not(check$available, check$message)
  check
}

test_that("managed delta setup validates arguments before changing the system", {
  expect_error(
    dataplane::dp_delta_setup(pyarrow_version = "latest"),
    "must look like"
  )
  expect_error(
    dataplane::dp_delta_setup(upgrade = NA),
    "must be TRUE or FALSE"
  )
  expect_error(
    dataplane::dp_delta_setup(env_dir = c("first", "second")),
    "one non-empty path"
  )
})

test_that("delta check verifies explicit-encoding support", {
  check <- delta_backend_or_skip()
  expect_true(check$available)
  expect_true(nzchar(check$python))
  expect_match(check$pyarrow_version, "^[0-9]+[.]")
})

test_that("delta writer preserves mixed Arrow values and nulls", {
  delta_backend_or_skip()
  path <- tempfile(fileext = ".parquet")
  on.exit(unlink(path, force = TRUE), add = TRUE)
  dt <- data.table::data.table(
    id = as.integer(1:10000),
    value = c(NA_real_, seq(0.1, 999.9, length.out = 9999)),
    group = rep(c("alpha", "beta", NA_character_), length.out = 10000),
    flag = rep(c(TRUE, FALSE, NA), length.out = 10000),
    day = as.Date("2025-01-01") + rep(0:30, length.out = 10000),
    instant = as.POSIXct("2025-01-01", tz = "UTC") + seq_len(10000)
  )
  expected <- arrow::as_arrow_table(dt)
  result <- dataplane::dp_write_parquet_delta(dt, path, validate_read = TRUE)
  actual <- arrow::read_parquet(path, as_data_frame = FALSE)
  expect_true(file.exists(path))
  expect_gt(result$bytes, 0)
  expect_equal(result$rows, nrow(dt))
  expect_true(result$full_read_validated)
  expect_true(actual$Equals(expected))
  expect_false(file.exists(paste0(path, ".backup")))
  expect_false(dir.exists(paste0(path, ".write-lock")))
})

test_that("delta writer atomically replaces an existing output", {
  delta_backend_or_skip()
  path <- tempfile(fileext = ".parquet")
  on.exit(unlink(c(path, paste0(path, ".backup")), force = TRUE), add = TRUE)
  dataplane::dp_write_parquet_delta(data.frame(id = 1:3), path)
  replacement <- data.frame(id = 4:8, value = seq(0.5, 2.5, by = 0.5))
  dataplane::dp_write_parquet_delta(replacement, path, validate_read = TRUE)
  actual <- arrow::read_parquet(path, as_data_frame = FALSE)
  expect_true(actual$Equals(arrow::as_arrow_table(replacement)))
  expect_false(file.exists(paste0(path, ".backup")))
})

test_that("delta writer refuses an active output lock", {
  delta_backend_or_skip()
  path <- tempfile(fileext = ".parquet")
  lock <- paste0(path, ".write-lock")
  dir.create(lock)
  on.exit(unlink(lock, recursive = TRUE, force = TRUE), add = TRUE)
  expect_error(
    dataplane::dp_write_parquet_delta(data.frame(id = 1:2), path),
    "stale lock"
  )
})

test_that("dp_write delta mode preserves Dataplane metadata", {
  delta_backend_or_skip()
  path <- tempfile(fileext = ".parquet")
  on.exit(unlink(path, force = TRUE), add = TRUE)
  dt <- data.table::data.table(temp = c(25.1, 25.4, NA_real_), rh = c(55, 52, 61))
  result <- dataplane::dp_write(
    dt, path, spec = "metric", parquet_encoding = "delta", validate_write = TRUE
  )
  read_result <- dataplane::dp_read(path)
  file_meta <- dataplane::dp_read_meta(path)
  expect_equal(result$parquet_encoding, "delta")
  expect_equal(result$write_result$rows, nrow(dt))
  expect_equal(as.numeric(read_result$dt$temp), dt$temp)
  expect_equal(as.numeric(read_result$dt$rh), dt$rh)
  expect_equal(attr(read_result$dt$temp, "units"), "degC")
  expect_equal(attr(read_result$dt$rh, "units"), "percent")
  expect_equal(file_meta$kv[["dp:parquet_encoding"]], "delta")
  expect_equal(file_meta$kv[["dp:compression"]], "zstd-6")
})

test_that("dp_write default backend remains available", {
  path <- tempfile(fileext = ".parquet")
  on.exit(unlink(path, force = TRUE), add = TRUE)
  result <- dataplane::dp_write(
    data.frame(value = 1:3), path, spec = "metric", parquet_encoding = "default"
  )
  expect_equal(result$parquet_encoding, "default")
  expect_equal(arrow::read_parquet(path)$value, 1:3)
})
