#' Check availability of the optional delta-Parquet backend
#'
#' @param python Optional path to a Python executable. Resolution otherwise uses
#'   `getOption("dataplane.python")`, `DATAPLANE_PYTHON`, then `PATH`.
#' @return A list describing whether Python, PyArrow, and the bundled helper are available.
#' @export
dp_delta_check <- function(python = NULL) {
  helper <- system.file("python", "write_delta_parquet.py", package = "dataplane")
  if (!nzchar(helper)) {
    source_helper <- file.path("inst", "python", "write_delta_parquet.py")
    if (file.exists(source_helper)) helper <- normalizePath(source_helper, winslash = "/")
  }

  candidates <- unique(c(
    python,
    getOption("dataplane.python", NULL),
    Sys.getenv("DATAPLANE_PYTHON", unset = ""),
    Sys.which("python"),
    Sys.which("python3")
  ))
  candidates <- candidates[!is.na(candidates) & nzchar(candidates)]
  existing <- candidates[file.exists(candidates)]
  resolved_python <- if (length(existing)) normalizePath(existing[[1]], winslash = "/") else ""

  result <- list(
    available = FALSE,
    python = resolved_python,
    helper = helper,
    pyarrow_version = NA_character_,
    message = NULL
  )
  if (!nzchar(helper) || !file.exists(helper)) {
    result$message <- "The bundled write_delta_parquet.py helper was not found."
    return(result)
  }
  if (!nzchar(resolved_python)) {
    result$message <- paste0(
      "No Python executable was found. Supply `python=`, set option `dataplane.python`, ",
      "or set DATAPLANE_PYTHON."
    )
    return(result)
  }

  check <- suppressWarnings(system2(
    resolved_python,
    c("-c", shQuote("import pyarrow; print(pyarrow.__version__)")),
    stdout = TRUE,
    stderr = TRUE
  ))
  status <- attr(check, "status")
  if (is.null(status)) status <- 0L
  if (status != 0L) {
    result$message <- paste0(
      "PyArrow is unavailable in ", resolved_python, ". Install it with: `",
      resolved_python, " -m pip install pyarrow`. Python reported: ",
      paste(check, collapse = " ")
    )
    return(result)
  }

  result$available <- TRUE
  result$pyarrow_version <- if (length(check)) trimws(check[[1]]) else NA_character_
  result$message <- "Delta-Parquet backend is available."
  result
}


#' Write a Parquet file with explicit lossless numeric encodings
#'
#' Integer columns use `DELTA_BINARY_PACKED`; floating-point columns use
#' `BYTE_STREAM_SPLIT`; character and dictionary columns retain dictionary
#' encoding. The function stages Arrow IPC, validates the Parquet metadata, and
#' atomically replaces the destination only after a successful write.
#'
#' @param x A `data.frame`, `data.table`, or Arrow `Table`.
#' @param path Output `.parquet` path.
#' @param python Optional Python executable; see [dp_delta_check()].
#' @param compression_level ZSTD compression level from 1 through 22.
#' @param row_group_size Positive number of rows per Parquet row group.
#' @param validate_read If `TRUE`, read the completed temporary Parquet in R and
#'   compare every value and null with the input Arrow table.
#' @return Invisibly returns write metadata including path, size, row count, and timing.
#' @export
dp_write_parquet_delta <- function(
    x,
    path,
    python = NULL,
    compression_level = 6L,
    row_group_size = 1000000L,
    validate_read = FALSE) {
  .dp_require("arrow")

  check <- dp_delta_check(python)
  if (!isTRUE(check$available)) .dp_stop("dp_write_parquet_delta(): %s", check$message)
  if (!.dp_is_scalar_chr(path)) .dp_stop("dp_write_parquet_delta(): `path` must be one non-empty path.")
  compression_level <- as.integer(compression_level)
  row_group_size <- as.integer(row_group_size)
  if (length(compression_level) != 1L || is.na(compression_level) ||
      compression_level < 1L || compression_level > 22L) {
    .dp_stop("dp_write_parquet_delta(): `compression_level` must be between 1 and 22.")
  }
  if (length(row_group_size) != 1L || is.na(row_group_size) || row_group_size < 1L) {
    .dp_stop("dp_write_parquet_delta(): `row_group_size` must be positive.")
  }

  path <- normalizePath(path, winslash = "/", mustWork = FALSE)
  output_dir <- dirname(path)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  if (!dir.exists(output_dir)) .dp_stop("Could not create output directory: %s", output_dir)

  table <- if (inherits(x, "Table")) x else arrow::as_arrow_table(x)
  expected_rows <- table$num_rows
  token <- paste0(Sys.getpid(), format(Sys.time(), "%Y%m%d%H%M%OS6"))
  token <- gsub("[^0-9]", "", token)
  staging <- paste0(path, ".arrow-part-", token)
  partial <- paste0(path, ".parquet-part-", token)
  backup <- paste0(path, ".backup")
  lock_dir <- paste0(path, ".write-lock")
  finalized <- FALSE

  if (!dir.create(lock_dir, showWarnings = FALSE)) {
    .dp_stop("Another write may be using this output, or a stale lock exists: %s", lock_dir)
  }
  on.exit({
    unlink(c(staging, partial), force = TRUE)
    if (!finalized && file.exists(backup) && !file.exists(path)) file.rename(backup, path)
    unlink(lock_dir, recursive = TRUE, force = TRUE)
  }, add = TRUE)

  if (file.exists(backup)) {
    if (file.exists(path)) {
      .dp_stop("Both output and recovery backup exist; inspect them before continuing: %s", path)
    }
    if (!file.rename(backup, path)) .dp_stop("Could not restore recovery backup: %s", backup)
  }

  started <- Sys.time()
  arrow::write_feather(
    table,
    staging,
    compression = "uncompressed",
    chunk_size = row_group_size
  )
  if (!file.exists(staging) || is.na(file.info(staging)$size) || file.info(staging)$size == 0) {
    .dp_stop("Arrow IPC staging write failed: %s", staging)
  }

  messages <- suppressWarnings(system2(
    check$python,
    c(
      shQuote(check$helper),
      "--input", shQuote(staging),
      "--output", shQuote(partial),
      "--expected-rows", expected_rows,
      "--compression-level", compression_level,
      "--row-group-size", row_group_size
    ),
    stdout = TRUE,
    stderr = TRUE
  ))
  status <- attr(messages, "status")
  if (is.null(status)) status <- 0L
  if (status != 0L) {
    .dp_stop("Delta Parquet helper failed: %s", paste(messages, collapse = "\n"))
  }
  if (!file.exists(partial) || is.na(file.info(partial)$size) || file.info(partial)$size == 0) {
    .dp_stop("Delta Parquet helper produced no usable output: %s", partial)
  }

  if (isTRUE(validate_read)) {
    read_back <- arrow::read_parquet(partial, as_data_frame = FALSE)
    if (!read_back$Equals(table)) .dp_stop("Parquet read-back differs from the input table.")
  }

  unlink(staging, force = TRUE)
  if (file.exists(staging)) .dp_stop("Could not remove staging file: %s", staging)

  if (file.exists(path) && !file.rename(path, backup)) {
    .dp_stop("Could not move existing output to recovery backup: %s", path)
  }
  if (!file.rename(partial, path)) {
    if (file.exists(backup)) file.rename(backup, path)
    .dp_stop("Could not finalize Parquet output: %s", path)
  }
  finalized <- TRUE
  if (file.exists(backup)) {
    unlink(backup, force = TRUE)
    if (file.exists(backup)) .dp_warn("New output is complete, but backup removal failed: %s", backup)
  }

  elapsed <- as.numeric(difftime(Sys.time(), started, units = "secs"))
  invisible(list(
    path = path,
    rows = expected_rows,
    bytes = unname(file.info(path)$size),
    write_seconds = elapsed,
    integer_encoding = "DELTA_BINARY_PACKED",
    floating_encoding = "BYTE_STREAM_SPLIT",
    compression = paste0("zstd-", compression_level),
    row_group_size = row_group_size,
    full_read_validated = isTRUE(validate_read),
    python = check$python,
    pyarrow_version = check$pyarrow_version
  ))
}

