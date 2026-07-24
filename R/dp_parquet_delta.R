.dp_delta_config_file <- function() {
  file.path(tools::R_user_dir("dataplane", "config"), "delta-python")
}

.dp_delta_managed_dir <- function() {
  file.path(tools::R_user_dir("dataplane", "cache"), "delta-python")
}

.dp_delta_venv_python <- function(env_dir) {
  if (.Platform$OS.type == "windows") {
    file.path(env_dir, "Scripts", "python.exe")
  } else {
    file.path(env_dir, "bin", "python")
  }
}

.dp_delta_saved_python <- function() {
  config_file <- .dp_delta_config_file()
  if (!file.exists(config_file)) return("")
  value <- tryCatch(
    trimws(readLines(config_file, warn = FALSE, n = 1L)),
    error = function(e) ""
  )
  if (length(value) && nzchar(value[[1]])) value[[1]] else ""
}

.dp_delta_resolve_python <- function(candidates) {
  candidates <- unique(as.character(candidates))
  candidates <- candidates[!is.na(candidates) & nzchar(candidates)]
  for (candidate in candidates) {
    expanded <- path.expand(candidate)
    resolved <- if (file.exists(expanded)) expanded else Sys.which(candidate)
    if (length(resolved) && nzchar(resolved[[1]]) && file.exists(resolved[[1]])) {
      resolved <- normalizePath(resolved[[1]], winslash = "/", mustWork = TRUE)
      probe <- tryCatch(
        suppressWarnings(system2(
          resolved,
          "--version",
          stdout = TRUE,
          stderr = TRUE
        )),
        error = function(e) structure(conditionMessage(e), status = 1L)
      )
      status <- attr(probe, "status")
      if (is.null(status)) status <- 0L
      if (status == 0L) return(resolved)
    }
  }
  ""
}

.dp_delta_run <- function(python, args) {
  output <- tryCatch(
    suppressWarnings(system2(
      python,
      args,
      stdout = TRUE,
      stderr = TRUE
    )),
    error = function(e) structure(conditionMessage(e), status = 1L)
  )
  status <- attr(output, "status")
  if (is.null(status)) status <- 0L
  list(status = as.integer(status), output = output)
}

.dp_delta_save_python <- function(python) {
  config_file <- .dp_delta_config_file()
  dir.create(dirname(config_file), recursive = TRUE, showWarnings = FALSE)
  if (!dir.exists(dirname(config_file))) {
    .dp_stop("Could not create Dataplane configuration directory: %s", dirname(config_file))
  }
  temporary <- tempfile("delta-python-", tmpdir = dirname(config_file))
  on.exit(unlink(temporary, force = TRUE), add = TRUE)
  writeLines(python, temporary, useBytes = TRUE)
  if (!file.copy(temporary, config_file, overwrite = TRUE)) {
    .dp_stop("Could not save the managed Python path: %s", config_file)
  }
  invisible(config_file)
}


#' Set up a private Python environment for delta-encoded Parquet
#'
#' Creates or reuses a Dataplane-managed Python virtual environment, installs a
#' compatible PyArrow release, validates the explicit Parquet encodings with a
#' lossless round trip, and saves the environment for future R sessions.
#'
#' This function is deliberately opt-in because it downloads a Python package.
#' It does not modify the system Python environment.
#'
#' @param python Optional base Python executable used to create the environment.
#'   When omitted, `DATAPLANE_BOOTSTRAP_PYTHON`, `python3`, and `python` are
#'   considered in that order.
#' @param env_dir Optional virtual-environment directory. The default is a
#'   Dataplane directory returned by [tools::R_user_dir()].
#' @param pyarrow_version Optional exact PyArrow version, such as `"24.0.0"`.
#'   The default installs a compatible release (`pyarrow>=14.0.0`).
#' @param upgrade If `TRUE`, ask pip to upgrade an existing compatible PyArrow.
#' @param recreate If `TRUE`, remove and recreate an existing managed virtual
#'   environment. Removal is permitted only when `pyvenv.cfg` is present.
#' @param validate If `TRUE`, write and fully read back a small delta-encoded
#'   Parquet file before saving the configuration.
#' @param quiet If `TRUE`, suppress progress messages from this function.
#' @return Invisibly returns setup details, including the managed Python path
#'   and installed PyArrow version.
#' @export
dp_delta_setup <- function(
    python = NULL,
    env_dir = NULL,
    pyarrow_version = NULL,
    upgrade = FALSE,
    recreate = FALSE,
    validate = TRUE,
    quiet = FALSE) {
  .dp_require("arrow")

  for (argument in c("upgrade", "recreate", "validate", "quiet")) {
    value <- get(argument)
    if (!is.logical(value) || length(value) != 1L || is.na(value)) {
      .dp_stop("dp_delta_setup(): `%s` must be TRUE or FALSE.", argument)
    }
  }
  if (!is.null(pyarrow_version) &&
      (!.dp_is_scalar_chr(pyarrow_version) ||
       !grepl("^[0-9]+([.][0-9]+){1,3}$", pyarrow_version))) {
    .dp_stop("dp_delta_setup(): `pyarrow_version` must look like \"24.0.0\".")
  }

  env_dir <- env_dir %||% .dp_delta_managed_dir()
  if (!.dp_is_scalar_chr(env_dir)) {
    .dp_stop("dp_delta_setup(): `env_dir` must be one non-empty path.")
  }
  env_dir <- normalizePath(path.expand(env_dir), winslash = "/", mustWork = FALSE)
  env_python <- .dp_delta_venv_python(env_dir)

  if (isTRUE(recreate) && dir.exists(env_dir)) {
    marker <- file.path(env_dir, "pyvenv.cfg")
    if (!file.exists(marker)) {
      .dp_stop(
        "dp_delta_setup(): refusing to remove `%s` because it is not a Python virtual environment.",
        env_dir
      )
    }
    unlink(env_dir, recursive = TRUE, force = TRUE)
    if (dir.exists(env_dir)) {
      .dp_stop("dp_delta_setup(): could not remove the existing environment: %s", env_dir)
    }
  }

  created <- FALSE
  if (!file.exists(env_python)) {
    if (dir.exists(env_dir) && length(list.files(env_dir, all.files = TRUE, no.. = TRUE))) {
      .dp_stop(
        paste0(
          "dp_delta_setup(): `%s` exists but is not a usable virtual environment. ",
          "Choose another `env_dir` or use `recreate = TRUE` for a valid virtual environment."
        ),
        env_dir
      )
    }
    base_python <- .dp_delta_resolve_python(c(
      python,
      Sys.getenv("DATAPLANE_BOOTSTRAP_PYTHON", unset = ""),
      Sys.getenv("DATAPLANE_PYTHON", unset = ""),
      "python3",
      "python"
    ))
    if (!nzchar(base_python)) {
      .dp_stop(
        paste0(
          "dp_delta_setup(): no base Python was found. Install Python 3 and its venv module, ",
          "or supply `python = \"/path/to/python3\"`."
        )
      )
    }
    dir.create(dirname(env_dir), recursive = TRUE, showWarnings = FALSE)
    if (!isTRUE(quiet)) message("Creating Dataplane Python environment at ", env_dir)
    creation <- .dp_delta_run(
      base_python,
      c("-m", "venv", shQuote(env_dir))
    )
    if (creation$status != 0L || !file.exists(env_python)) {
      unlink(env_dir, recursive = TRUE, force = TRUE)
      .dp_stop(
        paste0(
          "dp_delta_setup(): virtual-environment creation failed. On Debian/Ubuntu, ",
          "install `python3-venv`. Python reported:\n%s"
        ),
        paste(creation$output, collapse = "\n")
      )
    }
    created <- TRUE
  }

  pip_check <- .dp_delta_run(env_python, c("-m", "pip", "--version"))
  if (pip_check$status != 0L) {
    ensure_pip <- .dp_delta_run(env_python, c("-m", "ensurepip", "--upgrade"))
    if (ensure_pip$status != 0L) {
      .dp_stop(
        "dp_delta_setup(): pip is unavailable in the managed environment:\n%s",
        paste(c(pip_check$output, ensure_pip$output), collapse = "\n")
      )
    }
  }

  requirement <- if (is.null(pyarrow_version)) {
    "pyarrow>=14.0.0"
  } else {
    paste0("pyarrow==", pyarrow_version)
  }
  install_args <- c("-m", "pip", "install", "--disable-pip-version-check")
  if (isTRUE(upgrade)) install_args <- c(install_args, "--upgrade")
  install_args <- c(install_args, shQuote(requirement))
  if (!isTRUE(quiet)) message("Installing ", requirement)
  installation <- .dp_delta_run(env_python, install_args)
  if (installation$status != 0L) {
    .dp_stop(
      "dp_delta_setup(): PyArrow installation failed:\n%s",
      paste(installation$output, collapse = "\n")
    )
  }

  check <- dp_delta_check(python = env_python)
  if (!isTRUE(check$available)) {
    .dp_stop("dp_delta_setup(): managed environment validation failed: %s", check$message)
  }

  if (isTRUE(validate)) {
    smoke_path <- tempfile(fileext = ".parquet")
    on.exit(unlink(smoke_path, force = TRUE), add = TRUE)
    smoke_data <- data.frame(
      id = 1:8,
      value = c(1.5, NA_real_, seq(2, 7)),
      label = rep(c("a", "b"), 4),
      stringsAsFactors = FALSE
    )
    dp_write_parquet_delta(
      smoke_data,
      smoke_path,
      python = env_python,
      row_group_size = 4L,
      validate_read = TRUE
    )
  }

  config_file <- .dp_delta_save_python(env_python)
  if (!isTRUE(quiet)) {
    message("Dataplane delta backend is ready (PyArrow ", check$pyarrow_version, ").")
  }
  invisible(list(
    available = TRUE,
    created = created,
    python = env_python,
    env_dir = env_dir,
    config_file = config_file,
    pyarrow_version = check$pyarrow_version,
    validated = isTRUE(validate)
  ))
}


#' Check availability of the optional delta-Parquet backend
#'
#' @param python Optional path to a Python executable. Resolution otherwise uses
#'   `getOption("dataplane.python")`, `DATAPLANE_PYTHON`, the saved path from
#'   [dp_delta_setup()], the default managed environment, then `PATH`.
#' @return A list describing whether Python, PyArrow, required encoding support,
#'   and the bundled helper are available.
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
    .dp_delta_saved_python(),
    .dp_delta_venv_python(.dp_delta_managed_dir()),
    "python3",
    "python"
  ))
  resolved_python <- .dp_delta_resolve_python(candidates)

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
      "No Python executable was found. Run `dp_delta_setup()`, supply `python=`, ",
      "set option `dataplane.python`, or set DATAPLANE_PYTHON."
    )
    return(result)
  }

  check <- .dp_delta_run(
    resolved_python,
    c(
      "-c",
      shQuote(paste0(
        "import inspect, pyarrow; ",
        "from pyarrow import parquet as pq; ",
        "assert 'column_encoding' in inspect.signature(pq.ParquetWriter).parameters; ",
        "print(pyarrow.__version__)"
      ))
    )
  )
  if (check$status != 0L) {
    result$message <- paste0(
      "Compatible PyArrow is unavailable in ", resolved_python,
      ". Run `dp_delta_setup()` or install a current PyArrow release. Python reported: ",
      paste(check$output, collapse = " ")
    )
    return(result)
  }

  result$available <- TRUE
  result$pyarrow_version <- if (length(check$output)) trimws(check$output[[1]]) else NA_character_
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
