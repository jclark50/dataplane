#' Check Available System RAM
#'
#' Checks whether the system has at least a requested amount of available
#' physical memory. The function returns `FALSE` after its final attempt instead
#' of terminating the R session.
#'
#' @param threshold Numeric scalar. Minimum available memory required.
#' @param unit Character. One of `"B"`, `"KB"`, `"MB"`, or `"GB"`.
#' @param wait_time Non-negative numeric scalar. Seconds between attempts.
#' @param max_attempts Positive integer. Maximum number of checks.
#' @param verbose If `TRUE`, report each check.
#'
#' @return `TRUE` when the threshold is met and `FALSE` otherwise.
#' @export
#'
#' @examples
#' check_ram(threshold = 1, unit = "MB", wait_time = 0, max_attempts = 1)
check_ram <- function(threshold,
                      unit = "MB",
                      wait_time = 60,
                      max_attempts = 3,
                      verbose = TRUE) {
  if (!is.numeric(threshold) || length(threshold) != 1L ||
      is.na(threshold) || !is.finite(threshold) || threshold < 0) {
    stop("`threshold` must be one finite, non-negative number.", call. = FALSE)
  }
  if (!is.numeric(wait_time) || length(wait_time) != 1L ||
      is.na(wait_time) || !is.finite(wait_time) || wait_time < 0) {
    stop("`wait_time` must be one finite, non-negative number.", call. = FALSE)
  }
  max_attempts <- as.integer(max_attempts)
  if (length(max_attempts) != 1L || is.na(max_attempts) || max_attempts < 1L) {
    stop("`max_attempts` must be a positive integer.", call. = FALSE)
  }
  if (!is.logical(verbose) || length(verbose) != 1L || is.na(verbose)) {
    stop("`verbose` must be TRUE or FALSE.", call. = FALSE)
  }

  multipliers <- c(B = 1, KB = 1024, MB = 1024^2, GB = 1024^3)
  unit <- toupper(as.character(unit)[1])
  if (!unit %in% names(multipliers)) {
    stop("`unit` must be one of B, KB, MB, or GB.", call. = FALSE)
  }
  multiplier <- unname(multipliers[[unit]])
  threshold_bytes <- threshold * multiplier

  get_available_ram <- function() {
    os <- Sys.info()[["sysname"]]

    if (identical(os, "Linux") && file.exists("/proc/meminfo")) {
      info <- readLines("/proc/meminfo", warn = FALSE)
      line <- grep("^MemAvailable:", info, value = TRUE)
      if (!length(line)) line <- grep("^MemFree:", info, value = TRUE)
      if (!length(line)) {
        stop("Linux memory information did not contain an available-memory field.")
      }
      value_kb <- suppressWarnings(as.numeric(sub(
        "^[^0-9]*([0-9]+).*$", "\\1", line[[1]]
      )))
      return(value_kb * 1024)
    }

    if (identical(os, "Windows")) {
      command <- paste(
        "(Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory",
        sep = ""
      )
      output <- suppressWarnings(system2(
        "powershell",
        c("-NoProfile", "-NonInteractive", "-Command", shQuote(command)),
        stdout = TRUE,
        stderr = TRUE
      ))
      status <- attr(output, "status")
      if (is.null(status)) status <- 0L
      value_kb <- if (length(output)) {
        suppressWarnings(as.numeric(trimws(output[[1]])))
      } else {
        NA_real_
      }
      if (status == 0L && is.finite(value_kb)) return(value_kb * 1024)
    }

    if (identical(os, "Darwin")) {
      output <- suppressWarnings(system2(
        "vm_stat", stdout = TRUE, stderr = TRUE
      ))
      status <- attr(output, "status")
      if (is.null(status)) status <- 0L
      if (status == 0L && length(output) > 1L) {
        page_size <- suppressWarnings(as.numeric(sub(
          ".*page size of ([0-9]+) bytes.*", "\\1", output[[1]]
        )))
        fields <- c("Pages free", "Pages inactive", "Pages speculative")
        pages <- vapply(fields, function(field) {
          line <- grep(paste0("^", field, ":"), output, value = TRUE)
          if (!length(line)) return(0)
          suppressWarnings(as.numeric(gsub("[^0-9]", "", line[[1]])))
        }, numeric(1))
        if (is.finite(page_size) && all(is.finite(pages))) {
          return(sum(pages) * page_size)
        }
      }
    }

    stop(
      "Available RAM could not be determined on this system.",
      call. = FALSE
    )
  }

  for (attempt in seq_len(max_attempts)) {
    available_bytes <- get_available_ram()
    available <- available_bytes / multiplier

    if (isTRUE(verbose)) {
      message(sprintf(
        "Attempt %d: available RAM = %s %s; threshold = %s %s",
        attempt, format(round(available, 2), big.mark = ","),
        unit, threshold, unit
      ))
    }
    if (available_bytes >= threshold_bytes) return(TRUE)
    if (attempt < max_attempts && wait_time > 0) Sys.sleep(wait_time)
  }

  FALSE
}
