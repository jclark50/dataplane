#' Check Available System RAM
#'
#' @description
#' `check_ram` verifies that the system has at least the specified amount of
#' free physical memory. It optionally retries up to `max_attempts`, waiting
#' `wait_time` seconds between attempts. If the threshold is never met, the
#' process exits with a non-zero status.
#'
#' @param threshold Numeric. Minimum amount of free memory required.
#' @param unit Character. Unit for the threshold: one of `"B"`, `"KB"`,
#'   `"MB"`, or `"GB"` (default `"MB"`).
#' @param wait_time Numeric. Seconds to wait between retries (default 60).
#' @param max_attempts Integer. Maximum number of attempts before quitting
#'   the R session with status 1 (default 3).
#'
#' @return Returns `TRUE` if available RAM meets or exceeds the threshold.
#'   Otherwise, the session is terminated via `quit(status = 1)`.
#'
#' @details
#' On Windows, uses `wmic OS get FreePhysicalMemory` to retrieve free RAM.
#' On Unix-like systems, calls `free -b` and parses the "available" column.
#' @export
#'
#' @examples
#' \dontrun{
#' # Check for at least 2 GB of free memory, retrying up to 5 times
#' check_ram(threshold = 2000, unit = "MB", wait_time = 30, max_attempts = 5)
#' }
check_ram <- function(threshold,
                      unit = "MB",
                      wait_time = 60,
                      max_attempts = 3) {
  # Convert threshold into bytes
  multiplier <- switch(toupper(unit),
                       "B"  = 1,
                       "KB" = 1024,
                       "MB" = 1024^2,
                       "GB" = 1024^3,
                       stop("Invalid unit. Use B, KB, MB, or GB."))
  threshold_bytes <- threshold * multiplier

  # Internal helper to get free RAM in bytes
  get_available_ram <- function() {
    if (.Platform$OS.type == "windows") {
      mem_info <- system2("wmic",
                          args = c("OS", "get", "FreePhysicalMemory", "/Value"),
                          stdout = TRUE)
      # Parse "FreePhysicalMemory=xxxx"
      value_line <- grep("FreePhysicalMemory", mem_info, value = TRUE)
      kb <- as.numeric(sub("FreePhysicalMemory=", "", value_line))
      return(kb * 1024)
    } else {
      mem_info <- system2("free", args = c("-b"), stdout = TRUE)
      # The 7th column of the second line is "available" on Linux
      parts <- strsplit(mem_info[2], "\\s+")[[1]]
      as.numeric(parts[7])
    }
  }

  for (attempt in seq_len(max_attempts)) {
    available_bytes <- get_available_ram()
    available <- available_bytes / multiplier

    if (available_bytes >= threshold_bytes) {
      cat(sprintf("Attempt %d: Available RAM = %s %s >= threshold %s %s\n", 
                  attempt,
                  format(available, big.mark = ","),
                  unit,
                  threshold,
                  unit))
      return(TRUE)
    }

    cat(sprintf("Attempt %d: Available RAM = %s %s < threshold %s %s; \
        waiting %s seconds...\n",
                attempt,
                format(available, big.mark = ","),
                unit,
                threshold,
                unit,
                wait_time))
    Sys.sleep(wait_time)
  }

  cat(sprintf("RAM availability remained below %s %s after %d attempts. Exiting.\n",
              threshold, unit, max_attempts))
  quit(save = "no", status = 1)
}
