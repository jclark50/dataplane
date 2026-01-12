#' Block Execution Until Available RAM Exceeds Threshold
#'
#' \code{require_ram} pauses execution in a loop until the system reports at least
#' the specified amount of free RAM. Useful for long-running data-intensive tasks
#' that require guaranteed memory availability before proceeding.
#'
#' @param threshold Numeric. Minimum amount of free memory required (default 4000).
#' @param unit Character. Unit for memory threshold (e.g., "MB" or "GB").
#' @param wait_time Numeric. Seconds to wait between consecutive checks (default 10).
#' @param max_attempts Integer. Maximum number of check attempts before giving up (default 10).
#'
#' @return Invisibly returns \code{TRUE} once the available RAM meets or exceeds the threshold.
#'
#' @details This function relies on \code{check_ram()} to query system memory.
#' It will repeatedly sleep for \code{wait_time} seconds and retry until
#' the available RAM is sufficient or the maximum number of attempts is reached.
#'
#' @export
#'
#' @examples
#' \dontrun{
#' # Wait for at least 4 GB free memory before starting heavy computation
#' require_ram(threshold = 4000, unit = "MB", wait_time = 10, max_attempts = 20)
#' }
require_ram <- function(threshold = 4000, unit = "MB",
                         wait_time = 10, max_attempts = 10) {
  while (!check_ram(threshold, unit = unit,
                    wait_time = wait_time,
                    max_attempts = max_attempts)) {
    message(sprintf("Waiting for ≥%s %s free… retrying in %ss",
                    threshold, unit, wait_time))
  }
  invisible(TRUE)
}
