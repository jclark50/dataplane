#' Block Execution Until Sufficient RAM Is Available
#'
#' Calls [check_ram()] until the requested number of attempts has been exhausted.
#' Unlike earlier versions, this function never terminates the R process.
#'
#' @param threshold Numeric. Minimum amount of free memory required.
#' @param unit Character. One of `"B"`, `"KB"`, `"MB"`, or `"GB"`.
#' @param wait_time Numeric. Seconds between checks.
#' @param max_attempts Positive integer. Maximum number of checks.
#'
#' @return Invisibly returns `TRUE` when sufficient memory is available.
#' @export
#'
#' @examples
#' \dontrun{
#' require_ram(threshold = 4000, unit = "MB", wait_time = 10, max_attempts = 20)
#' }
require_ram <- function(threshold = 4000, unit = "MB",
                        wait_time = 10, max_attempts = 10) {
  available <- check_ram(
    threshold,
    unit = unit,
    wait_time = wait_time,
    max_attempts = max_attempts
  )
  if (!isTRUE(available)) {
    stop(
      sprintf(
        "Available RAM remained below %s %s after %s attempts.",
        threshold, unit, max_attempts
      ),
      call. = FALSE
    )
  }
  invisible(TRUE)
}
