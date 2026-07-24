#' Stop Explicitly Identified Worker Processes
#'
#' Sends a termination signal only to the supplied process identifiers. The
#' function deliberately refuses a missing or empty PID vector and never kills
#' processes by name.
#'
#' @param workerpids Positive integer vector of worker process identifiers.
#' @param signal Integer signal passed to [tools::pskill()].
#'
#' @return `NULL`, invisibly.
#' @export
#'
#' @examples
#' \dontrun{
#' stop_cluster(c(12345L, 12346L))
#' }
stop_cluster <- function(workerpids, signal = 15L) {
  if (missing(workerpids) || is.null(workerpids) || !length(workerpids)) {
    stop("`workerpids` must explicitly identify at least one process.", call. = FALSE)
  }
  workerpids <- suppressWarnings(as.integer(workerpids))
  if (anyNA(workerpids) || any(workerpids <= 0L)) {
    stop("`workerpids` must contain only positive integers.", call. = FALSE)
  }
  if (Sys.getpid() %in% workerpids) {
    stop("Refusing to terminate the current R process.", call. = FALSE)
  }
  signal <- as.integer(signal)
  if (length(signal) != 1L || is.na(signal)) {
    stop("`signal` must be one integer.", call. = FALSE)
  }

  for (pid in unique(workerpids)) {
    try(tools::pskill(pid, signal = signal), silent = TRUE)
  }
  invisible(NULL)
}
