#' Start a Small Cross-Platform Parallel Cluster
#'
#' Creates a base R PSOCK cluster and, when available, registers it with
#' `doParallel`. Earlier development versions launched external watchdog
#' scripts; the CRAN-safe implementation does not create or start external
#' scripts.
#'
#' @param num_cores Positive integer no greater than two.
#' @param threshold Retained for backward compatibility; no watchdog is started.
#' @param consecutive_seconds Retained for backward compatibility.
#' @param stop_file Optional temporary stop-file path retained for compatibility.
#' @param windows_path Deprecated and ignored.
#' @param linux_path Deprecated and ignored.
#' @param quiet If `TRUE`, suppress informational messages.
#'
#' @return A list containing `cluster`, `stop_file`, `worker_pids`, and
#'   `watchdog_started`.
#' @export
#'
#' @examples
#' \dontrun{
#' env_info <- env_start(num_cores = 2)
#' on.exit(env_close(env_info), add = TRUE)
#' }
env_start <- function(num_cores = 2,
                      threshold = 0.99,
                      consecutive_seconds = 15,
                      stop_file = tempfile("stop_watchdog_"),
                      windows_path = NULL,
                      linux_path = NULL,
                      quiet = FALSE) {
  num_cores <- as.integer(num_cores)
  if (length(num_cores) != 1L || is.na(num_cores) ||
      num_cores < 1L || num_cores > 2L) {
    stop("`num_cores` must be 1 or 2.", call. = FALSE)
  }
  if (!is.logical(quiet) || length(quiet) != 1L || is.na(quiet)) {
    stop("`quiet` must be TRUE or FALSE.", call. = FALSE)
  }
  if (!is.null(windows_path) || !is.null(linux_path)) {
    warning(
      "`windows_path` and `linux_path` are deprecated and ignored.",
      call. = FALSE
    )
  }

  cluster <- parallel::makeCluster(num_cores)
  ready <- FALSE
  on.exit({
    if (!ready) try(parallel::stopCluster(cluster), silent = TRUE)
  }, add = TRUE)

  if (requireNamespace("doParallel", quietly = TRUE)) {
    doParallel::registerDoParallel(cluster)
  } else if (!isTRUE(quiet)) {
    message(
      "Package 'doParallel' is not installed; returning an unregistered cluster."
    )
  }

  worker_pids <- parallel::parSapply(
    cluster,
    seq_len(num_cores),
    function(index) Sys.getpid()
  )
  ready <- TRUE

  if (!isTRUE(quiet)) {
    message("Cluster started with ", num_cores, " worker(s).")
  }
  list(
    cluster = cluster,
    stop_file = stop_file,
    worker_pids = unname(as.integer(worker_pids)),
    watchdog_started = FALSE,
    threshold = threshold,
    consecutive_seconds = consecutive_seconds
  )
}
