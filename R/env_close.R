#' Stop a Cluster Created by `env_start()`
#'
#' Gracefully stops the cluster and removes an existing temporary stop file.
#'
#' @param env_info A list returned by [env_start()].
#' @param remove_watchdog_stopfile If `TRUE`, remove an existing stop file.
#'
#' @return `NULL`, invisibly.
#' @export
#'
#' @examples
#' \dontrun{
#' env_info <- env_start(num_cores = 2)
#' env_close(env_info)
#' }
env_close <- function(env_info, remove_watchdog_stopfile = TRUE) {
  if (!is.list(env_info) || is.null(env_info$cluster)) {
    stop("`env_info` must be a list returned by env_start().", call. = FALSE)
  }
  try(parallel::stopCluster(env_info$cluster), silent = TRUE)

  stop_file <- env_info$stop_file
  if (isTRUE(remove_watchdog_stopfile) &&
      is.character(stop_file) && length(stop_file) == 1L &&
      !is.na(stop_file) && file.exists(stop_file)) {
    unlink(stop_file, force = TRUE)
  }
  invisible(NULL)
}
