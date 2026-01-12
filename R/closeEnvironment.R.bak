
################################################################################################!
################################################################################################!
#' @title Gracefully Stop the Watchdog and Cluster
#'
#' @description
#' The `closeEnvironment()` function stops the watchdog by creating the stop file,
#' then uses `jstopCluster()` to kill the associated Rscript.exe processes.
#'
#' @param env_info A list as returned by `startEnvironment()`, containing `cluster`, `stop_file`, and `worker_pids`.
#' @param remove_watchdog_stopfile Boolean. If true, removes stopfile corresponding to this watchdog/environment. Default is TRUE.
#'
#' @details
#' This function is useful for a clean shutdown after your computations are finished.
#' It signals the watchdog script to stop by creating the specified stop file, and then
#' forcefully kills the worker processes to ensure a clean slate.
#'
#' After calling `closeEnvironment()`, if you also wish to stop the parallel cluster, call:
#' \code{parallel::stopCluster(env_info$cluster)}.
#'
#' @return NULL (invisibly).
#'
#' @examples
#' \dontrun{
#' env_info <- startEnvironment(num_cores = 2)
#'
#' # ... run computations ...
#'
#' closeEnvironment(env_info)
#' # Now the watchdog should stop and the processes are terminated.
#'
#' parallel::stopCluster(env_info$cluster)
#' }
#'
#' @export
closeEnvironment <- function(env_info, remove_watchdog_stopfile = TRUE) {
  file.create(env_info$stop_file)
  jstopCluster(env_info$worker_pids)
  Sys.sleep(1)
  if (remove_watchdog_stopfile){
    file.remove(env_info$stop_file)
  } else {
    message(sprintf("Did not remove watchdog stopfile %s", env_info$stop_file))
  }
  invisible(NULL)
}
