
#' @title Forcefully Stop Worker Processes
#'
#' @description
#' `stop_cluster()` attempts to kill the processes by PID.
#' On Windows, uses `taskkill /F /PID`.
#' On Linux, uses `kill -9`.
#'
#' @param workerpids Integer vector of worker PIDs. If `NULL`, kills all `Rscript.exe` (Windows)
#'   or `R` (Linux) processes. Use with caution.
#'
#' @return NULL invisibly.
#'
#' @examples
#' \dontrun{
#' stop_cluster(env_info$worker_pids)
#' }
#'
#' @export
# stop_cluster <- function(workerpids = NULL) {
  # if (is.null(workerpids)) {
    # # Kill all Rscript.exe processes
    # system("taskkill /F /IM Rscript.exe")
  # } else {
    # workerpids <- paste(workerpids, collapse = "|")  # create pattern like "23964|10116"
    # procs <- system("tasklist", intern = TRUE)
    # pwsh_line <- grep(workerpids, procs, value = TRUE)
    # if (length(pwsh_line) > 0) {
      # # Extract PID from lines containing these worker PIDs
      # pid <- sub(".*Rscript.exe\\s+([0-9]+).*", "\\1", pwsh_line)
      # # Now kill each PID found
      # for (pp in pid) {
        # system(paste("taskkill /F /PID", pp))
      # }
    # }
  # }
  # invisible(NULL)
# }
stop_cluster <- function(workerpids = NULL) {
  os <- Sys.info()[["sysname"]]
  if (is.null(workerpids)) {
    if (os == "Windows") {
      # Kill all Rscript.exe processes
      system("taskkill /F /IM Rscript.exe")
    } else {
      # Linux: kill all R processes (use with caution)
      system("pkill -9 R")
    }
  } else {
    for (pp in workerpids) {
      if (os == "Windows") {
        # Check if process is running and kill
        system(sprintf("taskkill /F /PID %d", pp))
      } else {
        # Linux
        if (system2("ps", c("-p", as.character(pp)), stdout = FALSE, stderr = FALSE) == 0) {
          system(sprintf("kill -9 %d", pp))
        }
      }
    }
  }
  invisible(NULL)
}
