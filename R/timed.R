# internal state (no roxygen here; do NOT put this before @export)
#.jj_timers <- new.env(parent = emptyenv())

.timers_env <- new.env(parent = emptyenv())
#' timed: simple script timer
#'
#' Start/stop a single unnamed timer for the whole script. Prints start/finish
#' timestamps and elapsed time. Optionally returns the timestamp/elapsed.
#'
#' @param what One of "start", "end", or "stop".
#' @param round Integer(1). Decimal places for rounding elapsed time. Default 2.
#' @param ret   Logical(1). If TRUE, returns start time (on "start") or elapsed
#'   difftime (on "end"/"stop"). Default FALSE.
#'
#' @return Invisibly NULL by default; if `ret = TRUE`, a POSIXct (start) or
#'   difftime (end/stop).
#' @examples
#' timed("start"); Sys.sleep(0.2); timed("end")
#' @export
timed <- local({
    .timers_env <- new.env(parent = emptyenv())
    
    function(action, label = NULL, round = 2, ret = FALSE) {
      action <- match.arg(action, c("start", "end", "stop"))
      if (action == "stop") action <- "end"
      
      has_crayon <- requireNamespace("crayon", quietly = TRUE)
      
      if (has_crayon) {
        options(crayon.enabled = TRUE)
        if (!nzchar(Sys.getenv("R_CRAYON_ENABLED"))) Sys.setenv(R_CRAYON_ENABLED = "TRUE")
      }
      
      green <- if (has_crayon) crayon::green else function(x) x
      red   <- if (has_crayon) crayon::red   else function(x) x
      
      if (is.null(label)) {
        if (action == "start") {
          .timers_env[["__unnamed__"]] <- Sys.time()
          cat("Script started at:", green(format(.timers_env[["__unnamed__"]])), "\n")
          if (ret) return(.timers_env[["__unnamed__"]])
        } else {
          st <- .timers_env[["__unnamed__"]]
          if (is.null(st)) return(NULL)
          et <- Sys.time()
          elapsed <- et - st
          cat("Script finished at", green(format(et)),
              "after", green(round(elapsed, round)), units(elapsed), "\n")
          rm(list = "__unnamed__", envir = .timers_env)
          if (ret) return(elapsed)
        }
        return(invisible(NULL))
      }
      
      lbl <- paste0("[", label, "]")
      lbl_col <- red(lbl)
      
      if (action == "start") {
        .timers_env[[label]] <- Sys.time()
        cat(sprintf("%s started at %s\n",
                    lbl_col,
                    green(format(.timers_env[[label]], "%H:%M:%OS3"))))
        if (ret) return(.timers_env[[label]])
      } else {
        st <- .timers_env[[label]]
        if (is.null(st)) return(NULL)
        et <- Sys.time()
        elapsed <- et - st
        cat(sprintf("%s finished at %s after %s %s\n",
                    lbl_col,
                    green(format(et, "%H:%M:%OS3")),
                    green(round(elapsed, round)),
                    attr(elapsed, "units")))
        rm(list = label, envir = .timers_env)
        if (ret) return(elapsed)
      }
      
      invisible(NULL)
    }
})