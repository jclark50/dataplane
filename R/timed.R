# =============================================================================
# timed.R - simple script timer
# =============================================================================

#' timed: simple script timer
#'
#' Start/stop timers and print start/finish timestamps plus elapsed time.
#' Supports:
#' - a single unnamed script timer (default), and
#' - optional labeled timers (multiple independent timers).
#'
#' @param action Character(1). One of "start", "end", or "stop".
#'   ("stop" is treated as "end".)
#' @param label Character(1) or NULL. Optional label to manage independent timers.
#'   If NULL (default), an unnamed script timer is used.
#' @param round Integer(1). Decimal places for rounding elapsed time. Default 2.
#' @param ret Logical(1). If TRUE, returns the start time (on "start") or the
#'   elapsed difftime (on "end"/"stop"). Default FALSE.
#'
#' @return Invisibly NULL by default. If `ret = TRUE`, returns a POSIXct start time
#'   (on "start") or a difftime elapsed duration (on "end"/"stop").
#'
#' @examples
#' timed("start"); Sys.sleep(0.2); timed("end")
#' timed("start", label = "download"); Sys.sleep(0.1); timed("stop", label = "download")
#' @export
timed <- local({
  timers_env <- new.env(parent = emptyenv())

  function(action, label = NULL, round = 2, ret = FALSE) {
    action <- match.arg(action, c("start", "end", "stop"))
    if (identical(action, "stop")) action <- "end"

    has_crayon <- requireNamespace("crayon", quietly = TRUE)

    # Keep coloring best-effort + non-fatal; do not require crayon.
    if (has_crayon) {
      options(crayon.enabled = TRUE)
      if (!nzchar(Sys.getenv("R_CRAYON_ENABLED"))) Sys.setenv(R_CRAYON_ENABLED = "TRUE")
    }

    green <- if (has_crayon) crayon::green else function(x) x
    red   <- if (has_crayon) crayon::red   else function(x) x

    # -------------------------------------------------------------------------
    # Unnamed (script-wide) timer
    # -------------------------------------------------------------------------
    if (is.null(label)) {
      key <- "__unnamed__"

      if (identical(action, "start")) {
        timers_env[[key]] <- Sys.time()
        cat("Script started at:", green(format(timers_env[[key]])), "\n")
        if (isTRUE(ret)) return(timers_env[[key]])
        return(invisible(NULL))
      }

      st <- timers_env[[key]]
      if (is.null(st)) return(invisible(NULL))

      et <- Sys.time()
      elapsed <- et - st

      cat(
        "Script finished at", green(format(et)),
        "after", green(round(elapsed, round)), units(elapsed), "\n"
      )

      rm(list = key, envir = timers_env)

      if (isTRUE(ret)) return(elapsed)
      return(invisible(NULL))
    }

    # -------------------------------------------------------------------------
    # Labeled timers (multiple independent timers)
    # -------------------------------------------------------------------------
    lbl <- paste0("[", label, "]")
    lbl_col <- red(lbl)

    if (identical(action, "start")) {
      timers_env[[label]] <- Sys.time()
      cat(sprintf(
        "%s started at %s\n",
        lbl_col,
        green(format(timers_env[[label]], "%H:%M:%OS3"))
      ))
      if (isTRUE(ret)) return(timers_env[[label]])
      return(invisible(NULL))
    }

    st <- timers_env[[label]]
    if (is.null(st)) return(invisible(NULL))

    et <- Sys.time()
    elapsed <- et - st

    cat(sprintf(
      "%s finished at %s after %s %s\n",
      lbl_col,
      green(format(et, "%H:%M:%OS3")),
      green(round(elapsed, round)),
      attr(elapsed, "units")
    ))

    rm(list = label, envir = timers_env)

    if (isTRUE(ret)) return(elapsed)
    invisible(NULL)
  }
})
