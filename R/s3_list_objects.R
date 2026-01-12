#' Robust S3 object listing (no `Owner` assumption) with retries
#'
#' @description
#' Lists objects from an S3 bucket/prefix using **aws.s3**, avoiding the
#' `Owner$ID` assumption that can trigger `subscript out of bounds` when
#' `Owner` is omitted. Adds retries and (optionally) sorts by `LastModified`
#' and returns only the newest rows.
#'
#' @details
#' Calls [aws.s3::get_bucket()] with `parse_response = TRUE` and converts the
#' parsed list into a data frame while ignoring any `Owner` fields. Handles
#' "folder" entries by falling back to `Name` or `Prefix` when `Key` is absent.
#' Timestamps are parsed to UTC when present.
#'
#' Note: this issues a single ListObjects request (no pagination). For >1,000
#' keys, loop with `marker`/continuation tokens.
#'
#' @param bucket Character. S3 bucket name.
#' @param prefix Character, optional. Limit results to keys beginning with this prefix.
#' @param delimiter Character, optional. E.g., `"/"` to group by common prefixes.
#' @param max Integer, optional. Maximum number of keys to return (per request).
#' @param marker Character, optional. Key to start with when listing.
#' @param ... Passed through to [aws.s3::get_bucket()] (e.g., credentials, region).
#' @param max_tries Integer. Maximum attempts before failing. Default `3`.
#' @param wait Numeric (seconds). Initial wait between attempts. Default `1`.
#' @param backoff Numeric multiplier for exponential backoff. Default `1.6`.
#' @param verbose Logical. If `TRUE`, prints retry messages. Default `TRUE`.
#' @param order_by_last_modified Logical. If `TRUE`, sort ascending by `LastModified`.
#'   Default `FALSE`.
#' @param return_tail Logical. If `TRUE`, return only the last `tail_n` rows
#'   *after* optional sorting. Default `FALSE`.
#' @param tail_n Integer. Number of rows to return when `return_tail = TRUE`.
#'   Default `6`.
#'
#' @return
#' A `data.frame` with columns:
#' - `Key` (character)
#' - `Size` (numeric; `NA` for common prefixes)
#' - `LastModified` (POSIXct, UTC; `NA` if absent)
#' - `StorageClass` (character; `NA` if absent)
#'
#' @examples
#' \dontrun{
#' # Basic
#' df <- safe_get_bucket_df("my-bucket", prefix = "hrrr/20250809/")
#'
#' # Newest few objects (sorted by LastModified, tail 6)
#' newest <- safe_get_bucket_df("my-bucket", prefix = "hrrr/20250809/",
#'                              order_by_last_modified = TRUE, return_tail = TRUE, tail_n = 6)
#' }
#'
#' @seealso [aws.s3::get_bucket()], [aws.s3::save_object()]
#' @export

s3_list_objects <- function(bucket,
                        prefix = NULL,
                        delimiter = NULL,
                        max = NULL,
                        marker = NULL,
                        ...,
                        max_tries = 3,
                        wait = 1,
                        backoff = 1.6,
                        verbose = TRUE,
                        order_by_last_modified = FALSE,
                        return_tail = FALSE,
                        tail_n = 6) {
  
  `%||%` <- function(a, b) if (is.null(a) || length(a) == 0) b else a
  
  if (is.null(prefix)) {
    curdate <- format(Sys.time(), "%Y-%m-%d", tz = "UTC")
    prefix <- file.path("goes", curdate)
  }
  
  parse_list <- function(r) {
    if (length(r) == 0) {
      return(data.frame(Key = character(),
                        Size = numeric(),
                        LastModified = as.POSIXct(character()),
                        StorageClass = character(),
                        stringsAsFactors = FALSE))
    }
    rows <- lapply(r, function(x) {
      key <- x[["Key"]] %||% x[["Name"]] %||% x[["Prefix"]] %||% NA_character_
      lm  <- x[["LastModified"]]
      if (inherits(lm, "POSIXt")) {
        lm_posix <- lm
      } else if (is.character(lm) && length(lm)) {
        lm_posix <- try(as.POSIXct(lm, format = "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC"), silent = TRUE)
        if (inherits(lm_posix, "try-error") || is.na(lm_posix)) lm_posix <- as.POSIXct(lm, tz = "UTC")
      } else {
        lm_posix <- as.POSIXct(NA)
      }
      data.frame(Key = key,
                 Size = suppressWarnings(as.numeric(x[["Size"]] %||% NA)),
                 LastModified = lm_posix,
                 StorageClass = x[["StorageClass"]] %||% NA_character_,
                 stringsAsFactors = FALSE)
    })
    do.call(rbind, rows)
  }
  
  all_rows <- list()
  total_fetched <- 0L
  next_marker <- marker
  truncated <- TRUE
  
  repeat {
    # Respect user-specified `max`: fetch at most 1000 per request, stop when reached
    max_keys_this_call <- 15000
    if (!is.null(max)) {
      remaining <- as.integer(max) - total_fetched
      if (remaining <= 0L) break
      max_keys_this_call <- min(1000L, remaining)
    }
    
    # Retry wrapper for a single page
    delay <- wait
    page_res <- NULL
    for (attempt in seq_len(max_tries)) {
      page_res <- tryCatch({
        aws.s3::get_bucket(
          bucket        = bucket,
          prefix        = prefix,
          delimiter     = delimiter,
          max           = max_keys_this_call,
          marker        = next_marker,
          parse_response= TRUE,
          ...
        )
      }, error = function(e) e, warning = function(w) w)
      
      if (!inherits(page_res, "error") && !inherits(page_res, "warning")) break
      
      if (verbose) {
        message(sprintf("[s3_list_objects] attempt %d/%d failed: %s",
                        attempt, max_tries, conditionMessage(page_res)))
      }
      if (attempt < max_tries) {
        Sys.sleep(delay + stats::runif(1, 0, 0.25 * wait))
        delay <- delay * backoff
      }
    }
    if (inherits(page_res, "error") || inherits(page_res, "warning")) {
      stop("s3_list_objects() failed ", max_tries, " times for prefix: ", prefix %||% "")
    }
    
    # Parse current page
    page_df <- parse_list(page_res)
    n_new <- nrow(page_df)
    if (n_new > 0L) {
      all_rows[[length(all_rows) + 1L]] <- page_df
      total_fetched <- total_fetched + n_new
      # Update marker: S3 v1 pagination can use the last returned Key
      next_marker <- tail(page_df$Key, 1)
    }
    
    # Check truncation flag; aws.s3 stores it as an attribute on the response
    # Fallback: if returned fewer than requested and not using delimiter, likely not truncated.
    is_trunc <- FALSE
    at <- attributes(page_res)
    if (!is.null(at) && !is.null(at$IsTruncated)) {
      is_trunc <- isTRUE(at$IsTruncated)
    } else {
      # Heuristic when attribute missing: if we got < requested AND no delimiter, assume done
      is_trunc <- (n_new == max_keys_this_call) || !is.null(delimiter)
    }
    
    if (!is_trunc) break
    # If server sends a NextMarker attribute, prefer it
    if (!is.null(at) && !is.null(at$NextMarker) && nzchar(at$NextMarker)) {
      next_marker <- at$NextMarker
    }
    
    # Safety: if no progress, break to avoid infinite loop
    if (n_new == 0L) break
  }
  
  out <- if (length(all_rows)) do.call(rbind, all_rows) else {
    data.frame(Key = character(), Size = numeric(),
               LastModified = as.POSIXct(character()),
               StorageClass = character(), stringsAsFactors = FALSE)
  }
  
  # Optional ordering/tail
  if (order_by_last_modified || return_tail) {
    if (requireNamespace("data.table", quietly = TRUE)) {
      dt <- data.table::as.data.table(out)
      if (order_by_last_modified) data.table::setorder(dt, LastModified)
      if (return_tail) dt <- utils::tail(dt, n = tail_n)
      out <- as.data.frame(dt)
    } else {
      if (order_by_last_modified) out <- out[order(out$LastModified, na.last = TRUE), , drop = FALSE]
      if (return_tail) out <- utils::tail(out, n = tail_n)
    }
  }
  
  out
}
