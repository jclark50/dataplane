# synapse_rest_backend.R
# Pure REST (httr2) helpers for Synapse file/folder ops + annotations2.
# Intended for inclusion in an R package (roxygen2-ready).
#
# Dependencies (Imports): httr2, jsonlite, tools
# Suggested (Suggests): digest
#
# Notes:
# - All networked examples are wrapped in \dontrun{}.
# - `token` can be provided explicitly or via env vars:
#   - SYNAPSE_PAT (preferred)
#   - SYNAPSE_AUTH_TOKEN (fallback)

# ==============================================================================
# Utilities (internal)
# ==============================================================================

#' Null/empty coalesce
#'
#' Internal helper: return `b` when `a` is `NULL` or length 0; otherwise return `a`.
#' This is intentionally different from `.syn_coalesce()` which also treats some scalar
#' NA/"" as missing.
#'
#' @param a,b Objects to coalesce.
#' @return `a` if present, otherwise `b`.
#' @keywords internal
`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L) b else a

#' Coalesce with scalar NA/empty-string handling
#'
#' Internal helper: return `b` when `a` is `NULL`/len0. If `a` is scalar, also treat
#' `NA` as missing; if scalar character, treat `""` as missing.
#'
#' @param a,b Objects to coalesce.
#' @return `a` if present, otherwise `b`.
#' @keywords internal
.syn_coalesce <- function(a, b) {
  if (is.null(a) || length(a) == 0L) return(b)
  if (length(a) == 1L) {
    if (is.na(a)) return(b)
    if (is.character(a) && identical(a, "")) return(b)
  }
  a
}

#' Test for Synapse IDs
#' @param x Object.
#' @return Logical scalar.
#' @keywords internal
.syn_is_id <- function(x) is.character(x) && length(x) == 1L && grepl("^syn\\d+$", x)

#' Test for scalar character
#' @param x Object.
#' @return Logical scalar.
#' @keywords internal
.syn_is_scalar_chr <- function(x) is.character(x) && length(x) == 1L && !is.na(x)

#' Base URL for Synapse repository services
#' @return A length-1 character URL.
#' @keywords internal
.syn_base_url <- function() "https://repo-prod.prod.sagebase.org"

#' Message helper
#' @param verbose Logical.
#' @param ... Passed to `message()`.
#' @keywords internal
.syn_verbose <- function(verbose, ...) if (isTRUE(verbose)) message(...)

#' Error helper
#' @param ... Concatenated into a single message.
#' @keywords internal
.syn_stop <- function(...) stop(paste0(...), call. = FALSE)

#' Normalize a remote path
#'
#' Converts backslashes to slashes, collapses repeated slashes, trims leading/trailing
#' slashes.
#'
#' @param path Character scalar.
#' @return Normalized character scalar.
#' @keywords internal
.syn_norm_path <- function(path) {
  path <- gsub("\\\\", "/", path)
  path <- gsub("/{2,}", "/", path)
  path <- gsub("^/+", "", path)
  path <- gsub("/+$", "", path)
  path
}

#' Split a remote path into segments
#' @param path Character scalar.
#' @return Character vector of non-empty segments.
#' @keywords internal
.syn_split_path <- function(path) {
  path <- .syn_norm_path(path)
  if (!nzchar(path)) return(character())
  parts <- strsplit(path, "/", fixed = TRUE)[[1]]
  parts[nzchar(parts)]
}

#' Validate a single folder path segment
#' @param seg Character scalar.
#' @return Invisibly `TRUE` if valid; otherwise errors.
#' @keywords internal
.syn_validate_segment <- function(seg) {
  if (!nzchar(seg)) .syn_stop("Invalid folder path segment: empty")
  if (grepl("[[:cntrl:]]", seg)) .syn_stop("Invalid folder path segment: control characters not allowed")
  if (grepl("/", seg, fixed = TRUE)) .syn_stop("Invalid folder path segment: must not contain '/'")
  invisible(TRUE)
}

#' Guess content type from file extension
#' @param path Local file path.
#' @return MIME type string.
#' @keywords internal
.syn_guess_content_type <- function(path) {
  ext <- tolower(tools::file_ext(path))
  if (ext %in% c("csv")) return("text/csv")
  if (ext %in% c("tsv")) return("text/tab-separated-values")
  if (ext %in% c("json")) return("application/json")
  # Parquet and most binary formats are fine as octet-stream
  "application/octet-stream"
}

#' MD5 helpers (Suggests: digest)
#' @keywords internal
.syn_md5_hex_file <- function(path) {
  if (!requireNamespace("digest", quietly = TRUE)) {
    .syn_stop("Package 'digest' is required for md5=TRUE. Install it or set md5=FALSE.")
  }
  digest::digest(file = path, algo = "md5", serialize = FALSE)
}

#' MD5 helpers (Suggests: digest)
#' @keywords internal
.syn_md5_hex_raw <- function(raw) {
  if (!requireNamespace("digest", quietly = TRUE)) {
    .syn_stop("Package 'digest' is required for md5=TRUE. Install it or set md5=FALSE.")
  }
  digest::digest(raw, algo = "md5", serialize = FALSE)
}

#' Normalize "list of objects" payload shapes
#'
#' Synapse responses sometimes arrive as list-of-lists or as a simplified data.frame.
#'
#' @param x Object.
#' @return List of row-like lists.
#' @keywords internal
.syn_as_list_of_rows <- function(x) {
  if (is.null(x)) return(list())
  
  if (is.data.frame(x)) {
    if (nrow(x) == 0) return(list())
    return(lapply(seq_len(nrow(x)), function(i) as.list(x[i, , drop = TRUE])))
  }
  
  if (is.list(x) && length(x) && is.list(x[[1]])) return(x)
  if (is.list(x)) return(list(x))
  list()
}

# ==============================================================================
# Token resolution (exported)
# ==============================================================================

#' Resolve a Synapse Personal Access Token (PAT)
#'
#' Returns a token from (in order):
#' 1) `token` argument (if a long-ish scalar string)
#' 2) `Sys.getenv("SYNAPSE_PAT")` (preferred)
#' 3) `Sys.getenv("SYNAPSE_AUTH_TOKEN")` (fallback)
#'
#' @param token Optional token string.
#' @return A character scalar token.
#' @export
#' @examples
#' \dontrun{
#' Sys.setenv(SYNAPSE_PAT = "YOUR_PAT")
#' tok <- syn_resolve_token()
#' }
syn_resolve_token <- function(token = NULL) {
  if (.syn_is_scalar_chr(token) && nchar(token) > 20) return(token)
  
  tok <- Sys.getenv("SYNAPSE_PAT", unset = "")
  if (nzchar(tok)) return(tok)
  
  tok <- Sys.getenv("SYNAPSE_AUTH_TOKEN", unset = "")
  if (nzchar(tok)) return(tok)
  
  .syn_stop(
    "Missing Synapse token.\n",
    "Provide `token=` explicitly, or set Sys.setenv(SYNAPSE_PAT='...') (preferred),\n",
    "or Sys.setenv(SYNAPSE_AUTH_TOKEN='...')."
  )
}

# ==============================================================================
# HTTP layer (exported)
# ==============================================================================

#' Perform a Synapse REST request
#'
#' Low-level HTTP wrapper around Synapse endpoints using \pkg{httr2}.
#'
#' - Adds Authorization header using a PAT (see [syn_resolve_token()]).
#' - Retries transient failures (network, HTTP 429, HTTP 5xx) with exponential backoff.
#' - Parses JSON responses when content-type indicates JSON; otherwise returns text.
#'
#' @param method HTTP method (e.g., `"GET"`, `"POST"`, `"PUT"`).
#' @param path Path starting with `/` (e.g., `"/repo/v1/entity/syn123"`).
#' @param query Optional named list of query parameters.
#' @param body Optional request body.
#' @param body_json If `TRUE`, encode body as JSON and set content-type accordingly.
#' @param headers Optional named list of extra headers.
#' @param token Optional Synapse token (PAT). If `NULL`, env vars are used.
#' @param retry Logical; retry transient failures when `TRUE`.
#' @param verbose Logical; emit request/response progress messages.
#' @param dry_run Logical; do not execute the request, return a description instead.
#'
#' @return For JSON: a list (parsed JSON). For non-JSON: a character string body.
#' @export
#' @examples
#' \dontrun{
#' tok <- syn_resolve_token()
#' ent <- syn_request("GET", "/repo/v1/entity/syn123", token = tok)
#' str(ent)
#' }
syn_request <- function(method,
                        path,
                        query = NULL,
                        body = NULL,
                        body_json = TRUE,
                        headers = list(),
                        token = NULL,
                        retry = TRUE,
                        verbose = FALSE,
                        dry_run = FALSE) {
  method <- toupper(method)
  stopifnot(.syn_is_scalar_chr(method), .syn_is_scalar_chr(path))
  tok <- syn_resolve_token(token)
  
  url <- paste0(.syn_base_url(), path)
  
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] %s %s", method, path))
    if (!is.null(query)) .syn_verbose(TRUE, paste("  query:", jsonlite::toJSON(query, auto_unbox = TRUE)))
    if (!is.null(body))  .syn_verbose(TRUE, paste("  body :",  jsonlite::toJSON(body,  auto_unbox = TRUE)))
    return(invisible(list(dry_run = TRUE, method = method, path = path, url = url)))
  }
  
  req <- httr2::request(url) |>
    httr2::req_method(method) |>
    httr2::req_headers(
      Authorization = paste("Bearer", tok),
      Accept = "application/json",
      `User-Agent` = "synapse-rest-r/0.1"
    ) |>
    httr2::req_options(timeout = 120, connecttimeout = 30)
  
  if (length(headers)) req <- httr2::req_headers(req, !!!headers)
  if (!is.null(query)) req <- httr2::req_url_query(req, !!!query)
  
  if (!is.null(body)) {
    if (isTRUE(body_json)) {
      req <- httr2::req_headers(req, `Content-Type` = "application/json")
      req <- httr2::req_body_json(req, body, auto_unbox = TRUE, null = "null")
    } else {
      req <- httr2::req_body_raw(req, body)
    }
  }
  
  max_tries <- if (isTRUE(retry)) 6L else 1L
  
  for (attempt in seq_len(max_tries)) {
    resp <- tryCatch(httr2::req_perform(req), error = function(e) e)
    
    # Network-level error (no HTTP response)
    if (inherits(resp, "error") && !inherits(resp, "httr2_http")) {
      if (attempt >= max_tries) {
        .syn_stop("HTTP request failed (network error) for ", method, " ", path, ": ", conditionMessage(resp))
      }
      wait <- min(30, (2 ^ (attempt - 1)) + stats::runif(1, 0, 0.5))
      .syn_verbose(verbose, sprintf("[syn_request] %s %s -> network error; retrying in %.2fs", method, path, wait))
      Sys.sleep(wait)
      next
    }
    
    # HTTP error raised by httr2
    if (inherits(resp, "httr2_http")) {
      r <- if (!is.null(resp$response)) resp$response else attr(resp, "response", exact = TRUE)
      status <- if (!is.null(r)) httr2::resp_status(r) else NA_integer_
      
      if (attempt < max_tries && !is.na(status) && (status == 429L || status >= 500L)) {
        ra <- tryCatch(httr2::resp_header(r, "Retry-After"), error = function(e) NA_character_)
        wait <- suppressWarnings(as.numeric(ra))
        if (!is.finite(wait)) wait <- min(30, (2 ^ (attempt - 1)) + stats::runif(1, 0, 0.5))
        .syn_verbose(verbose, sprintf("[syn_request] %s %s -> %s; retrying in %.2fs", method, path, status, wait))
        Sys.sleep(wait)
        next
      }
      
      # Parse a useful message when possible
      reason <- NULL
      if (!is.null(r)) {
        txt <- tryCatch(httr2::resp_body_string(r), error = function(e) "")
        js  <- tryCatch(jsonlite::fromJSON(txt), error = function(e) NULL)
        if (is.list(js)) reason <- js$reason %||% js$message %||% NULL
        if (is.null(reason) && nzchar(txt)) reason <- substr(txt, 1, 400)
      }
      if (is.null(reason)) reason <- conditionMessage(resp)
      
      .syn_stop("Synapse HTTP error ", status, " for ", method, " ", path, ": ", reason)
    }
    
    # Success
    status <- httr2::resp_status(resp)
    .syn_verbose(verbose, sprintf("[syn_request] %s %s -> %s", method, path, status))
    if (status == 204L) return(invisible(list()))
    
    ct <- tryCatch(httr2::resp_header(resp, "Content-Type"), error = function(e) "")
    if (grepl("application/json", ct, fixed = TRUE)) {
      return(httr2::resp_body_json(resp, simplifyVector = TRUE))
    }
    return(httr2::resp_body_string(resp))
  }
  
  .syn_stop("Unexpected: syn_request retry loop exhausted for ", method, " ", path)
}

# ==============================================================================
# Entity operations (exported)
# ==============================================================================

#' Get a Synapse entity
#'
#' @param entity_id Synapse ID (e.g., `"syn123"`).
#' @param token Optional token (PAT).
#' @param verbose Logical; emit request progress messages.
#' @param dry_run Logical; do not execute.
#' @return Parsed JSON list describing the entity.
#' @export
#' @examples
#' \dontrun{
#' e <- syn_get_entity("syn123")
#' e$concreteType
#' }
syn_get_entity <- function(entity_id, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(entity_id))
  syn_request("GET", paste0("/repo/v1/entity/", entity_id),
              token = token, verbose = verbose, dry_run = dry_run)
}

#' Create a Folder under a parent container
#'
#' @param parent_id Parent Synapse ID (Project or Folder).
#' @param name Folder name (single path segment).
#' @param description Optional description string.
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return Parsed JSON list for created entity; contains `id`.
#' @export
#' @examples
#' \dontrun{
#' f <- syn_create_folder("synProjectId", "my_folder", description = "Created via REST")
#' f$id
#' }
syn_create_folder <- function(parent_id, name, description = NULL, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(parent_id), .syn_is_scalar_chr(name))
  .syn_validate_segment(name)
  
  body <- list(
    concreteType = "org.sagebionetworks.repo.model.Folder",
    parentId = parent_id,
    name = name
  )
  if (!is.null(description)) body$description <- description
  
  syn_request("POST", "/repo/v1/entity",
              body = body, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

#' Create a FileEntity under a parent container
#'
#' Typically used after creating a file handle via multipart upload.
#'
#' @param parent_id Parent Synapse ID (Project or Folder).
#' @param name FileEntity name.
#' @param dataFileHandleId File handle ID returned by multipart upload completion.
#' @param description Optional description string.
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return Parsed JSON list for created entity; contains `id`.
#' @export
#' @examples
#' \dontrun{
#' fe <- syn_create_file_entity("synFolderId", "data.csv", dataFileHandleId = "123456")
#' fe$id
#' }
syn_create_file_entity <- function(parent_id, name, dataFileHandleId, description = NULL,
                                   token = NULL, verbose = FALSE, dry_run = FALSE) {
  body <- list(
    concreteType = "org.sagebionetworks.repo.model.FileEntity",
    parentId = parent_id,
    name = name,
    dataFileHandleId = dataFileHandleId
  )
  if (!is.null(description)) body$description <- description
  
  syn_request("POST", "/repo/v1/entity",
              body = body, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

#' Update a FileEntity to point to a new file handle
#'
#' @param entity_id FileEntity synId.
#' @param new_dataFileHandleId New file handle ID.
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return Parsed JSON list for updated entity.
#' @export
#' @examples
#' \dontrun{
#' syn_update_file_entity("synFileEntityId", new_dataFileHandleId = "999999")
#' }
syn_update_file_entity <- function(entity_id, new_dataFileHandleId, token = NULL, verbose = FALSE, dry_run = FALSE) {
  ent <- syn_get_entity(entity_id, token = token, verbose = verbose, dry_run = dry_run)
  ctype <- ent$concreteType %||% ""
  if (!grepl("FileEntity$", ctype)) {
    .syn_stop("Entity is not a FileEntity: ", entity_id, " type=", ctype)
  }
  ent$dataFileHandleId <- new_dataFileHandleId
  
  syn_request("PUT", paste0("/repo/v1/entity/", entity_id),
              body = ent, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

# ==============================================================================
# Child lookup + folder path creation (exported)
# ==============================================================================

#' Get a child entity by name under a parent container
#'
#' Wraps `POST /repo/v1/entity/child`.
#'
#' @param parent_id Parent synId.
#' @param name Child name.
#' @param include_types Optional vector of type filters (e.g., `"folder"`, `"file"`).
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return Parsed JSON list including `id` and (depending on response) type metadata.
#' @export
#' @examples
#' \dontrun{
#' hit <- syn_get_child_by_name("synParent", "my_folder", include_types = "folder")
#' hit$id
#' }
syn_get_child_by_name <- function(parent_id,
                                  name,
                                  include_types = NULL,
                                  token = NULL,
                                  verbose = FALSE,
                                  dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(parent_id), .syn_is_scalar_chr(name))
  
  body <- list(parentId = parent_id, entityName = name)
  
  if (!is.null(include_types)) {
    stopifnot(is.character(include_types), length(include_types) >= 1L)
    include_types <- tolower(include_types)
    include_types <- include_types[nzchar(include_types)]
    body$includeTypes <- as.list(include_types)
  }
  
  syn_request("POST", "/repo/v1/entity/child",
              body = body, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

#' Try to get a child entity by name (returns NULL on 404)
#'
#' @inheritParams syn_get_child_by_name
#' @return Parsed JSON list, or `NULL` if not found.
#' @export
#' @examples
#' \dontrun{
#' hit <- syn_try_get_child_by_name("synParent", "maybe_exists", include_types = c("folder","file"))
#' if (is.null(hit)) message("Not found")
#' }
syn_try_get_child_by_name <- function(parent_id,
                                      name,
                                      include_types = NULL,
                                      token = NULL,
                                      verbose = FALSE,
                                      dry_run = FALSE) {
  tryCatch(
    syn_get_child_by_name(parent_id, name, include_types = include_types,
                          token = token, verbose = verbose, dry_run = dry_run),
    error = function(e) {
      msg <- conditionMessage(e)
      if (grepl("404", msg, fixed = TRUE) || grepl("Not Found", msg, fixed = TRUE)) return(NULL)
      stop(e)
    }
  )
}

#' Lookup a child synId by parent + name
#'
#' Convenience wrapper returning just the child `id` or `NULL` if not found.
#'
#' @param parent_id Parent synId.
#' @param entity_name Child name.
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return Child synId character scalar, or `NULL`.
#' @export
#' @examples
#' \dontrun{
#' cid <- syn_lookup_child_id("synParent", "child_name")
#' }
syn_lookup_child_id <- function(parent_id, entity_name, token = NULL, verbose = FALSE, dry_run = FALSE) {
  hit <- syn_try_get_child_by_name(parent_id, entity_name,
                                   token = token, verbose = verbose, dry_run = dry_run)
  if (is.null(hit)) return(NULL)
  hit$id %||% NULL
}

#' Ensure a folder path exists under a base container
#'
#' Ensures each segment exists as a Folder under `parent_id`. Missing folders are
#' created. Existing segments are validated as Folders (not files) by calling
#' [syn_get_entity()] on each resolved synId.
#'
#' @param parent_id Base container synId (Project or Folder).
#' @param path Remote folder path like `"a/b/c"` (slashes only; leading/trailing ignored).
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return synId of the deepest folder in the path.
#' @export
#' @examples
#' \dontrun{
#' folder_id <- syn_ensure_folder_path("synProjectId", "analysis/run_01")
#' folder_id
#' }
syn_ensure_folder_path <- function(parent_id, path,
                                   token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(parent_id), .syn_is_scalar_chr(path))
  
  parent <- syn_get_entity(parent_id, token = token, verbose = verbose, dry_run = dry_run)
  ptype <- parent$concreteType %||% parent$entityType %||% ""
  if (!grepl("Project$|Folder$", ptype)) {
    .syn_stop("parent_id must be a Project or Folder. Got concreteType: ", ptype)
  }
  
  path <- .syn_norm_path(path)
  if (!nzchar(path)) return(parent_id)
  
  parts <- strsplit(path, "/", fixed = TRUE)[[1]]
  parts <- parts[nzchar(parts)]
  for (seg in parts) .syn_validate_segment(seg)
  
  cur <- parent_id
  for (seg in parts) {
    child_id <- syn_lookup_child_id(cur, seg, token = token, verbose = verbose, dry_run = dry_run)
    
    if (is.null(child_id)) {
      created <- syn_create_folder(cur, seg, token = token, verbose = verbose, dry_run = dry_run)
      cur <- created$id %||% created$properties$id %||% created$entity$id
      if (!.syn_is_scalar_chr(cur)) .syn_stop("Folder create returned unexpected payload.")
    } else {
      ent <- syn_get_entity(child_id, token = token, verbose = verbose, dry_run = dry_run)
      ctype <- ent$concreteType %||% ""
      if (!grepl("Folder$", ctype)) {
        .syn_stop("Path segment exists but is not a Folder: '", seg, "' (", child_id, ") type=", ctype)
      }
      cur <- child_id
    }
  }
  
  cur
}

# ==============================================================================
# Storage locations (internal)
# ==============================================================================

#' Pick a storageLocationId from uploadDestinationLocations
#' @param x Parsed response object.
#' @return storageLocationId (scalar) or NULL.
#' @keywords internal
.syn_pick_storage_location <- function(x) {
  locs <- x$list %||% x$locations %||% x$uploadDestinationLocations %||% x$results %||% x
  if (is.null(locs)) return(NULL)
  
  if (is.list(locs) && !is.null(locs$storageLocationId)) locs <- list(locs)
  if (!is.list(locs) || !length(locs)) return(NULL)
  
  pick <- NULL
  for (li in locs) {
    if (is.list(li) && isTRUE(li$isDefault)) { pick <- li; break }
  }
  if (is.null(pick)) pick <- locs[[1]]
  
  id <- pick$storageLocationId %||% pick$id %||% pick$storage_location_id %||% NULL
  if (is.null(id) && is.list(pick$storageLocation)) {
    id <- pick$storageLocation$id %||% pick$storageLocation$storageLocationId %||% NULL
  }
  id
}

# ==============================================================================
# Multipart upload (internal)
# ==============================================================================

#' Get presigned URLs for multipart upload parts
#' @param upload_id Multipart upload id.
#' @param part_numbers Integer vector of part numbers.
#' @inheritParams syn_request
#' @return Parsed JSON list.
#' @keywords internal
.syn_multipart_presigned_urls <- function(upload_id, part_numbers,
                                          token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(upload_id))
  body <- list(
    uploadId    = upload_id,
    partNumbers = as.list(as.integer(part_numbers)) # force JSON array even for length 1
  )
  syn_request("POST",
              paste0("/file/v1/file/multipart/", upload_id, "/presigned/url/batch"),
              body = body, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

#' Build maps from a presigned url/batch response
#' @param presigned_resp Parsed response from `.syn_multipart_presigned_urls()`.
#' @return List with `url_by_part` and `hdr_by_part`.
#' @keywords internal
.syn_build_presigned_maps <- function(presigned_resp) {
  items <- presigned_resp$partPresignedUrls %||% presigned_resp$part_presigned_urls %||% presigned_resp
  items <- .syn_as_list_of_rows(items)
  if (!length(items)) .syn_stop("presigned/url/batch returned no partPresignedUrls")
  
  pns  <- vapply(items, function(it) as.character(it$partNumber %||% it$part_number %||% NA_character_), "")
  urls <- vapply(items, function(it) as.character(
    it$uploadPresignedUrl %||% it$uploadPresignedURL %||% it$url %||% it$presignedUrl %||% NA_character_
  ), "")
  hdrs <- lapply(items, function(it) it$signedHeaders %||% it$signed_headers %||% list())
  
  keep <- !is.na(pns) & nzchar(pns) & !is.na(urls) & nzchar(urls)
  if (!any(keep)) {
    .syn_stop(
      "Could not extract (partNumber, uploadPresignedUrl) pairs from presigned response.\n",
      "First item keys: ", paste(names(items[[1]]), collapse = ", ")
    )
  }
  
  list(
    url_by_part = setNames(urls[keep], pns[keep]),
    hdr_by_part = setNames(hdrs[keep], pns[keep])
  )
}

#' PUT a multipart part to a presigned URL
#' @param url Presigned URL.
#' @param raw_bytes Raw vector.
#' @param signed_headers Named list of headers required by Synapse (may be empty).
#' @param verbose Logical.
#' @return httr2 response.
#' @keywords internal
.syn_put_presigned_part <- function(url, raw_bytes, signed_headers = list(), verbose = FALSE) {
  req <- httr2::request(url) |>
    httr2::req_method("PUT") |>
    httr2::req_body_raw(raw_bytes) |>
    httr2::req_options(timeout = 600, http_version = 1.1)
  
  if (length(signed_headers)) req <- httr2::req_headers(req, !!!signed_headers)
  
  resp <- httr2::req_perform(req)
  st <- httr2::resp_status(resp)
  if (st < 200L || st >= 300L) .syn_stop("Presigned PUT failed (HTTP ", st, ")")
  resp
}

# ==============================================================================
# Upload (exported)
# ==============================================================================

#' Upload a local file to Synapse (multipart) and create/update a FileEntity
#'
#' Implements Synapse multipart upload:
#' 1) Determine upload destination storage location for `parent_id`
#' 2) Initiate multipart upload
#' 3) Upload parts to presigned S3 URLs and `add` each part
#' 4) Complete multipart upload to obtain a file handle ID
#' 5) Create a new FileEntity under `parent_id` or update an existing file with the same name
#' 6) Optionally set annotations (annotations2)
#'
#' @param local_path Path to an existing local file.
#' @param parent_id Parent container synId (Project or Folder).
#' @param name Optional name for the FileEntity (defaults to `basename(local_path)`).
#' @param contentType Optional MIME type (defaults to [tools::file_ext()] guess).
#' @param md5 Logical; compute MD5 for local file and parts (requires \pkg{digest}).
#' @param overwrite Logical; if a file with the same name exists, update it.
#' @param description Optional description for the FileEntity.
#' @param annotations Optional named list of annotations (applied via annotations2).
#' @param token Optional Synapse token (PAT).
#' @param verbose Logical; emit progress.
#' @param dry_run Logical; do not execute network requests.
#'
#' @return An object of class `"syn_upload_result"` with fields:
#' - `file_entity_id`
#' - `file_handle_id`
#' - `upload_id`
#' - `md5_hex` (file MD5 if computed)
#' - `size_bytes`
#' - `etags` (list of part ETags when available)
#'
#' @export
#' @examples
#' \dontrun{
#' tmp <- tempfile(fileext = ".csv")
#' write.csv(data.frame(x=1:3, y=letters[1:3]), tmp, row.names = FALSE)
#'
#' res <- syn_upload_file(
#'   local_path = tmp,
#'   parent_id  = "synProjectOrFolder",
#'   overwrite  = TRUE,
#'   annotations = list(stage = "raw", kind = "csv"),
#'   verbose = TRUE
#' )
#' res$file_entity_id
#' }
syn_upload_file <- function(local_path,
                            parent_id,
                            name = NULL,
                            contentType = NULL,
                            md5 = TRUE,
                            overwrite = FALSE,
                            description = NULL,
                            annotations = NULL,
                            token = NULL,
                            verbose = FALSE,
                            dry_run = FALSE) {
  stopifnot(.syn_is_id(parent_id), .syn_is_scalar_chr(local_path))
  if (!file.exists(local_path)) .syn_stop("local_path does not exist: ", local_path)
  if (dir.exists(local_path))   .syn_stop("local_path is a directory: ", local_path)
  
  parent <- syn_get_entity(parent_id, token = token, verbose = verbose, dry_run = dry_run)
  ct <- parent$concreteType %||% ""
  if (!isTRUE(grepl("(Project|Folder)$", ct))) {
    .syn_stop("parent_id must be a Project or Folder. Got: ", ct)
  }
  
  name <- name %||% basename(local_path)
  stopifnot(.syn_is_scalar_chr(name))
  
  contentType <- contentType %||% .syn_guess_content_type(local_path)
  
  size_bytes <- as.numeric(file.info(local_path)$size)
  if (!is.finite(size_bytes) || size_bytes < 0) .syn_stop("Could not determine file size: ", local_path)
  size_bytes <- as.integer(size_bytes)
  
  md5_hex <- NULL
  if (isTRUE(md5)) md5_hex <- .syn_md5_hex_file(local_path)
  
  locs <- syn_request("GET", paste0("/file/v1/entity/", parent_id, "/uploadDestinationLocations"),
                      token = token, verbose = verbose, dry_run = dry_run)
  storage_location_id <- .syn_pick_storage_location(locs)
  if (is.null(storage_location_id)) {
    keys <- paste(names(locs), collapse = ", ")
    .syn_stop(
      "Could not determine storageLocationId from uploadDestinationLocations response.\n",
      "Top-level keys: ", keys, "\n",
      "Raw response (first 1000 chars): ",
      substr(jsonlite::toJSON(locs, auto_unbox = TRUE, null = "null"), 1, 1000)
    )
  }
  
  min_part <- 5L * 1024L * 1024L
  part_size_bytes <- as.integer(max(min_part, min(64L * 1024L * 1024L, ceiling(size_bytes / 5000L))))
  
  upload_body <- list(
    concreteType      = "org.sagebionetworks.repo.model.file.MultipartUploadRequest",
    fileName          = name,
    contentType       = contentType,
    fileSizeBytes     = as.numeric(size_bytes),
    storageLocationId = as.numeric(storage_location_id),
    partSizeBytes     = as.numeric(part_size_bytes)
  )
  if (!is.null(md5_hex)) upload_body$contentMD5Hex <- md5_hex
  
  init <- syn_request(
    "POST", "/file/v1/file/multipart",
    query = list(forceRestart = "true"),
    body  = upload_body, body_json = TRUE,
    token = token, verbose = verbose, dry_run = dry_run
  )
  
  upload_id <- as.character(init$uploadId %||% init$id %||% NA_character_)
  if (!nzchar(upload_id) || is.na(upload_id)) .syn_stop("Multipart initiation did not return uploadId.")
  
  part_size <- as.integer(init$partSizeBytes %||% part_size_bytes)
  n_parts <- max(1L, as.integer(ceiling(as.numeric(size_bytes) / as.numeric(part_size))))
  
  .syn_verbose(verbose, sprintf("[multipart] uploadId=%s size=%d partSize=%d parts=%d",
                                upload_id, size_bytes, part_size, n_parts))
  
  con <- file(local_path, open = "rb")
  on.exit(try(close(con), silent = TRUE), add = TRUE)
  
  part_numbers <- seq_len(n_parts)
  pres <- .syn_multipart_presigned_urls(upload_id, part_numbers, token = token, verbose = verbose, dry_run = dry_run)
  maps <- .syn_build_presigned_maps(pres)
  url_by_part <- maps$url_by_part
  hdr_by_part <- maps$hdr_by_part
  
  etags <- vector("list", n_parts)
  
  if (isTRUE(verbose)) {
    message("[multipart] presigned map parts: ", paste(names(url_by_part), collapse = ", "))
  }
  
  for (pn in part_numbers) {
    raw_bytes <- readBin(con, what = "raw", n = part_size)
    if (!length(raw_bytes)) .syn_stop("Unexpected EOF while reading part ", pn, "/", n_parts)
    
    part_md5_hex <- if (isTRUE(md5)) .syn_md5_hex_raw(raw_bytes) else NULL
    
    key <- as.character(pn)
    part_url <- unname(url_by_part[key])
    if (is.na(part_url) || !nzchar(part_url)) .syn_stop("Missing presigned URL for part ", pn)
    
    signed_headers <- hdr_by_part[[key]] %||% list()
    
    if (isTRUE(dry_run)) {
      .syn_verbose(TRUE, sprintf("[dry_run] would PUT presigned URL for part %d (%d bytes)", pn, length(raw_bytes)))
    } else {
      resp <- .syn_put_presigned_part(part_url, raw_bytes, signed_headers = signed_headers, verbose = verbose)
      et <- tryCatch(httr2::resp_header(resp, "ETag"), error = function(e) NA_character_)
      etags[[pn]] <- et
    }
    
    add_query <- if (!is.null(part_md5_hex)) list(partMD5Hex = part_md5_hex) else list()
    syn_request("PUT", paste0("/file/v1/file/multipart/", upload_id, "/add/", pn),
                query = add_query,
                token = token, verbose = verbose, dry_run = dry_run)
    
    .syn_verbose(verbose, sprintf("[multipart] uploaded+added part %d/%d", pn, n_parts))
  }
  
  done <- syn_request("PUT", paste0("/file/v1/file/multipart/", upload_id, "/complete"),
                      token = token, verbose = verbose, dry_run = dry_run)
  
  file_handle_id <- as.character(done$resultFileHandleId %||% done$fileHandleId %||% done$id %||% NA_character_)
  if (!nzchar(file_handle_id) || is.na(file_handle_id)) {
    .syn_stop("Multipart complete did not return a fileHandleId (resultFileHandleId/fileHandleId).")
  }
  
  existing <- syn_try_get_child_by_name(parent_id, name,
                                        token = token, verbose = verbose, dry_run = dry_run)
  existing_id <- if (is.null(existing)) NULL else (existing$id %||% NULL)
  
  file_entity_id <- NULL
  
  if (!is.null(existing_id) && nzchar(existing_id)) {
    if (!isTRUE(overwrite)) {
      .syn_stop("A file named '", name, "' already exists under ", parent_id,
                " (entity ", existing_id, "). Set overwrite=TRUE to update it.")
    }
    
    ent <- syn_get_entity(existing_id, token = token, verbose = verbose, dry_run = dry_run)
    ent$concreteType <- "org.sagebionetworks.repo.model.FileEntity"
    ent$dataFileHandleId <- file_handle_id
    if (!is.null(description)) ent$description <- description
    
    syn_request("PUT", paste0("/repo/v1/entity/", existing_id),
                body = ent, body_json = TRUE,
                token = token, verbose = verbose, dry_run = dry_run)
    
    file_entity_id <- existing_id
  } else {
    body <- list(
      concreteType = "org.sagebionetworks.repo.model.FileEntity",
      name = name,
      parentId = parent_id,
      dataFileHandleId = file_handle_id
    )
    if (!is.null(description)) body$description <- description
    
    created <- syn_request("POST", "/repo/v1/entity",
                           body = body, body_json = TRUE,
                           token = token, verbose = verbose, dry_run = dry_run)
    
    file_entity_id <- created$id %||% created$entity$id %||% created$properties$id %||% NULL
    if (is.null(file_entity_id)) .syn_stop("FileEntity creation succeeded but no id returned.")
  }
  
  if (!is.null(annotations)) {
    if (!is.list(annotations) || is.null(names(annotations)) || any(!nzchar(names(annotations)))) {
      .syn_stop("`annotations` must be a *named* list (e.g., list(stage='raw', kind='csv')).")
    }
    syn_set_annotations(file_entity_id, annotations, token = token, verbose = verbose, dry_run = dry_run)
  }
  
  structure(list(
    file_entity_id = file_entity_id,
    file_handle_id = file_handle_id,
    upload_id      = upload_id,
    md5_hex        = md5_hex,
    size_bytes     = size_bytes,
    etags          = etags
  ), class = "syn_upload_result")
}

# ==============================================================================
# Download (internal + exported)
# ==============================================================================

#' Get a presigned download URL for a file handle (robust redirect handling)
#'
#' Synapse file handle url endpoint can respond with a redirect (307 + Location)
#' or with JSON containing a URL, depending on deployment and headers.
#'
#' This function deliberately avoids following redirects automatically, to capture
#' the `Location` header safely.
#'
#' @param file_handle_id File handle ID (character).
#' @inheritParams syn_request
#' @return Presigned URL (character scalar).
#' @keywords internal
.syn_filehandle_download_url <- function(file_handle_id,
                                         token = NULL,
                                         verbose = FALSE,
                                         dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(file_handle_id))
  tok <- syn_resolve_token(token)
  
  path <- paste0("/file/v1/fileHandle/", file_handle_id, "/url")
  url  <- paste0(.syn_base_url(), path)
  
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, paste0("[dry_run] would GET (no redirect) ", path))
    return(invisible(NULL))
  }
  
  req <- httr2::request(url) |>
    httr2::req_method("GET") |>
    httr2::req_headers(
      Authorization = paste("Bearer", tok),
      Accept = "application/json"
    ) |>
    httr2::req_options(
      followlocation = 0,
      maxredirs = 0,
      timeout = 120,
      connecttimeout = 30
    )
  
  resp <- httr2::req_perform(req)
  st   <- httr2::resp_status(resp)
  
  if (st %in% c(301L, 302L, 303L, 307L, 308L)) {
    loc <- httr2::resp_header(resp, "Location") %||% httr2::resp_header(resp, "location")
    if (!is.character(loc) || length(loc) != 1L || !nzchar(loc)) {
      .syn_stop("Synapse returned redirect (HTTP ", st, ") but no Location header was present.")
    }
    .syn_verbose(verbose, paste0("[fileHandle/url] redirect -> Location (", nchar(loc), " chars)"))
    return(loc)
  }
  
  ct <- tryCatch(httr2::resp_header(resp, "Content-Type"), error = function(e) "")
  if (grepl("application/json", ct, fixed = TRUE)) {
    js <- httr2::resp_body_json(resp, simplifyVector = FALSE)
    loc <- js$preSignedURL %||% js$preSignedUrl %||% js$presignedUrl %||% js$url %||% NULL
    if (is.null(loc) || !nzchar(loc)) {
      .syn_stop("JSON response did not contain a usable URL (preSignedURL/presignedUrl/url).")
    }
    return(loc)
  }
  
  body <- tryCatch(httr2::resp_body_string(resp), error = function(e) "")
  .syn_stop(
    "Unexpected response from fileHandle URL endpoint.\n",
    "HTTP ", st, " Content-Type: ", ct, "\n",
    "First 300 chars:\n", substr(body, 1, 300)
  )
}

#' Download a Synapse FileEntity to disk
#'
#' Fetches the FileEntity to obtain `dataFileHandleId`, resolves a presigned URL via
#' the file handle URL endpoint, and streams the file content to `dest_path`.
#'
#' @param entity_id FileEntity synId.
#' @param dest_path Destination path (will create parent directories).
#' @param overwrite Logical; overwrite existing file if `TRUE`.
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return `dest_path` (invisibly in dry-run, otherwise returned as a character scalar).
#' @export
#' @examples
#' \dontrun{
#' dest <- tempfile(fileext = ".csv")
#' syn_download_file("synFileEntityId", dest, overwrite = TRUE, verbose = TRUE)
#' read.csv(dest)
#' }
syn_download_file <- function(entity_id,
                              dest_path,
                              overwrite = FALSE,
                              token = NULL,
                              verbose = FALSE,
                              dry_run = FALSE) {
  stopifnot(.syn_is_id(entity_id))
  stopifnot(is.character(dest_path), length(dest_path) == 1L)
  
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] would download %s -> %s", entity_id, dest_path))
    return(invisible(dest_path))
  }
  
  ent <- syn_get_entity(entity_id, token = token, verbose = verbose)
  if ((ent$concreteType %||% "") != "org.sagebionetworks.repo.model.FileEntity") {
    .syn_stop("entity_id is not a FileEntity: ", entity_id)
  }
  
  fh <- ent$dataFileHandleId %||% NULL
  if (is.null(fh) || !nzchar(fh)) .syn_stop("FileEntity has no dataFileHandleId: ", entity_id)
  
  url <- .syn_filehandle_download_url(as.character(fh), token = token, verbose = verbose)
  
  dir.create(dirname(dest_path), recursive = TRUE, showWarnings = FALSE)
  if (file.exists(dest_path) && !isTRUE(overwrite)) {
    .syn_stop("dest_path exists and overwrite=FALSE: ", dest_path)
  }
  
  .syn_verbose(verbose, paste0("[syn_download_file] downloading -> ", dest_path))
  
  req <- httr2::request(url) |>
    httr2::req_method("GET") |>
    httr2::req_options(timeout = 300)
  
  httr2::req_perform(req, path = dest_path)
  dest_path
}

# ==============================================================================
# annotations2 (exported)
# ==============================================================================

#' Determine Synapse annotation type (annotations2)
#' @param x Atomic vector.
#' @return Type string: BOOLEAN/LONG/DOUBLE/STRING.
#' @keywords internal
.syn_anno_type <- function(x) {
  if (is.logical(x)) return("BOOLEAN")
  if (is.integer(x)) return("LONG")
  if (is.numeric(x)) return("DOUBLE")
  "STRING"
}

#' Pack a named list into annotations2 schema
#' @param annotations Named list.
#' @return List suitable for annotations2 endpoint.
#' @keywords internal
.syn_anno_pack <- function(annotations) {
  stopifnot(is.list(annotations), length(names(annotations)) == length(annotations))
  out <- list()
  for (k in names(annotations)) {
    v <- annotations[[k]]
    if (is.null(v) || length(v) == 0L) next
    
    if (is.list(v) && !is.data.frame(v)) {
      v <- unlist(v, recursive = TRUE, use.names = FALSE)
    }
    if (!is.atomic(v)) v <- as.character(v)
    
    tp <- .syn_anno_type(v)
    if (tp == "STRING") v <- as.character(v)
    
    out[[k]] <- list(type = tp, value = unname(as.list(v)))
  }
  out
}

#' Get annotations2 for an entity
#'
#' @param entity_id synId.
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return Parsed JSON list containing `etag` and `annotations`.
#' @export
#' @examples
#' \dontrun{
#' ann <- syn_get_annotations("synEntityId")
#' names(ann$annotations)
#' }
syn_get_annotations <- function(entity_id, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_id(entity_id))
  syn_request("GET", paste0("/repo/v1/entity/", entity_id, "/annotations2"),
              token = token, verbose = verbose, dry_run = dry_run)
}

#' Set (merge) annotations2 on an entity
#'
#' Fetches current annotations2, merges keys provided in `annotations` (overwriting those keys),
#' and PUTs the updated annotations back using the current `etag`.
#'
#' @param entity_id synId.
#' @param annotations Named list of annotations. Values may be scalar or vectors.
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return Parsed JSON list from the PUT request.
#' @export
#' @examples
#' \dontrun{
#' syn_set_annotations("synEntityId", list(stage = "raw", tags = c("a","b")))
#' }
syn_set_annotations <- function(entity_id, annotations, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_id(entity_id))
  if (!is.list(annotations) || is.null(names(annotations)) || any(!nzchar(names(annotations)))) {
    .syn_stop("annotations must be a *named* list")
  }
  
  cur <- syn_get_annotations(entity_id, token = token, verbose = verbose, dry_run = dry_run)
  
  cur_id   <- cur$id   %||% entity_id
  cur_etag <- cur$etag %||% NULL
  cur_ann  <- cur$annotations %||% list()
  
  new_ann <- .syn_anno_pack(annotations)
  
  merged <- cur_ann
  for (k in names(new_ann)) merged[[k]] <- new_ann[[k]]
  
  body <- list(id = cur_id, etag = cur_etag, annotations = merged)
  
  syn_request("PUT", paste0("/repo/v1/entity/", entity_id, "/annotations2"),
              body = body, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

# ==============================================================================
# "English path" wrappers (exported)
# ==============================================================================

#' Upload a file to a remote folder path (relative to a base container)
#'
#' Convenience wrapper:
#' - Ensures `remote_folder_path` exists under `base_id` (creating folders as needed)
#' - Uploads `local_path` to the resulting folder via [syn_upload_file()]
#'
#' @param local_path Local file path.
#' @param base_id Base container synId (Project or Folder).
#' @param remote_folder_path Remote folder path relative to `base_id`, e.g. `"a/b/c"`.
#' @param name Optional FileEntity name override.
#' @param contentType Optional MIME type override.
#' @param overwrite Logical; overwrite existing FileEntity with same name.
#' @param description Optional description.
#' @param annotations Optional named list for annotations2.
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return `"syn_upload_result"` (see [syn_upload_file()]).
#' @export
#' @examples
#' \dontrun{
#' res <- syn_upload_file_path(
#'   local_path = "C:/data/example.csv",
#'   base_id = "synProjectId",
#'   remote_folder_path = "zz_smoketests/synapse_rw/test2",
#'   overwrite = TRUE,
#'   annotations = list(stage = "smoke", kind = "csv"),
#'   verbose = TRUE
#' )
#' res$file_entity_id
#' }
syn_upload_file_path <- function(local_path,
                                 base_id,
                                 remote_folder_path,
                                 name = NULL,
                                 contentType = NULL,
                                 overwrite = FALSE,
                                 description = NULL,
                                 annotations = NULL,
                                 token = NULL,
                                 verbose = FALSE,
                                 dry_run = FALSE) {
  stopifnot(.syn_is_id(base_id), .syn_is_scalar_chr(remote_folder_path))
  
  folder_id <- syn_ensure_folder_path(
    parent_id = base_id,
    path = remote_folder_path,
    token = token, verbose = verbose, dry_run = dry_run
  )
  
  syn_upload_file(
    local_path   = local_path,
    parent_id    = folder_id,
    name         = name %||% basename(local_path),
    contentType  = contentType,
    overwrite    = overwrite,
    description  = description,
    annotations  = annotations,
    token        = token,
    verbose      = verbose,
    dry_run      = dry_run
  )
}

#' Resolve an entity synId from a remote path under a base container
#'
#' - If `remote_path` ends with `/`, the last segment must resolve to a Folder.
#' - Otherwise the last segment may resolve to either a Folder or FileEntity.
#'
#' Folder-ness checks are enforced by calling [syn_get_entity()] on intermediate segments.
#'
#' @param base_id Base container synId (Project or Folder).
#' @param remote_path Remote path like `"a/b/file.csv"` or `"a/b/folder/"`.
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return synId of the resolved entity.
#' @export
#' @examples
#' \dontrun{
#' eid <- syn_resolve_entity_by_path("synProjectId", "a/b/c/myfile.csv")
#' eid
#' }
syn_resolve_entity_by_path <- function(base_id,
                                       remote_path,
                                       token = NULL,
                                       verbose = FALSE,
                                       dry_run = FALSE) {
  stopifnot(.syn_is_id(base_id), .syn_is_scalar_chr(remote_path))
  
  p0 <- gsub("\\\\", "/", remote_path)
  is_folder_hint <- grepl("/+$", p0)
  parts <- .syn_split_path(p0)
  
  cur <- base_id
  if (!length(parts)) return(cur)
  
  for (i in seq_along(parts)) {
    seg  <- parts[[i]]
    last <- (i == length(parts))
    
    include_types <- if (!last) {
      "folder"
    } else if (is_folder_hint) {
      "folder"
    } else {
      c("folder", "file")
    }
    
    hit <- syn_try_get_child_by_name(
      cur, seg,
      include_types = include_types,
      token = token, verbose = verbose, dry_run = dry_run
    )
    
    if (is.null(hit)) {
      .syn_stop("No entity named '", seg, "' under ", cur, " while resolving path: ", remote_path)
    }
    
    child_id <- hit$id %||% NULL
    if (!.syn_is_id(child_id)) {
      .syn_stop("Unexpected child lookup payload for segment '", seg, "' under ", cur,
                ". Returned keys: ", paste(names(hit), collapse = ", "))
    }
    
    must_be_folder <- (!last) || is_folder_hint
    if (isTRUE(must_be_folder)) {
      ent <- syn_get_entity(child_id, token = token, verbose = verbose, dry_run = dry_run)
      ctype <- ent$concreteType %||% ""
      if (!grepl("Folder$", ctype)) {
        .syn_stop("Path segment '", seg, "' exists but is not a Folder: ", child_id, " concreteType=", ctype)
      }
    }
    
    cur <- child_id
  }
  
  cur
}

#' Download a file using a remote path (relative to a base container)
#'
#' @param base_id Base container synId.
#' @param remote_file_path Remote file path like `"a/b/c/file.csv"`.
#' @param dest_path Local destination path.
#' @param overwrite Logical; overwrite existing local file.
#' @param token Optional token (PAT).
#' @param verbose Logical.
#' @param dry_run Logical.
#' @return `dest_path`.
#' @export
#' @examples
#' \dontrun{
#' syn_download_file_path(
#'   base_id = "synProjectId",
#'   remote_file_path = "zz_smoketests/synapse_rw/test2/New Text Document.txt",
#'   dest_path = "C:/tmp/downloads/test.txt",
#'   overwrite = TRUE,
#'   verbose = TRUE
#' )
#' }
syn_download_file_path <- function(base_id,
                                   remote_file_path,
                                   dest_path,
                                   overwrite = FALSE,
                                   token = NULL,
                                   verbose = FALSE,
                                   dry_run = FALSE) {
  eid <- syn_resolve_entity_by_path(base_id, remote_file_path,
                                    token = token, verbose = verbose, dry_run = dry_run)
  syn_download_file(eid, dest_path = dest_path, overwrite = overwrite,
                    token = token, verbose = verbose, dry_run = dry_run)
}
