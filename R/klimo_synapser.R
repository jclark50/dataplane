# synapse_rest_backend.R
# Pure-R Synapse REST backend (no synapser, no reticulate)
# Deps: httr2, jsonlite, digest (optional but strongly recommended), fs (optional)

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
})

# =========================
# Internal helpers
# =========================
# ---- base URL ----
# ---- small helpers ----
.syn_is_scalar_chr <- function(x) is.character(x) && length(x) == 1L && !is.na(x)
.syn_is_id <- function(x) .syn_is_scalar_chr(x) && grepl("^syn[0-9]+$", x)



.syn_base_url <- function() "https://repo-prod.prod.sagebase.org"

.syn_is_scalar_chr <- function(x) is.character(x) && length(x) == 1L && !is.na(x)

.syn_stop <- function(...) stop(paste0(...), call. = FALSE)

.syn_verbose <- function(verbose, ...) if (isTRUE(verbose)) message(...)

# Robust coalesce: only treats NULL/len0 as missing; treats NA/"" missing only if scalar
.syn_coalesce <- function(a, b) {
  if (is.null(a) || length(a) == 0L) return(b)
  if (length(a) == 1L) {
    if (is.na(a)) return(b)
    if (is.character(a) && identical(a, "")) return(b)
  }
  a
}

# Token precedence:
# explicit argument > SYNAPSE_PAT > SYNAPSE_AUTH_TOKEN
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

# Parse Synapse error response (best-effort)
.syn_parse_error_reason <- function(resp) {
  if (is.null(resp)) return(NULL)
  txt <- tryCatch(resp_body_string(resp), error = function(e) NULL)
  if (is.null(txt) || !nzchar(txt)) return(NULL)
  js <- tryCatch(jsonlite::fromJSON(txt, simplifyVector = TRUE), error = function(e) NULL)
  if (is.list(js)) {
    # common Synapse ErrorResponse: {concreteType, reason}
    if (!is.null(js$reason)) return(as.character(js$reason))
    if (!is.null(js$message)) return(as.character(js$message))
    if (!is.null(js$error)) return(as.character(js$error))
  }
  # fallback: short snippet
  if (nchar(txt) > 500) txt <- paste0(substr(txt, 1, 500), "…")
  txt
}

# Extract httr2 response from an httr2_http_* condition
.syn_extract_resp <- function(cond) {
  if (inherits(cond, "httr2_http")) {
    if (!is.null(cond$resp)) return(cond$resp)
    if (!is.null(cond$response)) return(cond$response)
    r <- attr(cond, "response", exact = TRUE)
    if (!is.null(r)) return(r)
  }
  NULL
}

# Sleep with jitter, honoring Retry-After when present
.syn_retry_wait <- function(resp, attempt) {
  # Retry-After may be seconds or HTTP-date; we handle seconds robustly
  ra <- tryCatch(resp_header(resp, "Retry-After"), error = function(e) NA_character_)
  ra_sec <- suppressWarnings(as.numeric(ra))
  if (is.finite(ra_sec) && ra_sec >= 0) return(min(60, ra_sec))
  
  # exponential backoff + jitter
  base <- min(30, 2 ^ (attempt - 1))
  jitter <- stats::runif(1, 0, 0.5)
  base + jitter
}

# Main request helper with retries/backoff for 429 + 5xx + network errors
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
  
  url <- paste0(.syn_base_url(), path)
  
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] %s %s", method, path))
    if (!is.null(query)) .syn_verbose(TRUE, paste0("  query: ", jsonlite::toJSON(query, auto_unbox = TRUE)))
    if (!is.null(body))  .syn_verbose(TRUE, paste0("  body:  ", jsonlite::toJSON(body, auto_unbox = TRUE)))
    return(invisible(list(dry_run = TRUE, method = method, path = path, url = url)))
  }
  
  tok <- syn_resolve_token(token)
  
  req <- httr2::request(url) |>
    httr2::req_method(method) |>
    httr2::req_headers(
      Authorization = paste("Bearer", tok),
      Accept = "application/json",
      `User-Agent` = "synapse-rest-r/0.1"
    ) |>
    # sane timeouts to reduce “hanging” and increase determinism
    httr2::req_options(timeout = 120, connecttimeout = 30)
  
  if (length(headers)) {
    req <- httr2::req_headers(req, !!!headers)
  }
  if (!is.null(query)) {
    req <- httr2::req_url_query(req, !!!query)
  }
  if (!is.null(body)) {
    if (isTRUE(body_json)) {
      req <- httr2::req_headers(req, `Content-Type` = "application/json")
      req <- httr2::req_body_json(req, body, auto_unbox = TRUE, null = "null")
    } else {
      # raw body; caller provides correct content-type if needed
      req <- httr2::req_body_raw(req, body)
    }
  }
  
  max_tries <- if (isTRUE(retry)) 7L else 1L
  
  for (attempt in seq_len(max_tries)) {
    .syn_verbose(verbose, sprintf("[syn_request] %s %s", method, path))
    
    out <- tryCatch(
      httr2::req_perform(req),
      error = function(e) e
    )
    
    # ---- Network-level error (curl / connection / DNS) ----
    if (inherits(out, "curl_error") || inherits(out, "httr2_failure")) {
      if (attempt >= max_tries) {
        .syn_stop("HTTP request failed (network error) for ", method, " ", path, ": ", conditionMessage(out))
      }
      wait <- min(60, (2 ^ (attempt - 1)) + stats::runif(1, 0, 0.5))
      .syn_verbose(verbose, sprintf("[syn_request] %s %s -> network error; retrying in %.2fs", method, path, wait))
      Sys.sleep(wait)
      next
    }
    
    # ---- HTTP error thrown as condition (httr2_http_4xx / 5xx) ----
    if (inherits(out, "httr2_http")) {
      resp <- .syn_extract_resp(out)
      code <- if (!is.null(resp)) resp_status(resp) else NA_integer_
      reason <- .syn_parse_error_reason(resp)
      
      # Retry only for 429 + 5xx
      if (!is.na(code) && (code == 429L || code >= 500L) && attempt < max_tries) {
        wait <- if (!is.null(resp)) .syn_retry_wait(resp, attempt) else min(60, 2 ^ (attempt - 1))
        .syn_verbose(verbose, sprintf("[syn_request] %s %s -> HTTP %s; retrying in %.2fs",
                                      method, path, code, wait))
        Sys.sleep(wait)
        next
      }
      
      # non-retryable or exhausted
      .syn_stop(
        "HTTP ", code, " for ", method, " ", path,
        if (!is.null(reason)) paste0("\nSynapse reason: ", reason) else ""
      )
    }
    
    # ---- Success: return parsed JSON (or raw response if non-JSON) ----
    resp <- out
    code <- resp_status(resp)
    
    # Retry for success-ish endpoints? no.
    ct <- tryCatch(resp_header(resp, "Content-Type"), error = function(e) "")
    if (grepl("application/json", ct, fixed = TRUE)) {
      return(resp_body_json(resp, simplifyVector = FALSE))
    }
    return(resp)
  }
  
  .syn_stop("Unreachable: syn_request loop ended unexpectedly for ", method, " ", path)
}

# Validation that avoids embedding a NUL in a string literal (your earlier parse error)
.syn_validate_segment <- function(seg) {
  if (!nzchar(seg)) .syn_stop("Invalid folder path segment: empty")
  # any ASCII control char (includes NUL) => reject
  if (grepl("[[:cntrl:]]", seg)) .syn_stop("Invalid folder path segment: control characters not allowed")
  if (grepl("/", seg, fixed = TRUE)) .syn_stop("Invalid folder path segment: must not contain '/'")
  invisible(TRUE)
}

.syn_norm_path <- function(path) {
  path <- gsub("\\\\", "/", path)
  path <- gsub("/{2,}", "/", path)
  path <- gsub("^/+", "", path)
  path <- gsub("/+$", "", path)
  path
}

.syn_guess_content_type <- function(path) {
  ext <- tolower(tools::file_ext(path))
  if (ext %in% c("csv")) return("text/csv")
  if (ext %in% c("tsv")) return("text/tab-separated-values")
  if (ext %in% c("parquet")) return("application/octet-stream")
  if (ext %in% c("json")) return("application/json")
  "application/octet-stream"
}

# MD5 helpers (hex)
.syn_md5_file_hex <- function(path) {
  if (!requireNamespace("digest", quietly = TRUE)) {
    .syn_stop("Package 'digest' is required for MD5 (install.packages('digest')).")
  }
  digest::digest(file = path, algo = "md5", serialize = FALSE)
}
.syn_md5_raw_hex <- function(raw) {
  if (!requireNamespace("digest", quietly = TRUE)) {
    .syn_stop("Package 'digest' is required for MD5 (install.packages('digest')).")
  }
  digest::digest(raw, algo = "md5", serialize = FALSE)
}

# =========================
# Entity helpers
# =========================

syn_get_entity <- function(entity_id, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(entity_id))
  syn_request("GET", paste0("/repo/v1/entity/", entity_id),
              token = token, verbose = verbose, dry_run = dry_run)
}

# Child lookup by name under a parent (fast; avoids listing all children)
syn_lookup_child_id <- function(parent_id, name, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(parent_id), .syn_is_scalar_chr(name))
  body <- list(parentId = parent_id, entityName = name)
  out <- syn_request("POST", "/repo/v1/entity/child",
                     body = body, body_json = TRUE,
                     token = token, verbose = verbose, dry_run = dry_run)
  # returns { "id": "syn123" } OR 404 error
  out$id
}

syn_create_folder <- function(parent_id, name, description = NULL,
                              token = NULL, verbose = FALSE, dry_run = FALSE) {
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

# Ensure folder path exists under parent_id (Project or Folder)
syn_ensure_folder_path <- function(parent_id, path,
                                   token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(parent_id), .syn_is_scalar_chr(path))
  
  # Validate parent exists and is a container
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
    child_id <- NULL
    
    # Lookup child id by name; if not found, create folder
    child_id <- tryCatch(
      syn_lookup_child_id(cur, seg, token = token, verbose = verbose, dry_run = dry_run),
      error = function(e) {
        msg <- conditionMessage(e)
        # Synapse returns 404 for missing child; treat as "create needed"
        if (grepl("^HTTP 404", msg)) return(NULL)
        stop(e)
      }
    )
    
    if (is.null(child_id)) {
      created <- syn_create_folder(cur, seg, token = token, verbose = verbose, dry_run = dry_run)
      cur <- created$id %||% created$properties$id %||% created$entity$id
      if (!.syn_is_scalar_chr(cur)) {
        # fallback: try standard response key
        cur <- created$properties$id
      }
      if (!.syn_is_scalar_chr(cur)) .syn_stop("Folder create returned unexpected payload.")
    } else {
      # Ensure it's actually a Folder (not a file/entity with same name)
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

# =========================
# Multipart upload + FileEntity create/update
# =========================

# Fetch upload destination locations for a container (Project/Folder)
.syn_get_upload_destination_locations <- function(container_id, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(container_id))
  out <- syn_request("GET", paste0("/file/v1/entity/", container_id, "/uploadDestinationLocations"),
                     token = token, verbose = verbose, dry_run = dry_run)
  
  # Response: { "list": [ { "storageLocationId": ..., ... }, ... ] }
  locs <- out$list
  if (is.null(locs) || !length(locs)) {
    .syn_stop("No upload destination locations returned for container ", container_id)
  }
  locs
}

# PUT bytes to a presigned URL (NO bearer auth header)
.syn_put_presigned <- function(url, raw_bytes, signed_headers = NULL, verbose = FALSE) {
  req <- httr2::request(url) |>
    httr2::req_method("PUT") |>
    httr2::req_body_raw(raw_bytes) |>
    httr2::req_options(timeout = 300, connecttimeout = 30)
  
  # signed_headers is a map of required headers
  if (!is.null(signed_headers) && length(signed_headers)) {
    # convert named list to headers
    req <- httr2::req_headers(req, !!!signed_headers)
  }
  
  resp <- tryCatch(httr2::req_perform(req), error = function(e) e)
  if (inherits(resp, "httr2_http") || inherits(resp, "curl_error") || inherits(resp, "httr2_failure")) {
    # if it's an httr2_http condition, include status when possible
    if (inherits(resp, "httr2_http")) {
      r <- .syn_extract_resp(resp)
      code <- if (!is.null(r)) resp_status(r) else NA_integer_
      .syn_stop("Presigned upload failed: HTTP ", code, " (", conditionMessage(resp), ")")
    }
    .syn_stop("Presigned upload failed: ", conditionMessage(resp))
  }
  code <- resp_status(resp)
  if (code < 200 || code >= 300) {
    .syn_stop("Presigned upload failed: HTTP ", code)
  }
  invisible(TRUE)
}

# Multipart upload flow:
# returns list(uploadId, resultFileHandleId, fileSizeBytes, partSizeBytes, contentMD5Hex)
.syn_multipart_upload <- function(local_path,
                                  storageLocationId,
                                  fileName,
                                  contentType,
                                  partSizeBytes = NULL,
                                  token = NULL,
                                  verbose = FALSE,
                                  dry_run = FALSE) {
  
  stopifnot(.syn_is_scalar_chr(local_path), file.exists(local_path))
  info <- file.info(local_path)
  if (is.na(info$size) || info$size <= 0) .syn_stop("local_path is empty or unreadable: ", local_path)
  
  fileSizeBytes <- as.numeric(info$size)
  contentMD5Hex <- .syn_md5_file_hex(local_path)
  
  # Choose part size: >= 5 MiB; also avoid too many parts (rule of thumb)
  if (is.null(partSizeBytes)) {
    partSizeBytes <- max(5 * 1024^2, ceiling(fileSizeBytes / 5000)) # target <= ~5000 parts
  }
  partSizeBytes <- as.numeric(partSizeBytes)
  
  # 1) Start multipart upload
  body <- list(
    concreteType      = "org.sagebionetworks.repo.model.file.MultipartUploadRequest",
    fileName          = fileName,
    contentType       = contentType,
    fileSizeBytes     = fileSizeBytes,
    contentMD5Hex     = contentMD5Hex,
    storageLocationId = as.numeric(storageLocationId),
    partSizeBytes     = partSizeBytes
  )
  
  status <- syn_request("POST", "/file/v1/file/multipart",
                        body = body, body_json = TRUE,
                        token = token, verbose = verbose, dry_run = dry_run)
  
  uploadId <- status$uploadId
  if (!.syn_is_scalar_chr(uploadId)) {
    .syn_stop("Multipart start did not return uploadId. Payload keys: ",
              paste(names(status), collapse = ", "))
  }
  
  # Determine parts
  n_parts <- as.integer(ceiling(fileSizeBytes / partSizeBytes))
  if (n_parts < 1L) n_parts <- 1L
  
  .syn_verbose(verbose, sprintf("[multipart] uploadId=%s size=%s partSize=%s parts=%s",
                                uploadId, format(fileSizeBytes, scientific = FALSE),
                                format(partSizeBytes, scientific = FALSE), n_parts))
  
  # Open file connection for chunked reading
  con <- file(local_path, open = "rb")
  on.exit(try(close(con), silent = TRUE), add = TRUE)
  
  # Helper: request presigned URLs in batches (avoid giant payloads)
  get_presigned_batch <- function(part_numbers) {
    req_body <- list(uploadId = uploadId, partNumbers = as.list(as.integer(part_numbers)))
    syn_request("POST", paste0("/file/v1/file/multipart/", uploadId, "/presigned/url/batch"),
                body = req_body, body_json = TRUE,
                token = token, verbose = verbose, dry_run = dry_run)
  }
  
  # We'll process parts sequentially (simple + robust).
  # Batch URL fetch to reduce API calls.
  batch_size <- 50L
  part_num <- 1L
  
  while (part_num <= n_parts) {
    batch_parts <- part_num:min(n_parts, part_num + batch_size - 1L)
    
    presigned <- get_presigned_batch(batch_parts)
    urls <- presigned$partPresignedUrls
    if (is.null(urls) || !length(urls)) {
      .syn_stop("No presigned URLs returned for parts: ", paste(batch_parts, collapse = ","))
    }
    
    # Build lookup by partNumber
    url_map <- list()
    for (u in urls) {
      pn <- u$partNumber
      if (!is.null(pn)) url_map[[as.character(pn)]] <- u
    }
    
    for (pn in batch_parts) {
      # read next chunk
      remaining <- fileSizeBytes - (as.numeric(pn - 1L) * partSizeBytes)
      this_size <- as.integer(min(partSizeBytes, remaining))
      raw_bytes <- readBin(con, what = "raw", n = this_size)
      
      if (length(raw_bytes) != this_size) {
        .syn_stop("Short read on part ", pn, ": expected ", this_size, " bytes, got ", length(raw_bytes))
      }
      
      part_md5_hex <- .syn_md5_raw_hex(raw_bytes)
      
      u <- url_map[[as.character(pn)]]
      if (is.null(u) || is.null(u$uploadPresignedUrl)) {
        .syn_stop("Missing presigned URL for part ", pn)
      }
      
      # 2) Upload bytes to presigned URL
      if (isTRUE(dry_run)) {
        .syn_verbose(TRUE, sprintf("[dry_run] PUT presigned (part %s) bytes=%s", pn, this_size))
      } else {
        .syn_put_presigned(
          url = u$uploadPresignedUrl,
          raw_bytes = raw_bytes,
          signed_headers = u$signedHeaders %||% NULL,
          verbose = verbose
        )
      }
      
      # 3) Tell Synapse part was uploaded (register MD5)
      syn_request("PUT", paste0("/file/v1/file/multipart/", uploadId, "/add/", pn),
                  query = list(partMD5Hex = part_md5_hex),
                  token = token, verbose = verbose, dry_run = dry_run)
      
      .syn_verbose(verbose, sprintf("[multipart] uploaded+added part %s/%s", pn, n_parts))
    }
    
    part_num <- max(batch_parts) + 1L
  }
  
  # 4) Complete multipart upload
  done <- syn_request("PUT", paste0("/file/v1/file/multipart/", uploadId, "/complete"),
                      token = token, verbose = verbose, dry_run = dry_run)
  
  fh <- done$resultFileHandleId
  if (!.syn_is_scalar_chr(fh)) {
    .syn_stop("Multipart complete did not return resultFileHandleId. Payload keys: ",
              paste(names(done), collapse = ", "))
  }
  
  list(
    uploadId = uploadId,
    resultFileHandleId = fh,
    fileSizeBytes = fileSizeBytes,
    partSizeBytes = partSizeBytes,
    contentMD5Hex = contentMD5Hex
  )
}

# Create or update FileEntity to point to a file handle
syn_create_file_entity <- function(parent_id, name, dataFileHandleId,
                                   description = NULL,
                                   token = NULL, verbose = FALSE, dry_run = FALSE) {
  
  body <- list(
    concreteType     = "org.sagebionetworks.repo.model.FileEntity",
    parentId         = parent_id,
    name             = name,
    dataFileHandleId = dataFileHandleId
  )
  if (!is.null(description)) body$description <- description
  
  syn_request("POST", "/repo/v1/entity",
              body = body, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

syn_update_file_entity <- function(entity_id, new_dataFileHandleId,
                                   token = NULL, verbose = FALSE, dry_run = FALSE) {
  ent <- syn_get_entity(entity_id, token = token, verbose = verbose, dry_run = dry_run)
  ctype <- ent$concreteType %||% ""
  if (!grepl("FileEntity$", ctype)) .syn_stop("Entity is not a FileEntity: ", entity_id, " type=", ctype)
  
  ent$dataFileHandleId <- new_dataFileHandleId
  
  # PUT requires id in path; include existing etag etc as returned
  syn_request("PUT", paste0("/repo/v1/entity/", entity_id),
              body = ent, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

# Public: upload a local file into a Synapse folder/project
syn_upload_file <- function(local_path,
                            parent_id,
                            name = NULL,
                            contentType = NULL,
                            description = NULL,
                            overwrite = FALSE,
                            token = NULL,
                            verbose = FALSE,
                            dry_run = FALSE) {
  
  stopifnot(.syn_is_scalar_chr(local_path), file.exists(local_path))
  stopifnot(.syn_is_scalar_chr(parent_id))
  
  parent <- syn_get_entity(parent_id, token = token, verbose = verbose, dry_run = dry_run)
  ptype <- parent$concreteType %||% ""
  if (!grepl("Project$|Folder$", ptype)) {
    .syn_stop("parent_id must be a Project or Folder. Got: ", ptype)
  }
  
  fileName <- .syn_coalesce(name, basename(local_path))
  .syn_validate_segment(fileName)
  
  ctype <- .syn_coalesce(contentType, .syn_guess_content_type(local_path))
  
  # Choose an upload destination location
  locs <- .syn_get_upload_destination_locations(parent_id, token = token, verbose = verbose, dry_run = dry_run)
  first <- locs[[1]]
  if (!is.list(first) || is.null(first$storageLocationId)) {
    .syn_stop("Unexpected uploadDestinationLocations payload (missing storageLocationId).")
  }
  storageLocationId <- first$storageLocationId
  
  # Multipart upload => yields file handle id
  up <- .syn_multipart_upload(
    local_path = local_path,
    storageLocationId = storageLocationId,
    fileName = fileName,
    contentType = ctype,
    token = token,
    verbose = verbose,
    dry_run = dry_run
  )
  
  # Determine if FileEntity with same name already exists under parent
  existing_id <- tryCatch(
    syn_lookup_child_id(parent_id, fileName, token = token, verbose = verbose, dry_run = dry_run),
    error = function(e) {
      msg <- conditionMessage(e)
      if (grepl("^HTTP 404", msg)) return(NULL)
      stop(e)
    }
  )
  
  if (!is.null(existing_id)) {
    if (!isTRUE(overwrite)) {
      .syn_stop(
        "A child named '", fileName, "' already exists under parent ", parent_id, ": ", existing_id, "\n",
        "Set overwrite=TRUE to update the existing FileEntity."
      )
    }
    updated <- syn_update_file_entity(existing_id, up$resultFileHandleId,
                                      token = token, verbose = verbose, dry_run = dry_run)
    file_entity_id <- updated$id %||% updated$properties$id %||% existing_id
    return(list(
      file_entity_id = file_entity_id,
      file_handle_id = up$resultFileHandleId,
      upload_id      = up$uploadId,
      md5_hex        = up$contentMD5Hex,
      size_bytes     = up$fileSizeBytes
    ))
  }
  
  created <- syn_create_file_entity(parent_id, fileName, up$resultFileHandleId,
                                    description = description,
                                    token = token, verbose = verbose, dry_run = dry_run)
  file_entity_id <- created$id %||% created$properties$id
  if (!.syn_is_scalar_chr(file_entity_id)) .syn_stop("FileEntity create returned unexpected payload.")
  
  list(
    file_entity_id = file_entity_id,
    file_handle_id = up$resultFileHandleId,
    upload_id      = up$uploadId,
    md5_hex        = up$contentMD5Hex,
    size_bytes     = up$fileSizeBytes
  )
}






# 
# # source("synapse_rest_backend.R")
# 
# token <- Sys.getenv("SYNAPSE_PAT")
# project_id <- "syn64728425"
# 
# folder_id <- syn_ensure_folder_path(
#   project_id,
#   "zz_smoketests/synapse_rw",
#   token = token,
#   verbose = TRUE
# )
# 
# 
# # folder_id <- 'syn72246746'
# # syn72246747
# 
# 
# tmp <- tempfile(fileext = ".csv")
# write.csv(data.frame(x=1:5, y=letters[1:5]), tmp, row.names = FALSE)
# 
# up <- syn_upload_file(
#   tmp,
#   parent_id = folder_id,
#   token = token,
#   verbose = TRUE,
#   overwrite = TRUE,
#   description = "Test upload (pure REST multipart)"
# )
# 
# up
# 
# 
# 
# 
# 
# e <- syn_get_entity("syn72248755", token = token, verbose = TRUE)
# e$concreteType
# e$dataFileHandleId
# 
# 
# 
# 
# dest <- tempfile(fileext = ".csv")
# dl_path <- syn_download_file("syn72248755", dest_path = dest, overwrite = TRUE,
#                              token = token, verbose = TRUE)
# 
# # verify md5 matches what upload returned
# if (requireNamespace("digest", quietly = TRUE)) {
#   md5_dl <- digest::digest(file = dl_path, algo = "md5", serialize = FALSE)
#   md5_dl
# }
# 
# read.csv(dest)




#' List children of a container (project or folder)
#'
#' @param parent_id Synapse ID of a Project or Folder.
#' @param include_types Character vector of Synapse types to include.
#'   Common values: "folder", "file", "table", "link", "entityview", "dockerrepo".
#' @param page_size Page size for pagination (Synapse uses tokens).
#' @param token Synapse PAT.
#' @param verbose Logical.
#' @param dry_run Logical.
#'
#' @return data.frame with columns: id, name, type
#' @export
syn_get_children <- function(parent_id,
                             include_types = c("folder", "file"),
                             page_size = 50L,
                             token = NULL,
                             verbose = FALSE,
                             dry_run = FALSE) {
  stopifnot(.syn_is_id(parent_id))
  stopifnot(is.character(include_types), length(include_types) >= 1L)
  stopifnot(is.numeric(page_size), length(page_size) == 1L)
  
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] POST /repo/v1/entity/children (parentId=%s)", parent_id))
    return(invisible(list(dry_run = TRUE, parent_id = parent_id, include_types = include_types)))
  }
  
  all_rows <- list()
  next_token <- NULL
  
  repeat {
    body <- list(
      parentId = parent_id,
      includeTypes = as.list(include_types),
      sortBy = "NAME",
      sortDirection = "ASC",
      resultsPerPage = as.integer(page_size)
    )
    if (!is.null(next_token)) body$nextPageToken <- next_token
    
    resp <- syn_request("POST", "/repo/v1/entity/children",
                        body = body, body_json = TRUE,
                        token = token, verbose = verbose)
    
    # Response shape typically:
    # {
    #   "page": [{"id":"syn..","name":"..","type":"folder"}, ...],
    #   "nextPageToken": "..."
    # }
    page <- resp$page %||% list()
    if (length(page)) {
      df <- data.frame(
        id   = vapply(page, function(x) x$id   %||% NA_character_, character(1)),
        name = vapply(page, function(x) x$name %||% NA_character_, character(1)),
        type = vapply(page, function(x) x$type %||% NA_character_, character(1)),
        stringsAsFactors = FALSE
      )
      all_rows[[length(all_rows) + 1L]] <- df
    }
    
    next_token <- resp$nextPageToken %||% NULL
    if (is.null(next_token) || !nzchar(next_token)) break
  }
  
  if (!length(all_rows)) {
    return(data.frame(id=character(), name=character(), type=character(), stringsAsFactors = FALSE))
  }
  do.call(rbind, all_rows)
}

#' Get the relative path of an entity under a base container
#'
#' Example: base_project_id="syn64728425", entity_id="syn72246747"
#' might return "zz_smoketests/synapse_rw".
#'
#' @param base_id Synapse ID of the base container (typically a Project).
#' @param entity_id Synapse ID of a descendant entity (Folder/File).
#' @param token Synapse PAT.
#' @param verbose Logical.
#' @param dry_run Logical.
#'
#' @return Character scalar path relative to base_id (no leading slash).
#' @export
syn_entity_relpath <- function(base_id,
                               entity_id,
                               token = NULL,
                               verbose = FALSE,
                               dry_run = FALSE) {
  stopifnot(.syn_is_id(base_id), .syn_is_id(entity_id))
  
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] walk parentId chain: %s -> ... -> %s", entity_id, base_id))
    return(invisible(list(dry_run = TRUE, base_id = base_id, entity_id = entity_id)))
  }
  
  parts <- character()
  cur <- entity_id
  seen <- new.env(parent = emptyenv())  # cycle protection
  
  repeat {
    if (!is.null(seen[[cur]])) .syn_stop("Cycle detected while walking parents at: ", cur)
    seen[[cur]] <- TRUE
    
    ent <- syn_get_entity(cur, token = token, verbose = verbose)
    
    # If we hit the base container, stop (do not include base name)
    if (identical(cur, base_id)) break
    
    nm <- ent$name %||% NULL
    pid <- ent$parentId %||% NULL
    
    if (is.null(nm) || !nzchar(nm)) .syn_stop("Missing name for entity: ", cur)
    if (is.null(pid) || !nzchar(pid)) {
      .syn_stop("Entity ", entity_id, " is not under base_id ", base_id,
                " (hit top without reaching base). Last entity: ", cur)
    }
    
    parts <- c(nm, parts)   # prepend
    cur <- pid
  }
  
  paste(parts, collapse = "/")
}


#' Get the relative path of an entity under a base container
#'
#' Example: base_project_id="syn64728425", entity_id="syn72246747"
#' might return "zz_smoketests/synapse_rw".
#'
#' @param base_id Synapse ID of the base container (typically a Project).
#' @param entity_id Synapse ID of a descendant entity (Folder/File).
#' @param token Synapse PAT.
#' @param verbose Logical.
#' @param dry_run Logical.
#'
#' @return Character scalar path relative to base_id (no leading slash).
#' @export
syn_entity_relpath <- function(base_id,
                               entity_id,
                               token = NULL,
                               verbose = FALSE,
                               dry_run = FALSE) {
  stopifnot(.syn_is_id(base_id), .syn_is_id(entity_id))

  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] walk parentId chain: %s -> ... -> %s", entity_id, base_id))
    return(invisible(list(dry_run = TRUE, base_id = base_id, entity_id = entity_id)))
  }

  parts <- character()
  cur <- entity_id
  seen <- new.env(parent = emptyenv())  # cycle protection

  repeat {
    if (!is.null(seen[[cur]])) .syn_stop("Cycle detected while walking parents at: ", cur)
    seen[[cur]] <- TRUE

    ent <- syn_get_entity(cur, token = token, verbose = verbose)

    # If we hit the base container, stop (do not include base name)
    if (identical(cur, base_id)) break

    nm <- ent$name %||% NULL
    pid <- ent$parentId %||% NULL

    if (is.null(nm) || !nzchar(nm)) .syn_stop("Missing name for entity: ", cur)
    if (is.null(pid) || !nzchar(pid)) {
      .syn_stop("Entity ", entity_id, " is not under base_id ", base_id,
                " (hit top without reaching base). Last entity: ", cur)
    }

    parts <- c(nm, parts)   # prepend
    cur <- pid
  }

  paste(parts, collapse = "/")
}
# > syn_get_children
# function(parent_id, include_types = c("folder","file"),
#          token = NULL, verbose = FALSE) {
#   
#   body <- list(
#     parentId = parent_id,
#     includeTypes = as.list(include_types),
#     sortBy = "NAME",
#     sortDirection = "ASC"
#   )
#   
#   res <- syn_request("POST", "/repo/v1/entity/children",
#                      body = body, token = token, verbose = verbose)
#   
#   page <- res$page %||% list()
#   if (!length(page)) {
#     return(data.frame(id=character(), name=character(), type=character()))
#   }
#   
#   data.frame(
#     id   = vapply(page, `[[`, character(1), "id"),
#     name = vapply(page, `[[`, character(1), "name"),
#     type = vapply(page, `[[`, character(1), "type"),
#     stringsAsFactors = FALSE
#   )
# }
# 
# 
# token <- Sys.getenv("SYNAPSE_PAT")
# base  <- "syn64728425"
# leaf  <- "syn72246747"
# 
# syn_entity_relpath(base, leaf, token = token, verbose = TRUE)
# # "zz_smoketests/synapse_rw"




#' @keywords internal
.syn_norm_path <- function(x) {
  stopifnot(is.character(x), length(x) == 1L)
  x <- gsub("\\\\", "/", x)          # allow Windows-style slashes
  x <- gsub("/+", "/", x)            # collapse repeated slashes
  x <- gsub("^/|/$", "", x)          # trim leading/trailing slash
  x
}

#' @keywords internal
.syn_split_path <- function(path) {
  path <- .syn_norm_path(path)
  if (!nzchar(path)) return(character())
  parts <- strsplit(path, "/", fixed = TRUE)[[1]]
  parts[nzchar(parts)]
}

#' Resolve a nested folder path under a base container WITHOUT creating anything
#'
#' @param base_id Synapse ID of Project or Folder to treat as the root.
#' @param folder_path Character path like "a/b/c" (no leading slash needed).
#' @param token Synapse PAT.
#' @param verbose Logical.
#' @param dry_run Logical.
#'
#' @return Synapse ID of the final folder.
#' @export
syn_resolve_folder_path <- function(base_id,
                                    folder_path,
                                    token = NULL,
                                    verbose = FALSE,
                                    dry_run = FALSE) {
  stopifnot(.syn_is_id(base_id))
  stopifnot(is.character(folder_path), length(folder_path) == 1L)
  
  folder_path <- .syn_norm_path(folder_path)
  parts <- .syn_split_path(folder_path)
  
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] would resolve folder path under %s: %s", base_id, folder_path))
    return(invisible(list(dry_run = TRUE, base_id = base_id, folder_path = folder_path)))
  }
  
  cur <- base_id
  if (!length(parts)) return(cur)
  
  for (seg in parts) {
    kids <- syn_get_children(cur, include_types = "folder", token = token, verbose = verbose)
    hit <- kids[kids$name == seg, , drop = FALSE]
    
    if (nrow(hit) == 1L) {
      cur <- hit$id[1]
      next
    }
    
    if (nrow(hit) == 0L) {
      stop("Folder path does not exist under base_id. Missing segment: '", seg,
           "' in path: '", folder_path, "'", call. = FALSE)
    }
    
    # Shouldn't normally happen, but be defensive
    stop("Ambiguous folder name '", seg, "' under parent ", cur,
         " (multiple matches).", call. = FALSE)
  }
  
  cur
}




#' Find a file entity (by name) in a folder
#'
#' @param folder_id Synapse ID of Folder.
#' @param file_name Name of file entity to locate (exact match).
#' @param token Synapse PAT.
#' @param verbose Logical.
#' @param dry_run Logical.
#'
#' @return Synapse FileEntity ID (e.g., "syn123...").
#' @export
syn_find_file_in_folder <- function(folder_id,
                                    file_name,
                                    token = NULL,
                                    verbose = FALSE,
                                    dry_run = FALSE) {
  stopifnot(.syn_is_id(folder_id))
  stopifnot(is.character(file_name), length(file_name) == 1L, nzchar(file_name))
  
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] would search for file '%s' under %s", file_name, folder_id))
    return(invisible(list(dry_run = TRUE, folder_id = folder_id, file_name = file_name)))
  }
  
  kids <- syn_get_children(folder_id, include_types = "file", token = token, verbose = verbose)
  hit <- kids[kids$name == file_name, , drop = FALSE]
  
  if (nrow(hit) == 1L) return(hit$id[1])
  if (nrow(hit) == 0L) {
    stop("File not found in folder ", folder_id, ": '", file_name, "'", call. = FALSE)
  }
  stop("Ambiguous file name '", file_name, "' under folder ", folder_id,
       " (multiple matches).", call. = FALSE)
}




#' Upload a local file to a remote Synapse folder specified by path
#'
#' This is a convenience wrapper:
#'   1) ensures folder path exists under a base project/folder
#'   2) uploads the local file into the resolved folder
#'
#' @param local_path Local file path to upload.
#' @param base_id Synapse Project/Folder ID used as the root for `remote_folder_path`.
#' @param remote_folder_path Path like "zz_smoketests/synapse_rw/test2".
#' @param name Optional remote FileEntity name (defaults to basename(local_path)).
#' @param ... Passed through to syn_upload_file() (e.g., overwrite, description, annotations, contentType, md5, verbose, token, dry_run).
#'
#' @return Whatever syn_upload_file() returns.
#' @export
syn_upload_file_path <- function(local_path,
                                 base_id,
                                 remote_folder_path,
                                 name = NULL,
                                 ...) {
  stopifnot(.syn_is_id(base_id))
  stopifnot(is.character(remote_folder_path), length(remote_folder_path) == 1L)
  
  remote_folder_path <- .syn_norm_path(remote_folder_path)
  
  # Pull token/verbose/dry_run from ...
  dots <- list(...)
  token   <- dots$token   %||% NULL
  verbose <- isTRUE(dots$verbose %||% FALSE)
  dry_run <- isTRUE(dots$dry_run %||% FALSE)
  
  folder_id <- syn_ensure_folder_path(base_id, remote_folder_path,
                                      token = token, verbose = verbose, dry_run = dry_run)
  
  # Prefer explicit name; else syn_upload_file will default to basename(local_path) if you built it that way
  syn_upload_file(local_path, parent_id = folder_id, name = name, ...)
}







#' @keywords internal
.syn_has_formal <- function(fun, name) {
  name %in% names(formals(fun))
}

#' @keywords internal
.syn_rename_dots_for_upload <- function(dots) {
  # If caller provided annotations=..., adapt to what syn_upload_file() supports.
  if (!is.null(dots$annotations)) {
    if (.syn_has_formal(syn_upload_file, "annotations")) {
      # ok as-is
    } else if (.syn_has_formal(syn_upload_file, "extra_annotations")) {
      dots$extra_annotations <- dots$annotations
      dots$annotations <- NULL
    } else {
      stop(
        "syn_upload_file() does not accept `annotations` (or `extra_annotations`). ",
        "Either add one of those parameters to syn_upload_file(), or remove annotations from the call.",
        call. = FALSE
      )
    }
  }
  dots
}

#' @keywords internal
.syn_norm_path <- function(x) {
  stopifnot(is.character(x), length(x) == 1L)
  x <- gsub("\\\\", "/", x)
  x <- gsub("/+", "/", x)
  x <- gsub("^/|/$", "", x)
  x
}

#' Upload a local file to a remote Synapse folder specified by path
#'
#' @param local_path Local file path to upload.
#' @param base_id Synapse Project/Folder ID used as the root for remote_folder_path.
#' @param remote_folder_path Remote folder path like "zz_smoketests/synapse_rw/test2".
#' @param name Optional remote FileEntity name.
#' @param ... Passed through to syn_upload_file() (and syn_ensure_folder_path via token/verbose/dry_run).
#'
#' @return Whatever syn_upload_file() returns.
#' @export
syn_upload_file_path <- function(local_path,
                                 base_id,
                                 remote_folder_path,
                                 name = NULL,
                                 ...) {
  stopifnot(.syn_is_id(base_id))
  stopifnot(is.character(remote_folder_path), length(remote_folder_path) == 1L)
  
  remote_folder_path <- .syn_norm_path(remote_folder_path)
  
  dots <- list(...)
  dots <- .syn_rename_dots_for_upload(dots)
  
  token   <- dots$token   %||% NULL
  verbose <- isTRUE(dots$verbose %||% FALSE)
  dry_run <- isTRUE(dots$dry_run %||% FALSE)
  
  folder_id <- syn_ensure_folder_path(
    parent_id = base_id,
    path      = remote_folder_path,
    token     = token,
    verbose   = verbose,
    dry_run   = dry_run
  )
  
  # Call syn_upload_file() with the adapted dots
  do.call(
    syn_upload_file,
    c(list(local_path = local_path, parent_id = folder_id, name = name), dots)
  )
}


#' 
#' 
#' 
#' token <- Sys.getenv("SYNAPSE_PAT")
#' 
#' up <- syn_upload_file_path(
#'   local_path = "C:/tmp/test.csv",
#'   base_id = "syn64728425",
#'   remote_folder_path = "zz_smoketests/synapse_rw/test2",
#'   token = token,
#'   overwrite = TRUE,
#'   description = "Uploaded via path wrapper",
#'   annotations = list(stage = "smoke", kind = "csv"),
#'   verbose = TRUE
#' )
#' 
#' 
#' #' Download a Synapse file specified by a remote path
#'
#' Remote path format: "folderA/folderB/file.csv" relative to base_id.
#' This wrapper:
#'   1) resolves the folder path (no create)
#'   2) finds the FileEntity by name
#'   3) downloads via syn_download_file(entity_id, ...)
#'
#' @param base_id Synapse Project/Folder ID used as root.
#' @param remote_file_path Remote path including the file name, e.g. "zz_smoketests/synapse_rw/test2/test.csv".
#' @param dest_path Local destination path.
#' @param overwrite Logical.
#' @param token Synapse PAT.
#' @param verbose Logical.
#' @param dry_run Logical.
#'
#' @return The downloaded local path (from syn_download_file()).
#' @export
syn_download_file_path <- function(base_id,
                                   remote_file_path,
                                   dest_path,
                                   overwrite = FALSE,
                                   token = NULL,
                                   verbose = FALSE,
                                   dry_run = FALSE) {
  stopifnot(.syn_is_id(base_id))
  stopifnot(is.character(remote_file_path), length(remote_file_path) == 1L)
  
  rp <- .syn_norm_path(remote_file_path)
  parts <- .syn_split_path(rp)
  if (length(parts) < 1L) stop("remote_file_path is empty.", call. = FALSE)
  
  file_name <- tail(parts, 1L)
  folder_parts <- if (length(parts) > 1L) parts[-length(parts)] else character()
  folder_path <- paste(folder_parts, collapse = "/")
  
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] would resolve folder='%s' and download file='%s' -> %s",
                               folder_path, file_name, dest_path))
    return(invisible(list(dry_run = TRUE, base_id = base_id, remote_file_path = rp, dest_path = dest_path)))
  }
  
  folder_id <- if (nzchar(folder_path)) {
    syn_resolve_folder_path(base_id, folder_path, token = token, verbose = verbose, dry_run = dry_run)
  } else {
    base_id
  }
  
  entity_id <- syn_find_file_in_folder(folder_id, file_name, token = token, verbose = verbose, dry_run = dry_run)
  
  syn_download_file(entity_id, dest_path = dest_path, overwrite = overwrite,
                    token = token, verbose = verbose, dry_run = dry_run)
}

# 
# token <- Sys.getenv("SYNAPSE_PAT")
# 
# dl <- syn_download_file_path(
#   base_id = "syn64728425",
#   remote_file_path = "zz_smoketests/synapse_rw/file650c7dbf6466.csv",
#   dest_path = "C:/tmp/downloads/test.csv",
#   overwrite = TRUE,
#   token = token,
#   verbose = TRUE
# )
# 
#  
# 


































# ==============================================================================
# PURE REST Synapse helpers (httr2 + jsonlite + digest)
# - Folder path ensure
# - Multipart upload
# - Download (fileHandle presigned URL)
# - Annotations (annotations2)
# - Convenience wrappers using "remote paths"
# ==============================================================================

suppressPackageStartupMessages({
  library(httr2)
  library(jsonlite)
  library(digest)
})

# -----------------------------
# Small utilities
# -----------------------------
.syn_stop <- function(...) stop(paste0(...), call. = FALSE)

.syn_is_scalar_chr <- function(x) is.character(x) && length(x) == 1L && !is.na(x)
.syn_is_id <- function(x) .syn_is_scalar_chr(x) && grepl("^syn\\d+$", x)

`%||%` <- function(a, b) {
  if (is.null(a) || length(a) == 0L) return(b)
  if (length(a) == 1L) {
    if (is.na(a)) return(b)
    if (is.character(a) && identical(a, "")) return(b)
  }
  a
}

.syn_verbose <- function(verbose, msg) if (isTRUE(verbose)) cat(msg, "\n")

syn_resolve_token <- function(token = NULL) {
  if (.syn_is_scalar_chr(token) && nchar(token) > 10) return(token)
  tok <- Sys.getenv("SYNAPSE_PAT", unset = "")
  if (nzchar(tok)) return(tok)
  tok <- Sys.getenv("SYNAPSE_AUTH_TOKEN", unset = "")
  if (nzchar(tok)) return(tok)
  .syn_stop(
    "Missing Synapse token. Provide `token=...` or set env var SYNAPSE_PAT (preferred) ",
    "or SYNAPSE_AUTH_TOKEN."
  )
}

.syn_base_url <- function() "https://repo-prod.prod.sagebase.org"

.syn_validate_segment <- function(seg) {
  if (!nzchar(seg)) .syn_stop("Invalid folder path segment: empty")
  # Use [:cntrl:] to avoid embedding literal NUL which R can't parse in strings
  if (grepl("[[:cntrl:]]", seg, perl = TRUE)) {
    .syn_stop("Invalid folder path segment: control characters not allowed")
  }
  # Conservative: disallow "/" since we split on it; also disallow "\".
  if (grepl("[/\\\\]", seg)) .syn_stop("Invalid folder path segment: contains slash")
  invisible(TRUE)
}

.syn_norm_path <- function(path) {
  stopifnot(.syn_is_scalar_chr(path))
  p <- gsub("\\\\", "/", path)
  p <- gsub("/+", "/", p)
  p <- gsub("^/+", "", p)
  p <- gsub("/+$", "", p)
  p
}

.syn_split_path <- function(path) {
  p <- .syn_norm_path(path)
  if (!nzchar(p)) return(character())
  parts <- strsplit(p, "/", fixed = TRUE)[[1]]
  parts <- parts[nzchar(parts)]
  lapply(parts, .syn_validate_segment)
  parts
}

.syn_md5_hex_file <- function(path) digest(file = path, algo = "md5", serialize = FALSE)
.syn_md5_hex_raw  <- function(raw)  digest(raw,  algo = "md5", serialize = FALSE)

.syn_guess_content_type <- function(path) {
  ext <- tolower(tools::file_ext(path))
  switch(ext,
         "csv"  = "text/csv",
         "tsv"  = "text/tab-separated-values",
         "txt"  = "text/plain",
         "json" = "application/json",
         "parquet" = "application/octet-stream",
         "feather" = "application/octet-stream",
         "rds"  = "application/octet-stream",
         "rda"  = "application/octet-stream",
         "zip"  = "application/zip",
         "gz"   = "application/gzip",
         "nc"   = "application/octet-stream",
         "grib" = "application/octet-stream",
         "grb"  = "application/octet-stream",
         "tif"  = "image/tiff",
         "tiff" = "image/tiff",
         "png"  = "image/png",
         "jpg"  = "image/jpeg",
         "jpeg" = "image/jpeg",
         "application/octet-stream"
  )
}

.syn_extract_resp <- function(err) {
  if (inherits(err, "httr2_http")) {
    if (!is.null(err$response)) return(err$response)
    r <- attr(err, "response", exact = TRUE)
    if (!is.null(r)) return(r)
  }
  NULL
}

.syn_parse_error_body <- function(resp) {
  if (is.null(resp)) return(NULL)
  txt <- tryCatch(httr2::resp_body_string(resp), error = function(e) NULL)
  if (is.null(txt) || !nzchar(txt)) return(NULL)
  js <- tryCatch(jsonlite::fromJSON(txt), error = function(e) NULL)
  if (!is.null(js)) return(js)
  list(raw = txt)
}

# -----------------------------
# Robust HTTP layer
# -----------------------------
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
      Accept = "application/json"
    )
  
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
      r <- .syn_extract_resp(resp)
      status <- if (!is.null(r)) httr2::resp_status(r) else NA_integer_
      
      # Respect Retry-After for 429/5xx when available
      if (attempt < max_tries && !is.na(status) && (status == 429L || status >= 500L)) {
        ra <- tryCatch(httr2::resp_header(r, "Retry-After"), error = function(e) NA_character_)
        wait <- suppressWarnings(as.numeric(ra))
        if (!is.finite(wait)) wait <- min(30, (2 ^ (attempt - 1)) + stats::runif(1, 0, 0.5))
        .syn_verbose(verbose, sprintf("[syn_request] %s %s -> %s; retrying in %.2fs", method, path, status, wait))
        Sys.sleep(wait)
        next
      }
      
      # Final error: show Synapse reason when possible
      info <- .syn_parse_error_body(r)
      reason <- NULL
      if (is.list(info)) reason <- info$reason %||% info$message %||% NULL
      if (is.null(reason)) reason <- conditionMessage(resp)
      
      .syn_stop(
        "Synapse HTTP error ", status, " for ", method, " ", path,
        if (!is.null(reason)) paste0(": ", reason) else ""
      )
    }
    
    # Success: parse JSON if possible, else return response
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

# -----------------------------
# Entity operations
# -----------------------------
syn_get_entity <- function(entity_id, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_id(entity_id))
  syn_request("GET", paste0("/repo/v1/entity/", entity_id),
              token = token, verbose = verbose, dry_run = dry_run)
}

# Fast lookup by name under a parent. Uses the "child" endpoint.
syn_get_child_by_name <- function(parent_id,
                                  name,
                                  include_types = c("folder", "file", "project"),
                                  token = NULL,
                                  verbose = FALSE,
                                  dry_run = FALSE) {
  stopifnot(.syn_is_id(parent_id), .syn_is_scalar_chr(name))
  body <- list(parentId = parent_id, name = name, includeTypes = include_types)
  syn_request("POST", "/repo/v1/entity/child",
              body = body, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

# Full children listing (pagination).
syn_get_children <- function(parent_id,
                             include_types = c("folder", "file", "project"),
                             page_size = 100L,
                             token = NULL,
                             verbose = FALSE,
                             dry_run = FALSE) {
  stopifnot(.syn_is_id(parent_id))
  page_size <- as.integer(page_size)
  if (page_size < 1L || page_size > 500L) .syn_stop("page_size must be 1..500")
  
  out <- list()
  next_token <- NULL
  
  repeat {
    body <- list(
      parentId = parent_id,
      includeTypes = include_types,
      nextPageToken = next_token,
      pageSize = page_size
    )
    res <- syn_request("POST", "/repo/v1/entity/children",
                       body = body, body_json = TRUE,
                       token = token, verbose = verbose, dry_run = dry_run)
    
    kids <- res$page %||% res$results %||% res$children %||% list()
    if (length(kids)) out[[length(out) + 1L]] <- kids
    
    next_token <- res$nextPageToken %||% NULL
    if (is.null(next_token) || !nzchar(next_token)) break
  }
  
  # Normalize to a data.frame
  flat <- unlist(out, recursive = FALSE)
  if (!length(flat)) {
    return(data.frame(id = character(), name = character(), type = character(), stringsAsFactors = FALSE))
  }
  
  data.frame(
    id   = vapply(flat, function(x) as.character(x$id %||% NA_character_), character(1)),
    name = vapply(flat, function(x) as.character(x$name %||% NA_character_), character(1)),
    type = vapply(flat, function(x) as.character(x$type %||% NA_character_), character(1)),
    stringsAsFactors = FALSE
  )
}

syn_create_folder <- function(parent_id, name, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_id(parent_id), .syn_is_scalar_chr(name))
  .syn_validate_segment(name)
  
  body <- list(
    concreteType = "org.sagebionetworks.repo.model.Folder",
    name = name,
    parentId = parent_id
  )
  
  syn_request("POST", "/repo/v1/entity",
              body = body, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

# Idempotent ensure: "a/b/c" under a project or folder
syn_ensure_folder_path <- function(parent_id, path, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_id(parent_id), .syn_is_scalar_chr(path))
  parts <- .syn_split_path(path)
  
  # Validate parent exists and is container-like
  parent <- syn_get_entity(parent_id, token = token, verbose = verbose, dry_run = dry_run)
  ct <- parent$concreteType %||% ""
  if (!isTRUE(grepl("(Project|Folder)$", ct))) {
    .syn_stop("parent_id must be a Project or Folder. Got: ", ct)
  }
  
  cur <- parent_id
  if (!length(parts)) return(cur)
  
  for (seg in parts) {
    # child lookup by name
    child <- syn_get_child_by_name(cur, seg, include_types = c("folder"), token = token, verbose = verbose, dry_run = dry_run)
    
    # If found, child may come back as {id,name,type} or NULL-ish
    child_id <- child$id %||% NULL
    child_type <- child$type %||% NULL
    
    if (!is.null(child_id) && nzchar(child_id)) {
      if (!isTRUE(grepl("folder", tolower(child_type %||% "")))) {
        .syn_stop("Path segment exists but is not a folder: ", seg, " (", child_id, ")")
      }
      cur <- child_id
    } else {
      created <- syn_create_folder(cur, seg, token = token, verbose = verbose, dry_run = dry_run)
      cur <- created$id %||% created$entity$id %||% created$properties$id %||% NULL
      if (is.null(cur)) .syn_stop("Folder creation succeeded but no id returned for segment: ", seg)
    }
  }
  
  cur
}

# -----------------------------
# ID -> "english path" (names)
# -----------------------------
syn_get_entity_path <- function(entity_id, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_id(entity_id))
  # Synapse exposes entity path via /repo/v1/entity/{id}/path :contentReference[oaicite:5]{index=5}
  res <- syn_request("GET", paste0("/repo/v1/entity/", entity_id, "/path"),
                     token = token, verbose = verbose, dry_run = dry_run)
  
  # Common shape: list(path = list(list(id=..., name=...), ...))
  p <- res$path %||% res$paths %||% res
  if (!is.list(p)) return(p)
  
  data.frame(
    id   = vapply(p, function(x) as.character(x$id %||% NA_character_), character(1)),
    name = vapply(p, function(x) as.character(x$name %||% NA_character_), character(1)),
    stringsAsFactors = FALSE
  )
}

# -----------------------------
# Annotations (annotations2)
# -----------------------------
.syn_anno_type <- function(x) {
  if (is.logical(x)) return("BOOLEAN")
  if (is.integer(x)) return("LONG")
  if (is.numeric(x)) return("DOUBLE")
  "STRING"
}

.syn_anno_pack <- function(annotations) {
  stopifnot(is.list(annotations), length(names(annotations)) == length(annotations))
  out <- list()
  for (k in names(annotations)) {
    v <- annotations[[k]]
    if (is.null(v)) next
    if (length(v) == 0L) next
    
    # Coerce scalars/vectors to atomic
    if (is.list(v) && !is.data.frame(v)) {
      # allow list but flatten to character to be safe
      v <- unlist(v, recursive = TRUE, use.names = FALSE)
    }
    
    # Ensure atomic vector
    if (!is.atomic(v)) v <- as.character(v)
    
    tp <- .syn_anno_type(v)
    # Values must be serialized as strings for STRING; keep numeric/logical as-is
    if (tp == "STRING") v <- as.character(v)
    
    out[[k]] <- list(
      type  = tp,
      value = unname(as.list(v))
    )
  }
  out
}

syn_get_annotations <- function(entity_id, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_id(entity_id))
  syn_request("GET", paste0("/repo/v1/entity/", entity_id, "/annotations2"),
              token = token, verbose = verbose, dry_run = dry_run)
}

syn_set_annotations <- function(entity_id, annotations, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_id(entity_id))
  if (!is.list(annotations)) .syn_stop("annotations must be a named list")
  
  cur <- syn_get_annotations(entity_id, token = token, verbose = verbose, dry_run = dry_run)
  
  cur_id   <- cur$id   %||% entity_id
  cur_etag <- cur$etag %||% NULL
  cur_ann  <- cur$annotations %||% list()
  
  new_ann <- .syn_anno_pack(annotations)
  
  # merge (overwrite keys provided)
  merged <- cur_ann
  for (k in names(new_ann)) merged[[k]] <- new_ann[[k]]
  
  body <- list(
    id = cur_id,
    etag = cur_etag,
    annotations = merged
  )
  
  # PUT annotations2 (requires etag) :contentReference[oaicite:6]{index=6}
  syn_request("PUT", paste0("/repo/v1/entity/", entity_id, "/annotations2"),
              body = body, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

# -----------------------------
# Multipart upload
# -----------------------------
.syn_pick_storage_location <- function(x) {
  # Response shape varies; handle:
  # - list(locations = [ {storageLocationId=...}, ... ])
  # - or list of location objects
  locs <- x$locations %||% x$uploadDestinationLocations %||% x
  if (!is.list(locs) || !length(locs)) return(NULL)
  
  # If it's a list-of-lists, pick first
  first <- locs[[1]]
  if (!is.list(first)) return(NULL)
  
  first$storageLocationId %||% first$id %||% NULL
}

.syn_put_presigned_part <- function(url, raw_bytes, verbose = FALSE) {
  req <- httr2::request(url) |>
    httr2::req_method("PUT") |>
    httr2::req_body_raw(raw_bytes) |>
    httr2::req_options(timeout = 600, http_version = 1.1)
  
  resp <- httr2::req_perform(req)
  invisible(resp)
}

.syn_multipart_presigned_urls <- function(upload_id, part_numbers, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(upload_id))
  body <- list(uploadId = upload_id, partNumbers = as.integer(part_numbers))
  syn_request("POST", paste0("/file/v1/file/multipart/", upload_id, "/presigned/url/batch"),
              body = body, body_json = TRUE,
              token = token, verbose = verbose, dry_run = dry_run)
}

.syn_extract_part_urls <- function(res) {
  # Try common shapes:
  # - res$partPresignedUrls : list(list(partNumber=1, uploadPresignedUrl="..."), ...)
  # - res$presignedUrls     : ...
  # - res$parts             : ...
  p <- res$partPresignedUrls %||% res$presignedUrls %||% res$parts %||% res
  if (!is.list(p)) return(list())
  
  # If it's a named list keyed by part number -> url
  if (!is.null(names(p)) && all(nzchar(names(p)))) {
    out <- list()
    for (nm in names(p)) {
      out[[nm]] <- list(partNumber = as.integer(nm), url = as.character(p[[nm]]))
    }
    return(out)
  }
  
  # Else assume list of objects
  out <- list()
  for (it in p) {
    if (!is.list(it)) next
    pn <- it$partNumber %||% it$part_number %||% NULL
    u  <- it$uploadPresignedUrl %||% it$uploadPresignedURL %||% it$url %||% it$presignedUrl %||% NULL
    if (!is.null(pn) && !is.null(u)) out[[length(out) + 1L]] <- list(partNumber = as.integer(pn), url = as.character(u))
  }
  out
}

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
  
  # Validate parent container
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
  
  # Storage location lookup for the destination container
  locs <- syn_request("GET", paste0("/file/v1/entity/", parent_id, "/uploadDestinationLocations"),
                      token = token, verbose = verbose, dry_run = dry_run)
  storage_location_id <- .syn_pick_storage_location(locs)
  if (is.null(storage_location_id)) .syn_stop("Could not determine storageLocationId from uploadDestinationLocations response.")
  
  # Start multipart upload (forceRestart=true to avoid resuming stale uploads)
  upload_body <- list(
    concreteType = "org.sagebionetworks.repo.model.file.MultipartUploadRequest",
    fileName = name,
    contentType = contentType,
    fileSizeBytes = size_bytes,
    storageLocationId = as.integer(storage_location_id)
  )
  if (!is.null(md5_hex)) upload_body$contentMD5Hex <- md5_hex
  
  init <- syn_request("POST", "/file/v1/file/multipart",
                      query = list(forceRestart = "true"),
                      body = upload_body, body_json = TRUE,
                      token = token, verbose = verbose, dry_run = dry_run)
  
  upload_id <- as.character(init$uploadId %||% init$id %||% NA_character_)
  if (!nzchar(upload_id) || is.na(upload_id)) .syn_stop("Multipart initiation did not return uploadId.")
  
  part_size <- as.integer(init$partSizeBytes %||% 5L * 1024L * 1024L) # fallback 5MB
  n_parts <- max(1L, ceiling(size_bytes / part_size))
  
  .syn_verbose(verbose, sprintf("[multipart] uploadId=%s size=%d partSize=%d parts=%d",
                                upload_id, size_bytes, part_size, n_parts))
  
  # Upload parts
  con <- file(local_path, open = "rb")
  on.exit(try(close(con), silent = TRUE), add = TRUE)
  
  part_numbers <- seq_len(n_parts)
  
  pres <- .syn_multipart_presigned_urls(upload_id, part_numbers, token = token, verbose = verbose, dry_run = dry_run)
  part_url_objs <- .syn_extract_part_urls(pres)
  if (!length(part_url_objs)) .syn_stop("Did not find presigned part URLs in response.")
  
  # Index URLs by partNumber
  url_by_part <- setNames(
    vapply(part_url_objs, function(x) as.character(x$url), character(1)),
    vapply(part_url_objs, function(x) as.character(x$partNumber), character(1))
  )
  
  # We'll collect per-part etags if returned (optional)
  etags <- vector("list", n_parts)
  
  for (pn in part_numbers) {
    raw_bytes <- readBin(con, what = "raw", n = part_size)
    if (!length(raw_bytes) && pn <= n_parts) {
      .syn_stop("Unexpected EOF while reading part ", pn, "/", n_parts)
    }
    
    part_md5_hex <- .syn_md5_hex_raw(raw_bytes)
    
    part_url <- url_by_part[[as.character(pn)]] %||% NULL
    if (is.null(part_url)) .syn_stop("Missing presigned URL for part ", pn)
    
    if (isTRUE(dry_run)) {
      .syn_verbose(TRUE, sprintf("[dry_run] would PUT presigned URL for part %d (%d bytes)", pn, length(raw_bytes)))
    } else {
      resp <- .syn_put_presigned_part(part_url, raw_bytes, verbose = verbose)
      et <- tryCatch(httr2::resp_header(resp, "ETag"), error = function(e) NULL)
      etags[[pn]] <- et
    }
    
    # Tell Synapse the part was uploaded, passing MD5 hex
    syn_request("PUT", paste0("/file/v1/file/multipart/", upload_id, "/add/", pn),
                query = list(partMD5Hex = part_md5_hex),
                token = token, verbose = verbose, dry_run = dry_run)
    
    .syn_verbose(verbose, sprintf("[multipart] uploaded+added part %d/%d", pn, n_parts))
  }
  
  # Complete multipart upload
  done <- syn_request("PUT", paste0("/file/v1/file/multipart/", upload_id, "/complete"),
                      token = token, verbose = verbose, dry_run = dry_run)
  
  file_handle_id <- as.character(done$resultFileHandleId %||% done$fileHandleId %||% done$id %||% NA_character_)
  if (!nzchar(file_handle_id) || is.na(file_handle_id)) {
    .syn_stop("Multipart complete did not return a fileHandleId (resultFileHandleId/fileHandleId).")
  }
  
  # Create or update FileEntity under parent_id
  existing <- syn_get_child_by_name(parent_id, name, include_types = c("file"), token = token, verbose = verbose, dry_run = dry_run)
  existing_id <- existing$id %||% NULL
  
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
    
    # Update entity (PUT)
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
  
  # Apply annotations if provided (this is the missing piece you hit)
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

# -----------------------------
# Download
# -----------------------------
syn_download_file <- function(entity_id,
                              dest_path,
                              overwrite = FALSE,
                              token = NULL,
                              verbose = FALSE,
                              dry_run = FALSE) {
  stopifnot(.syn_is_id(entity_id))
  stopifnot(.syn_is_scalar_chr(dest_path))
  
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] would download %s -> %s", entity_id, dest_path))
    return(invisible(list(dry_run = TRUE, entity_id = entity_id, dest_path = dest_path)))
  }
  
  ent <- syn_get_entity(entity_id, token = token, verbose = verbose, dry_run = dry_run)
  if ((ent$concreteType %||% "") != "org.sagebionetworks.repo.model.FileEntity") {
    .syn_stop("entity_id is not a FileEntity: ", entity_id)
  }
  
  fh <- ent$dataFileHandleId %||% NULL
  if (is.null(fh) || !nzchar(fh)) .syn_stop("FileEntity has no dataFileHandleId: ", entity_id)
  
  # Signed URL for the file handle
  info <- syn_request("GET", paste0("/file/v1/fileHandle/", fh, "/url"),
                      token = token, verbose = verbose, dry_run = dry_run)
  
  url <- info$preSignedURL %||% info$presignedUrl %||% info$url %||% NULL
  if (is.null(url)) .syn_stop("File handle URL response did not include a URL for fileHandleId=", fh)
  
  # Destination checks
  dir.create(dirname(dest_path), recursive = TRUE, showWarnings = FALSE)
  if (file.exists(dest_path) && !isTRUE(overwrite)) {
    .syn_stop("dest_path exists and overwrite=FALSE: ", dest_path)
  }
  
  .syn_verbose(verbose, paste0("[syn_download_file] downloading to ", dest_path))
  
  req <- httr2::request(url) |>
    httr2::req_method("GET") |>
    httr2::req_options(http_version = 1.1, timeout = 600)
  
  httr2::req_perform(req, path = dest_path)
  dest_path
}

# -----------------------------
# Convenience wrappers using remote paths
# -----------------------------
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
  folder_id <- syn_ensure_folder_path(base_id, remote_folder_path,
                                      token = token, verbose = verbose, dry_run = dry_run)
  
  syn_upload_file(
    local_path   = local_path,
    parent_id    = folder_id,
    name         = name %||% basename(local_path),
    contentType  = contentType,
    overwrite    = overwrite,
    description  = description,
    annotations  = annotations,   # <-- FIXED: now supported by syn_upload_file()
    token        = token,
    verbose      = verbose,
    dry_run      = dry_run
  )
}

# Resolve an entity id from a "remote path" relative to base_id.
# - If path ends with '/', treat as folder
# - Otherwise last segment can be file or folder
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
    seg <- parts[[i]]
    last <- (i == length(parts))
    
    types <- if (last && !is_folder_hint) c("folder", "file") else c("folder")
    hit <- syn_get_child_by_name(cur, seg, include_types = types,
                                 token = token, verbose = verbose, dry_run = dry_run)
    
    child_id <- hit$id %||% NULL
    if (is.null(child_id) || !nzchar(child_id)) {
      .syn_stop("No entity named '", seg, "' under ", cur, " while resolving path: ", remote_path)
    }
    
    if (!last) {
      if (!isTRUE(grepl("folder", tolower(hit$type %||% "")))) {
        .syn_stop("Path segment '", seg, "' exists but is not a folder (", child_id, ")")
      }
    } else {
      if (is_folder_hint && !isTRUE(grepl("folder", tolower(hit$type %||% "")))) {
        .syn_stop("Path ends with '/', but last segment is not a folder: ", seg, " (", child_id, ")")
      }
    }
    
    cur <- child_id
  }
  
  cur
}

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





# 
# token <- Sys.getenv("SYNAPSE_PAT")
# 
# up <- syn_upload_file_path(
#   local_path = "C:/tmp/test.csv",
#   base_id = "syn64728425",
#   remote_folder_path = "zz_smoketests/synapse_rw/test2",
#   token = token,
#   overwrite = TRUE,
#   description = "Uploaded via path wrapper",
#   annotations = list(stage = "smoke", kind = "csv"),
#   verbose = TRUE
# )
# 



# --- helper: always encode vectors as JSON arrays even when length 1
.syn_json_array <- function(x) {
  if (is.null(x)) return(NULL)
  if (!is.atomic(x)) x <- unlist(x, recursive = TRUE, use.names = FALSE)
  unname(as.list(x))
}

# --- fix: extract httr2 error response reliably
.syn_extract_resp <- function(err) {
  if (inherits(err, "httr2_http")) {
    if (!is.null(err$resp))     return(err$resp)      # <-- important
    if (!is.null(err$response)) return(err$response)
    r <- attr(err, "response", exact = TRUE)
    if (!is.null(r)) return(r)
  }
  NULL
}

# --- fix: child-by-name must send includeTypes as JSON array
syn_get_child_by_name <- function(parent_id,
                                  name,
                                  include_types = c("folder", "file", "project"),
                                  token = NULL,
                                  verbose = FALSE,
                                  dry_run = FALSE) {
  stopifnot(.syn_is_id(parent_id), .syn_is_scalar_chr(name))
  
  body <- list(
    parentId = parent_id,
    name = name,
    includeTypes = .syn_json_array(include_types)  # <-- key fix
  )
  
  syn_request(
    "POST", "/repo/v1/entity/child",
    body = body, body_json = TRUE,
    token = token, verbose = verbose, dry_run = dry_run
  )
}

# --- fix: children listing also must send includeTypes as JSON array
syn_get_children <- function(parent_id,
                             include_types = c("folder", "file", "project"),
                             page_size = 100L,
                             token = NULL,
                             verbose = FALSE,
                             dry_run = FALSE) {
  stopifnot(.syn_is_id(parent_id))
  page_size <- as.integer(page_size)
  if (page_size < 1L || page_size > 500L) .syn_stop("page_size must be 1..500")
  
  out <- list()
  next_token <- NULL
  
  repeat {
    body <- list(
      parentId = parent_id,
      includeTypes = .syn_json_array(include_types),  # <-- key fix
      nextPageToken = next_token,
      pageSize = page_size
    )
    
    res <- syn_request(
      "POST", "/repo/v1/entity/children",
      body = body, body_json = TRUE,
      token = token, verbose = verbose, dry_run = dry_run
    )
    
    kids <- res$page %||% res$results %||% res$children %||% list()
    if (length(kids)) out[[length(out) + 1L]] <- kids
    
    next_token <- res$nextPageToken %||% NULL
    if (is.null(next_token) || !nzchar(next_token)) break
  }
  
  flat <- unlist(out, recursive = FALSE)
  if (!length(flat)) {
    return(data.frame(id = character(), name = character(), type = character(), stringsAsFactors = FALSE))
  }
  
  data.frame(
    id   = vapply(flat, function(x) as.character(x$id %||% NA_character_), character(1)),
    name = vapply(flat, function(x) as.character(x$name %||% NA_character_), character(1)),
    type = vapply(flat, function(x) as.character(x$type %||% NA_character_), character(1)),
    stringsAsFactors = FALSE
  )
}

# (Optional but recommended) multipart presigned batch: partNumbers should also be array
.syn_multipart_presigned_urls <- function(upload_id, part_numbers, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(upload_id))
  body <- list(
    uploadId = upload_id,
    partNumbers = .syn_json_array(as.integer(part_numbers))  # <-- array-safe
  )
  syn_request(
    "POST", paste0("/file/v1/file/multipart/", upload_id, "/presigned/url/batch"),
    body = body, body_json = TRUE,
    token = token, verbose = verbose, dry_run = dry_run
  )
}

