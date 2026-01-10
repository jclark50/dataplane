# ---- .syn_coalesce ----
.syn_coalesce <- function (a, b) 
{
  if (is.null(a) || length(a) == 0L) 
    return(b)
  if (length(a) == 1L) {
    if (is.na(a)) 
      return(b)
    if (is.character(a) && identical(a, "")) 
      return(b)
  }
  a
}


# ---- .syn_get_upload_destination_locations ----
.syn_get_upload_destination_locations <- function (container_id, token = NULL, verbose = FALSE, dry_run = FALSE) 
{
  stopifnot(.syn_is_scalar_chr(container_id))
  out <- syn_request("GET", paste0("/file/v1/entity/", container_id, "/uploadDestinationLocations"), token = token, verbose = verbose, dry_run = dry_run)
  locs <- out$list
  if (is.null(locs) || !length(locs)) {
    .syn_stop("No upload destination locations returned for container ", container_id)
  }
  locs
}


# ---- .syn_guess_content_type ----
.syn_guess_content_type <- function (path) 
{
  ext <- tolower(tools::file_ext(path))
  if (ext %in% c("csv")) 
    return("text/csv")
  if (ext %in% c("tsv")) 
    return("text/tab-separated-values")
  if (ext %in% c("parquet")) 
    return("application/octet-stream")
  if (ext %in% c("json")) 
    return("application/json")
  "application/octet-stream"
}


# ---- .syn_is_id ----
.syn_is_id <- function (x) 
  is.character(x) && length(x) == 1L && grepl("^syn\\d+$", x)


# ---- .syn_is_scalar_chr ----
.syn_is_scalar_chr <- function (x) 
  is.character(x) && length(x) == 1L && !is.na(x)


# ---- .syn_multipart_upload ----
.syn_multipart_upload <- function (local_path, storageLocationId, fileName, contentType, partSizeBytes = NULL, token = NULL, verbose = FALSE, dry_run = FALSE) 
{
  stopifnot(.syn_is_scalar_chr(local_path), file.exists(local_path))
  info <- file.info(local_path)
  if (is.na(info$size) || info$size <= 0) 
    .syn_stop("local_path is empty or unreadable: ", local_path)
  fileSizeBytes <- as.numeric(info$size)
  contentMD5Hex <- .syn_md5_file_hex(local_path)
  if (is.null(partSizeBytes)) {
    partSizeBytes <- max(5 * 1024^2, ceiling(fileSizeBytes/5000))
  }
  partSizeBytes <- as.numeric(partSizeBytes)
  body <- list(concreteType = "org.sagebionetworks.repo.model.file.MultipartUploadRequest", fileName = fileName, contentType = contentType, fileSizeBytes = fileSizeBytes, contentMD5Hex = contentMD5Hex, storageLocationId = as.numeric(storageLocationId), partSizeBytes = partSizeBytes)
  status <- syn_request("POST", "/file/v1/file/multipart", body = body, body_json = TRUE, token = token, verbose = verbose, dry_run = dry_run)
  uploadId <- status$uploadId
  if (!.syn_is_scalar_chr(uploadId)) {
    .syn_stop("Multipart start did not return uploadId. Payload keys: ", paste(names(status), collapse = ", "))
  }
  n_parts <- as.integer(ceiling(fileSizeBytes/partSizeBytes))
  if (n_parts < 1L) 
    n_parts <- 1L
  .syn_verbose(verbose, sprintf("[multipart] uploadId=%s size=%s partSize=%s parts=%s", uploadId, format(fileSizeBytes, scientific = FALSE), format(partSizeBytes, scientific = FALSE), n_parts))
  con <- file(local_path, open = "rb")
  on.exit(try(close(con), silent = TRUE), add = TRUE)
  get_presigned_batch <- function(part_numbers) {
    req_body <- list(uploadId = uploadId, partNumbers = as.list(as.integer(part_numbers)))
    syn_request("POST", paste0("/file/v1/file/multipart/", uploadId, "/presigned/url/batch"), body = req_body, body_json = TRUE, token = token, verbose = verbose, dry_run = dry_run)
  }
  batch_size <- 50L
  part_num <- 1L
  while (part_num <= n_parts) {
    batch_parts <- part_num:min(n_parts, part_num + batch_size - 1L)
    presigned <- get_presigned_batch(batch_parts)
    urls <- presigned$partPresignedUrls
    if (is.null(urls) || !length(urls)) {
      .syn_stop("No presigned URLs returned for parts: ", paste(batch_parts, collapse = ","))
    }
    url_map <- list()
    for (u in urls) {
      pn <- u$partNumber
      if (!is.null(pn)) 
        url_map[[as.character(pn)]] <- u
    }
    for (pn in batch_parts) {
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
      if (isTRUE(dry_run)) {
        .syn_verbose(TRUE, sprintf("[dry_run] PUT presigned (part %s) bytes=%s", pn, this_size))
      }
      else {
        .syn_put_presigned(url = u$uploadPresignedUrl, raw_bytes = raw_bytes, signed_headers = u$signedHeaders %||% NULL, verbose = verbose)
      }
      syn_request("PUT", paste0("/file/v1/file/multipart/", uploadId, "/add/", pn), query = list(partMD5Hex = part_md5_hex), token = token, verbose = verbose, dry_run = dry_run)
      .syn_verbose(verbose, sprintf("[multipart] uploaded+added part %s/%s", pn, n_parts))
    }
    part_num <- max(batch_parts) + 1L
  }
  done <- syn_request("PUT", paste0("/file/v1/file/multipart/", uploadId, "/complete"), token = token, verbose = verbose, dry_run = dry_run)
  fh <- done$resultFileHandleId
  if (!.syn_is_scalar_chr(fh)) {
    .syn_stop("Multipart complete did not return resultFileHandleId. Payload keys: ", paste(names(done), collapse = ", "))
  }
  list(uploadId = uploadId, resultFileHandleId = fh, fileSizeBytes = fileSizeBytes, partSizeBytes = partSizeBytes, contentMD5Hex = contentMD5Hex)
}


# ---- .syn_norm_path ----
.syn_norm_path <- function (path) 
{
  path <- gsub("\\\\", "/", path)
  path <- gsub("/{2,}", "/", path)
  path <- gsub("^/+", "", path)
  path <- gsub("/+$", "", path)
  path
}


# ---- syn_create_file_entity ----
syn_create_file_entity <- function (parent_id, name, dataFileHandleId, description = NULL, token = NULL, verbose = FALSE, dry_run = FALSE) 
{
  body <- list(concreteType = "org.sagebionetworks.repo.model.FileEntity", parentId = parent_id, name = name, dataFileHandleId = dataFileHandleId)
  if (!is.null(description)) 
    body$description <- description
  syn_request("POST", "/repo/v1/entity", body = body, body_json = TRUE, token = token, verbose = verbose, dry_run = dry_run)
}


# ---- syn_create_folder ----
syn_create_folder <- function (parent_id, name, description = NULL, token = NULL, verbose = FALSE, dry_run = FALSE) 
{
  stopifnot(.syn_is_scalar_chr(parent_id), .syn_is_scalar_chr(name))
  .syn_validate_segment(name)
  body <- list(concreteType = "org.sagebionetworks.repo.model.Folder", parentId = parent_id, name = name)
  if (!is.null(description)) 
    body$description <- description
  syn_request("POST", "/repo/v1/entity", body = body, body_json = TRUE, token = token, verbose = verbose, dry_run = dry_run)
}


# ---- syn_get_entity ----
syn_get_entity <- function (entity_id, token = NULL, verbose = FALSE, dry_run = FALSE) 
{
  stopifnot(.syn_is_scalar_chr(entity_id))
  syn_request("GET", paste0("/repo/v1/entity/", entity_id), token = token, verbose = verbose, dry_run = dry_run)
}


# ---- syn_lookup_child_id ----
syn_lookup_child_id <- function (parent_id, entity_name, token = NULL, verbose = FALSE, dry_run = FALSE) 
{
  tryCatch({
    res <- syn_get_child_by_name(parent_id, entity_name, token = token, verbose = verbose, dry_run = dry_run)
    res$id %||% NULL
  }, error = function(e) {
    msg <- conditionMessage(e)
    if (grepl("Synapse HTTP error 404", msg, fixed = TRUE)) 
      return(NULL)
    stop(e)
  })
}


# ---- syn_request ----
syn_request <- function (method, path, query = NULL, body = NULL, body_json = TRUE, headers = list(), token = NULL, retry = TRUE, verbose = FALSE, dry_run = FALSE) 
{
  method <- toupper(method)
  stopifnot(.syn_is_scalar_chr(method), .syn_is_scalar_chr(path))
  url <- paste0(.syn_base_url(), path)
  if (isTRUE(dry_run)) {
    .syn_verbose(TRUE, sprintf("[dry_run] %s %s", method, path))
    if (!is.null(query)) 
      .syn_verbose(TRUE, paste0("  query: ", jsonlite::toJSON(query, auto_unbox = TRUE)))
    if (!is.null(body)) 
      .syn_verbose(TRUE, paste0("  body:  ", jsonlite::toJSON(body, auto_unbox = TRUE)))
    return(invisible(list(dry_run = TRUE, method = method, path = path, url = url)))
  }
  tok <- syn_resolve_token(token)
  req <- httr2::req_options(httr2::req_headers(httr2::req_method(httr2::request(url), method), Authorization = paste("Bearer", tok), Accept = "application/json", `User-Agent` = "synapse-rest-r/0.1"), timeout = 120, connecttimeout = 30)
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
    }
    else {
      req <- httr2::req_body_raw(req, body)
    }
  }
  max_tries <- if (isTRUE(retry)) 
    7L
  else 1L
  for (attempt in seq_len(max_tries)) {
    .syn_verbose(verbose, sprintf("[syn_request] %s %s", method, path))
    out <- tryCatch(httr2::req_perform(req), error = function(e) e)
    if (inherits(out, "curl_error") || inherits(out, "httr2_failure")) {
      if (attempt >= max_tries) {
        .syn_stop("HTTP request failed (network error) for ", method, " ", path, ": ", conditionMessage(out))
      }
      wait <- min(60, (2^(attempt - 1)) + stats::runif(1, 0, 0.5))
      .syn_verbose(verbose, sprintf("[syn_request] %s %s -> network error; retrying in %.2fs", method, path, wait))
      Sys.sleep(wait)
      next
    }
    if (inherits(out, "httr2_http")) {
      resp <- .syn_extract_resp(out)
      code <- if (!is.null(resp)) 
        httr2::resp_status(resp)
      else NA_integer_
      reason <- .syn_parse_error_reason(resp)
      if (!is.na(code) && (code == 429L || code >= 500L) && attempt < max_tries) {
        wait <- if (!is.null(resp)) 
          .syn_retry_wait(resp, attempt)
        else min(60, 2^(attempt - 1))
        .syn_verbose(verbose, sprintf("[syn_request] %s %s -> HTTP %s; retrying in %.2fs", method, path, code, wait))
        Sys.sleep(wait)
        next
      }
      .syn_stop("HTTP ", code, " for ", method, " ", path, if (!is.null(reason)) 
        paste0("\nSynapse reason: ", reason)
        else "")
    }
    resp <- out
    code <- httr2::resp_status(resp)
    ct <- tryCatch(resp_header(resp, "Content-Type"), error = function(e) "")
    if (grepl("application/json", ct, fixed = TRUE)) {
      return(resp_body_json(resp, simplifyVector = FALSE))
    }
    return(resp)
  }
  .syn_stop("Unreachable: syn_request loop ended unexpectedly for ", method, " ", path)
}


# ---- syn_update_file_entity ----
syn_update_file_entity <- function (entity_id, new_dataFileHandleId, token = NULL, verbose = FALSE, dry_run = FALSE) 
{
  ent <- syn_get_entity(entity_id, token = token, verbose = verbose, dry_run = dry_run)
  ctype <- ent$concreteType %||% ""
  if (!grepl("FileEntity$", ctype)) 
    .syn_stop("Entity is not a FileEntity: ", entity_id, " type=", ctype)
  ent$dataFileHandleId <- new_dataFileHandleId
  syn_request("PUT", paste0("/repo/v1/entity/", entity_id), body = ent, body_json = TRUE, token = token, verbose = verbose, dry_run = dry_run)
}


# ---- .syn_verbose ----
.syn_verbose <- function(verbose, ...) if (isTRUE(verbose)) message(...)

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

.syn_base_url <- function() "https://repo-prod.prod.sagebase.org"


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

.syn_verbose <- function(verbose, ...) if (isTRUE(verbose)) message(...)

.syn_stop <- function(...) stop(paste0(...), call. = FALSE)


syn_get_entity <- function(entity_id, token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(entity_id))
  syn_request("GET", paste0("/repo/v1/entity/", entity_id),
              token = token, verbose = verbose, dry_run = dry_run)
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



.syn_validate_segment <- function(seg) {
  if (!nzchar(seg)) .syn_stop("Invalid folder path segment: empty")
  # any ASCII control char (includes NUL) => reject
  if (grepl("[[:cntrl:]]", seg)) .syn_stop("Invalid folder path segment: control characters not allowed")
  if (grepl("/", seg, fixed = TRUE)) .syn_stop("Invalid folder path segment: must not contain '/'")
  invisible(TRUE)
}

# syn_get_child_by_name <- function(parent_id, name, token = NULL, verbose = FALSE, dry_run = FALSE) {
#   stopifnot(.syn_is_scalar_chr(parent_id), .syn_is_scalar_chr(name))
#   body <- list(parentId = parent_id, entityName = name)
#   syn_request("POST", "/repo/v1/entity/child",
#               body = body, body_json = TRUE,
#               token = token, verbose = verbose, dry_run = dry_run)
# }


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
                            dry_run = FALSE
                            
) {
  
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
  # if (is.null(storage_location_id)) .syn_stop("Could not determine storageLocationId from uploadDestinationLocations response.")
  if (is.null(storage_location_id)) {
    keys <- paste(names(locs), collapse = ", ")
    .syn_stop(
      "Could not determine storageLocationId from uploadDestinationLocations response.\n",
      "Top-level keys: ", keys, "\n",
      "Raw response (first 1000 chars): ",
      substr(jsonlite::toJSON(locs, auto_unbox = TRUE, null = "null"), 1, 1000)
    )
  }
  
  # Start multipart upload (forceRestart=true to avoid resuming stale uploads)
  
  # Synapse requires partSizeBytes in the request.
  # Must be >= 5 MiB. Pick something reasonable to avoid huge part counts.
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
  
  
  
  
  # Upload parts
  con <- file(local_path, open = "rb")
  on.exit(try(close(con), silent = TRUE), add = TRUE)
  
  part_numbers <- seq_len(n_parts)
  
  pres <- .syn_multipart_presigned_urls(upload_id, part_numbers, token = token, verbose = verbose, dry_run = dry_run)
  
  maps <- .syn_build_presigned_maps(pres)
  url_by_part <- maps$url_by_part
  hdr_by_part <- maps$hdr_by_part
  
  etags <- vector("list", n_parts)
  
  # Optional sanity check (helps immediately if this breaks again)
  if (isTRUE(verbose)) {
    message("[multipart] presigned map parts: ", paste(names(url_by_part), collapse = ", "))
  }
  
  for (pn in part_numbers) {
    raw_bytes <- readBin(con, what = "raw", n = part_size)
    if (!length(raw_bytes)) .syn_stop("Unexpected EOF while reading part ", pn, "/", n_parts)
    
    part_md5_hex <- .syn_md5_hex_raw(raw_bytes)
    
    key <- as.character(pn)
    
    # Use [ ] not [[ ]] so a missing key yields NA (then we error cleanly)
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
  
  # # Create or update FileEntity under parent_id
  # existing <- syn_get_child_by_name(parent_id, name, 
  #                                   # include_types = c("file"),
  #                                   token = token, verbose = verbose, dry_run = dry_run)
  # existing_id <- existing$id %||% NULL
  
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

.syn_as_list_of_objects <- function(x) {
  if (is.null(x)) return(list())
  
  # If Synapse payload got simplified to a data.frame, convert rows -> list-of-lists
  if (is.data.frame(x)) {
    if (nrow(x) == 0) return(list())
    return(lapply(seq_len(nrow(x)), function(i) as.list(x[i, , drop = FALSE])))
  }
  
  # If it's already a list of lists, keep it
  if (is.list(x) && length(x) && is.list(x[[1]])) return(x)
  
  # If it's a single object list, wrap it
  if (is.list(x)) return(list(x))
  
  list()
}


.syn_get_upload_destination_locations <- function(container_id, token = NULL, verbose = FALSE, dry_run = FALSE) {
  out <- syn_request("GET", paste0("/file/v1/entity/", container_id, "/uploadDestinationLocations"),
                     token = token, verbose = verbose, dry_run = dry_run)
  
  locs <- out$list %||% out$locations %||% out$uploadDestinationLocations %||% out
  locs <- .syn_as_list_of_objects(locs)
  
  if (!length(locs)) .syn_stop("No upload destination locations returned for container ", container_id)
  
  # Prefer isDefault if present
  pick <- NULL
  for (li in locs) {
    if (isTRUE(li$isDefault)) { pick <- li; break }
  }
  if (is.null(pick)) pick <- locs[[1]]
  
  storageLocationId <- pick$storageLocationId %||% pick$id %||% NULL
  if (is.null(storageLocationId)) {
    .syn_stop("uploadDestinationLocations payload did not include storageLocationId.")
  }
  
  list(storageLocationId = storageLocationId, raw = locs)
}



.syn_md5_hex_file <- function(path) digest::digest(file = path, algo = "md5", serialize = FALSE)
.syn_md5_hex_raw  <- function(raw)  digest::digest(raw,  algo = "md5", serialize = FALSE)



.syn_pick_storage_location <- function(x) {
  # Synapse commonly returns: { "list": [ {storageLocationId: ...}, ... ] }
  # but other wrappers might produce "locations" or similar.
  locs <- x$list %||% x$locations %||% x$uploadDestinationLocations %||% x$results %||% x
  
  if (is.null(locs)) return(NULL)
  
  # If locs itself is a single location object, normalize to list-of-locations
  if (is.list(locs) && !is.null(locs$storageLocationId)) locs <- list(locs)
  
  if (!is.list(locs) || !length(locs)) return(NULL)
  
  # Prefer an explicitly-default location if present
  pick <- NULL
  for (i in seq_along(locs)) {
    li <- locs[[i]]
    if (is.list(li) && isTRUE(li$isDefault)) {
      pick <- li
      break
    }
  }
  if (is.null(pick)) pick <- locs[[1]]
  
  # Pull the ID from common fields
  id <- pick$storageLocationId %||% pick$id %||% pick$storage_location_id %||% NULL
  
  # Rare nested form (be defensive)
  if (is.null(id) && is.list(pick$storageLocation)) {
    id <- pick$storageLocation$id %||% pick$storageLocation$storageLocationId %||% NULL
  }
  
  id
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


`%||%` <- function(a, b) if (is.null(a)) b else a

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
    # critical: do NOT follow the 307 redirect
    httr2::req_options(followlocation = 0, maxredirs = 0, timeout = 120, connecttimeout = 30)
  
  resp <- httr2::req_perform(req)
  st   <- httr2::resp_status(resp)
  
  # Expected behavior per Synapse docs: 307 with Location header
  if (st %in% c(301L, 302L, 303L, 307L, 308L)) {
    loc <- httr2::resp_header(resp, "Location") %||% httr2::resp_header(resp, "location")
    if (!is.character(loc) || length(loc) != 1L || !nzchar(loc)) {
      .syn_stop("Synapse returned redirect (HTTP ", st, ") but no Location header was present.")
    }
    .syn_verbose(verbose, paste0("[fileHandle/url] redirect -> Location (", nchar(loc), " chars)"))
    return(loc)
  }
  
  # Some deployments/paths can return JSON instead of redirect; handle that too.
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
  
  # 1) Get a *presigned* URL (capture redirect Location; don't auto-follow)
  url <- .syn_filehandle_download_url(as.character(fh), token = token, verbose = verbose)
  
  # 2) Download the file content from that URL to disk
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



# .syn_multipart_presigned_urls <- function(upload_id, part_numbers, token = NULL, verbose = FALSE, dry_run = FALSE) {
# stopifnot(.syn_is_scalar_chr(upload_id))
# body <- list(uploadId = upload_id, partNumbers = as.integer(part_numbers))
# syn_request("POST", paste0("/file/v1/file/multipart/", upload_id, "/presigned/url/batch"),
# body = body, body_json = TRUE,
# token = token, verbose = verbose, dry_run = dry_run)
# }


# # (Optional but recommended) multipart presigned batch: partNumbers should also be array
# .syn_multipart_presigned_urls <- function(upload_id, part_numbers, token = NULL, verbose = FALSE, dry_run = FALSE) {
# stopifnot(.syn_is_scalar_chr(upload_id))
# body <- list(
# uploadId = upload_id,
# partNumbers = .syn_json_array(as.integer(part_numbers))  # <-- array-safe
# )
# syn_request(
# "POST", paste0("/file/v1/file/multipart/", upload_id, "/presigned/url/batch"),
# body = body, body_json = TRUE,
# token = token, verbose = verbose, dry_run = dry_run
# )
# }




.syn_json_array <- function(x) {
  if (is.null(x)) return(NULL)
  if (!is.atomic(x)) x <- unlist(x, recursive = TRUE, use.names = FALSE)
  unname(as.list(x))
}



`%||%` <- function(a, b) if (is.null(a) || length(a) == 0L) b else a

.syn_as_list_of_rows <- function(x) {
  if (is.null(x)) return(list())
  
  # If httr2/jsonlite simplified it to a data.frame, convert rows -> list
  if (is.data.frame(x)) {
    if (nrow(x) == 0) return(list())
    return(lapply(seq_len(nrow(x)), function(i) {
      # drop=TRUE gives a named list with list-columns preserved
      as.list(x[i, , drop = TRUE])
    }))
  }
  
  # Already list-of-objects
  if (is.list(x) && length(x) && is.list(x[[1]])) return(x)
  
  # Single object -> wrap
  if (is.list(x)) return(list(x))
  
  list()
}

.syn_multipart_presigned_urls <- function(upload_id, part_numbers,
                                          token = NULL, verbose = FALSE, dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(upload_id))
  # IMPORTANT: list() to force JSON array even for 1 part
  body <- list(
    uploadId    = upload_id,
    partNumbers = as.list(as.integer(part_numbers))
  )
  syn_request(
    "POST",
    paste0("/file/v1/file/multipart/", upload_id, "/presigned/url/batch"),
    body = body, body_json = TRUE,
    token = token, verbose = verbose, dry_run = dry_run
  )
}

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
    # Helpful debug dump
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

.syn_put_presigned_part <- function(url, raw_bytes, signed_headers = list(), verbose = FALSE) {
  req <- httr2::request(url) |>
    httr2::req_method("PUT") |>
    httr2::req_body_raw(raw_bytes) |>
    httr2::req_options(timeout = 600, http_version = 1.1)
  
  if (length(signed_headers)) {
    req <- httr2::req_headers(req, !!!signed_headers)
  }
  
  resp <- httr2::req_perform(req)
  st <- httr2::resp_status(resp)
  if (st < 200L || st >= 300L) .syn_stop("Presigned PUT failed (HTTP ", st, ")")
  resp
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





#' @keywords internal
.syn_split_path <- function(path) {
  path <- .syn_norm_path(path)
  if (!nzchar(path)) return(character())
  parts <- strsplit(path, "/", fixed = TRUE)[[1]]
  parts[nzchar(parts)]
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


syn_get_child_by_name <- function(parent_id,
                                  name,
                                  include_types = NULL,
                                  token = NULL,
                                  verbose = FALSE,
                                  dry_run = FALSE) {
  stopifnot(.syn_is_scalar_chr(parent_id), .syn_is_scalar_chr(name))
  
  body <- list(parentId = parent_id, entityName = name)
  
  # Optional type filter (Synapse supports this in the request body)
  if (!is.null(include_types)) {
    stopifnot(is.character(include_types), length(include_types) >= 1L)
    include_types <- tolower(include_types)
    include_types <- include_types[nzchar(include_types)]
    body$includeTypes <- as.list(include_types)
  }
  
  syn_request(
    "POST", "/repo/v1/entity/child",
    body = body, body_json = TRUE,
    token = token, verbose = verbose, dry_run = dry_run
  )
}
# 

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
    
    # Filtering is optional; keep it, but DO NOT trust the returned payload for typing
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
    
    # Enforce folder-ness by querying the entity itself (reliable)
    must_be_folder <- (!last) || is_folder_hint
    if (isTRUE(must_be_folder)) {
      ent <- syn_get_entity(child_id, token = token, verbose = verbose, dry_run = dry_run)
      ctype <- ent$concreteType %||% ""
      if (!grepl("Folder$", ctype)) {
        .syn_stop(
          "Path segment '", seg, "' exists but is not a Folder: ", child_id,
          " concreteType=", ctype
        )
      }
    }
    
    cur <- child_id
  }
  
  cur
}
# 
# # 
# # 
# 
# token <- c("eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsidmlldyIsImRvd25sb2FkIiwibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTczODM2Mjg5NSwiaWF0IjoxNzM4MzYyODk1LCJqdGkiOiIxNjAzMiIsInN1YiI6IjM1Mjk3NTkifQ.K9KPoxkBxMC769CozAL2Zda99mxtD_Y4eSpGrsaULtcF2ZCOy6Yk60RVXY-bYqVVyLe5Olz6NA9R6qeAFdR-ETypqVnP4tWqCgCfJr0UACUsuRllGgM1YMQ9N292wlYQuK_T38JTDX0M38hMCciM4D6y7HinF8Pu6VPv4R8-6zirWsJDWWbu3GzR4CsgsLN-meM0Oimf7-f_xngb9XLANUtKbo2eUJDt3W7vLmJKyU1pjeQLl7bU-yuqMIMKpFwStUwsAwU_Vh4YWjRCvIy5vzDJc0L2OmVARViCz0zUDBQ1KjIumtWLx8sj6QWYGoSFU7dr7wacLjga9MXv_n-sNg")
# 
# 
# 
# #
# dl <- syn_download_file_path(
#   base_id = "syn64728425",
#   remote_file_path = "zz_smoketests/synapse_rw/test2/New Text Document.txt",
#   dest_path = "C:/tmp/downloads/test.txt",
#   overwrite = TRUE,
#   token = token,
#   verbose = TRUE
# )
# #
# 
# up <- syn_upload_file_path(
#   local_path = "C:/Users/jordan/Desktop/New Text Document.txt",
#   base_id = "syn64728425",
#   remote_folder_path = "zz_smoketests/synapse_rw/test2",
#   token = token,
#   overwrite = TRUE,
#   description = "Uploaded via path wrapper",
#   annotations = list(stage = "smoke", kind = "csv"),
#   verbose = TRUE
# )
# 
# 
# 
# 
# 
# 
# 
# # source("synapse_rest_backend.R")
# 
# # token <- Sys.getenv("SYNAPSE_PAT")
# project_id <- "syn64728425"
# 
# folder_id <- syn_ensure_folder_path(
#   parent_id = project_id,
#   path = "zz_smoketests/synapse_rw",
#   token = token,
#   verbose = TRUE
# )
# 
# 
# # folder_id <- 'syn72246746'
# # syn72246747
# 
# tmp <- tempfile(fileext = ".csv")
# write.csv(data.frame(x=1:5, y=letters[1:5]), tmp, row.names = FALSE)
# 
# up <- syn_upload_file(
#   local_path =tmp,
#   parent_id = folder_id,
#   token = token,
#   verbose = TRUE,
#   overwrite = TRUE
#   # ,
#   # description = "Test upload (pure REST multipart)"
# )
# 
# up
# 
# # syn_upload_file1
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
# # 
# # 
# # 
# 
