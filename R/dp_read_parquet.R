# =============================================================================
# jj / Klimo Parquet I/O — READING (metadata-aware)
# =============================================================================
# Exports:
#   - dp_read_parquet_meta()
#   - dp_read_parquet() 
#   - dp_decode_scaled()
#   - dp_attach_units()
#   - dp_open_dataset_with_meta()
#
# Notes:
#   - All tabular returns are data.table.
#   - Schema metadata is stored as strings (JSON) in key_value_metadata.
# =============================================================================

.klimo_require <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(sprintf("Required package '%s' is not installed.", pkg), call. = FALSE)
  }
}

.klimo_dt <- function(x) {
  .klimo_require("data.table")
  
  if (inherits(x, "data.table")) return(x)
  
  # If it's a data.frame/tibble, Arrow may have attached attributes (including "sorted")
  if (inherits(x, "data.frame")) {
    x <- base::as.data.frame(x)  # drop tibble subclasses
    
    # Keep only the minimal attributes a data.frame needs
    keep <- c("names", "row.names", "class")
    at <- attributes(x)
    attributes(x) <- at[intersect(names(at), keep)]
    
    # belt + suspenders
    attr(x, "sorted") <- NULL
    
    return(data.table::as.data.table(x))
  }
  
  # For non-data.frame inputs, strip any "sorted" attribute anyway
  attr(x, "sorted") <- NULL
  data.table::as.data.table(x)
}

.klimo_stop <- function(...) stop(sprintf(...), call. = FALSE)
.klimo_warn <- function(...) warning(sprintf(...), call. = FALSE, immediate. = TRUE)
.klimo_msg  <- function(...) message(sprintf(...))

.klimo_from_json <- function(x) {
  .klimo_require("jsonlite")
  jsonlite::fromJSON(x, simplifyVector = TRUE)
}

# ---- internal: robust schema-metadata extraction -----------------------------

.klimo_arrow_table <- function(path) {
  .klimo_require("arrow")
  arrow::read_parquet(path, as_data_frame = FALSE)  # returns Arrow Table
}

.klimo_kv_to_dt <- function(kv) {
  .klimo_require("data.table")
  
  if (is.null(kv) || !length(kv)) {
    out <- data.table::data.table(k = character(), value = character())
    data.table::setnames(out, "k", "key")
    data.table::setkey(out, key)
    return(out)
  }
  
  out <- data.table::as.data.table(list(
    key = names(kv),
    value = vapply(kv, function(v) {
      if (is.null(v)) return(NA_character_)
      if (is.raw(v))  return(rawToChar(v))
      as.character(v)[1]
    }, character(1))
  ))
  
  data.table::setkey(out, key)
  out
}




.klimo_schema_metadata <- function(tab) {
  md <- tryCatch(tab$schema$metadata, error = function(e) NULL)
  if (is.null(md)) return(list())
  
  # Case A: Arrow returns key/value table
  if (inherits(md, "data.frame") && all(c("key", "value") %in% names(md))) {
    keys <- as.character(md$key)
    
    vals_chr <- vapply(md$value, function(v) {
      if (is.null(v)) return(NA_character_)
      if (is.raw(v))  return(rawToChar(v))
      as.character(v)[1]
    }, character(1))
    
    out <- as.list(vals_chr)
    names(out) <- keys
    return(out)
  }
  
  # Case B: named atomic vector
  if (is.atomic(md) && !is.null(names(md))) {
    out <- as.list(md)
    names(out) <- names(md)
    out <- lapply(out, function(v) {
      if (is.raw(v)) rawToChar(v) else as.character(v)[1]
    })
    return(out)
  }
  
  # Case C: named list
  if (is.list(md) && !is.null(names(md))) {
    out <- lapply(md, function(v) {
      if (is.raw(v)) rawToChar(v) else as.character(v)[1]
    })
    return(out)
  }
  
  list()
}




.klimo_parse_spec_dt <- function(kv) {
  js <- kv[["klimo:field_spec_json"]]
  if (is.null(js) || !nzchar(js)) return(NULL)
  obj <- .klimo_from_json(js)
  .klimo_dt(obj)
}



.klimo_parse_audit_dt <- function(kv) {
  js <- kv[["klimo:audit_json"]]
  if (is.null(js) || !nzchar(js)) return(NULL)
  obj <- .klimo_from_json(js)
  .klimo_dt(obj)
}



# =============================================================================
# Read metadata helpers (to make inspection easy)
# =============================================================================

#' Read Klimo Parquet schema metadata (spec + audit)
#' @export
dp_read_parquet_meta <- function(path, parse = TRUE) {
  .klimo_require("arrow")
  .klimo_require("data.table")
  .klimo_require("jsonlite")
  
  pf <- arrow::ParquetFileReader$create(path)
  md <- pf$metadata
  sch <- md$schema
  
  kv <- sch$metadata
  if (is.null(kv)) kv <- list()
  
  # kv can be a named list; convert to kv_dt for convenience
  kv_dt <- data.table::data.table(
    key = names(kv),
    value = unname(unlist(kv, use.names = FALSE))
  )
  data.table::setkey(kv_dt, key)
  
  out <- list(kv = kv, kv_dt = kv_dt)
  
  if (isTRUE(parse)) {
    spec_json  <- kv[["klimo:field_spec_json"]]
    audit_json <- kv[["klimo:audit_json"]]
    
    spec_dt <- NULL
    audit_dt <- NULL
    
    if (!is.null(spec_json) && nzchar(spec_json)) {
      spec_dt <- data.table::as.data.table(jsonlite::fromJSON(spec_json, simplifyDataFrame = TRUE))
    }
    if (!is.null(audit_json) && nzchar(audit_json)) {
      audit_dt <- data.table::as.data.table(jsonlite::fromJSON(audit_json, simplifyDataFrame = TRUE))
    }
    
    out$spec_dt  <- spec_dt
    out$audit_dt <- audit_dt
  }
  
  out
}

# =============================================================================
#' Attach declared units to columns (sets attr(x[[col]], "unit"))
#'
#' This does NOT convert values. It is simply a “tagging” step based on metadata.
#'
#' @param dt A data.table.
#' @param audit_dt Parsed audit table from metadata (preferred), or spec_dt.
#' @param prefer Character: "declared_units" (audit) or "metric_units"/"imperial_units" (spec).
#' @param system If using spec_dt units columns, which system to use.
#' @param warn_unmatched Warn if declared units exist for columns not present.
#'
#' @return data.table (modified copy)
#' @export
dp_attach_units <- function(
    dt,
    audit_dt = NULL,
    spec_dt = NULL,
    prefer = c("declared_units", "spec"),
    system = c("metric", "imperial"),
    warn_unmatched = FALSE
) {
  .klimo_require("data.table")
  dt <- data.table::copy(.klimo_dt(dt))
  prefer <- match.arg(prefer)
  system <- match.arg(system)
  
  if (prefer == "declared_units") {
    if (is.null(audit_dt) || !nrow(audit_dt)) return(dt)
    stopifnot(all(c("column", "declared_units") %in% names(audit_dt)))
    map <- audit_dt[, .(column = as.character(column), units = as.character(declared_units))]
  } else {
    if (is.null(spec_dt) || !nrow(spec_dt)) return(dt)
    stopifnot("column" %in% names(spec_dt))
    ucol <- if (system == "metric") "metric_units" else "imperial_units"
    if (!(ucol %in% names(spec_dt))) return(dt)
    map <- spec_dt[, .(column = as.character(column), units = as.character(get(ucol)))]
  }
  
  map <- map[!is.na(units) & nzchar(units)]
  if (!nrow(map)) return(dt)
  
  if (isTRUE(warn_unmatched)) {
    miss <- setdiff(map$column, names(dt))
    if (length(miss)) .klimo_warn("dp_attach_units(): %d unit entries not found in dt: %s",
                                  length(miss), paste(head(miss, 12L), collapse = ", "))
  }
  
  for (i in seq_len(nrow(map))) {
    col <- map$column[i]
    if (!col %in% names(dt)) next
    attr(dt[[col]], "unit") <- map$units[i]
  }
  
  dt
}

# =============================================================================
#' Decode scaled integer columns back to floats (using audit metadata)
#'
#' If your writer created columns like ta_i with scale=100, this recreates ta = ta_i / 100.
#' Optionally drops the scaled integer columns afterward.
#'
#' @param dt A data.table.
#' @param audit_dt Parsed audit table from metadata.
#' @param prefer_scaled_status If TRUE, only decode rows where audit says did_scale==TRUE
#'   or scale_status=="scaled". If FALSE, decode whenever scaled_column + suggested_scale exist.
#' @param drop_scaled_cols Drop the *_i columns after decoding.
#' @param overwrite If TRUE, overwrite existing base columns if present.
#'
#' @return data.table (modified copy)
#' @export
dp_decode_scaled <- function(
    dt,
    audit_dt,
    prefer_scaled_status = TRUE,
    drop_scaled_cols = FALSE,
    overwrite = FALSE
) {
  .klimo_require("data.table")
  dt <- data.table::copy(.klimo_dt(dt))
  if (is.null(audit_dt) || !nrow(audit_dt)) return(dt)
  
  need <- c("column", "scaled_column", "suggested_scale")
  if (!all(need %in% names(audit_dt))) return(dt)
  
  plan <- audit_dt[!is.na(scaled_column) & nzchar(scaled_column) & !is.na(suggested_scale)]
  if (isTRUE(prefer_scaled_status) && "did_scale" %in% names(audit_dt)) {
    plan <- plan[isTRUE(did_scale) | scale_status %in% c("scaled")]
  }
  if (!nrow(plan)) return(dt)
  
  for (i in seq_len(nrow(plan))) {
    base <- as.character(plan$column[i])
    scol <- as.character(plan$scaled_column[i])
    sc   <- as.numeric(plan$suggested_scale[i])
    
    if (!scol %in% names(dt)) next
    if (base %in% names(dt) && !isTRUE(overwrite)) next
    if (!is.finite(sc) || sc == 0) next
    
    dt[[base]] <- as.numeric(dt[[scol]]) / sc
    if (isTRUE(drop_scaled_cols)) dt[[scol]] <- NULL
  }
  
  dt
}

# =============================================================================
#' Read a Parquet file into a data.table, optionally using Klimo metadata
#'
#' Read Klimo Parquet metadata (spec + audit) without loading full data
#'
#' @param path Parquet file path.
#' @param parse Logical. If TRUE, parse klimo JSON blobs into data.tables.
#' @return A list with kv (named list), kv_dt (data.table), spec_dt (data.table or NULL), audit_dt (data.table or NULL)
#' @export
dp_read_parquet_meta <- function(path, parse = TRUE) {
  .klimo_require("arrow")
  .klimo_require("data.table")
  
  tab <- arrow::read_parquet(path, as_data_frame = FALSE)
  kv  <- .klimo_schema_metadata(tab)
  
  out <- list(
    kv = kv,
    kv_dt = .klimo_kv_to_dt(kv),
    spec_dt = NULL,
    audit_dt = NULL
  )
  
  if (isTRUE(parse)) {
    js_spec  <- kv[["klimo:field_spec_json"]]
    js_audit <- kv[["klimo:audit_json"]]
    
    if (!is.null(js_spec) && nzchar(js_spec)) {
      out$spec_dt <- data.table::as.data.table(.klimo_from_json(js_spec))
    }
    if (!is.null(js_audit) && nzchar(js_audit)) {
      out$audit_dt <- data.table::as.data.table(.klimo_from_json(js_audit))
    }
  }
  
  out
}




#' Quick human summary of Klimo metadata for a parquet file
#' @export
dp_meta_summary <- function(path, max_show = 12L) {
  .klimo_require("data.table")
  
  meta <- dp_read_parquet_meta(path, parse = TRUE)
  kv <- meta$kv
  audit <- meta$audit_dt
  
  declared <- kv[["klimo:declared_system"]]
  writer <- kv[["klimo:writer"]]
  ver <- kv[["klimo:spec_version"]]
  
  cat("Klimo Parquet metadata\n")
  cat(sprintf("  path: %s\n", path))
  cat(sprintf("  writer: %s\n", ifelse(is.null(writer), "<NA>", writer)))
  cat(sprintf("  spec_version: %s\n", ifelse(is.null(ver), "<NA>", ver)))
  cat(sprintf("  declared_system: %s\n", ifelse(is.null(declared), "<NA>", declared)))
  
  if (!is.null(audit) && nrow(audit)) {
    # writer-scaled columns
    scaled <- audit[isTRUE(did_scale) & !is.na(scaled_column) & nzchar(scaled_column), unique(scaled_column)]
    enc <- audit[encoding == "scaled_int", unique(column)]
    
    cat(sprintf("  audit rows: %d\n", nrow(audit)))
    cat(sprintf("  writer-scaled columns created: %d\n", length(scaled)))
    if (length(scaled)) cat(sprintf("    examples: %s\n", paste(head(scaled, max_show), collapse = ", ")))
    
    cat(sprintf("  already-encoded columns (encoding='scaled_int'): %d\n", length(enc)))
    if (length(enc)) cat(sprintf("    examples: %s\n", paste(head(enc, max_show), collapse = ", ")))
  } else {
    cat("  audit: <none>\n")
  }
  
  invisible(meta)
}


# =============================================================================
# Decode helper for already-encoded columns (e.g., lat_idx_10)
# =============================================================================

#' Decode spec-declared encoded columns (encoding='scaled_int')
#'
#' For columns declared as already encoded, creates a decoded numeric column.
#'
#' @param dt data.table
#' @param spec_dt parsed spec table (e.g., meta$spec_dt)
#' @param suffix suffix for decoded column name (default "_decoded")
#' @param drop_encoded whether to drop the encoded/original column
#' @export
dp_decode_encoded <- function(dt, spec_dt, suffix = "_decoded", drop_encoded = FALSE) {
  .klimo_require("data.table")
  dt <- .klimo_dt(dt)
  spec_dt <- .klimo_dt(spec_dt)
  
  enc <- spec_dt[encoding == "scaled_int" & is.finite(encoding_scale) & encoding_scale != 0,
                 .(column, encoding_scale)]
  
  if (!nrow(enc)) return(dt)
  
  out <- data.table::copy(dt)
  for (i in seq_len(nrow(enc))) {
    col <- enc$column[i]
    sc  <- enc$encoding_scale[i]
    if (!col %in% names(out)) next
    
    new_col <- paste0(col, suffix)
    out[[new_col]] <- as.numeric(out[[col]]) / sc
    
    if (isTRUE(drop_encoded)) out[[col]] <- NULL
  }
  
  out
}

# =============================================================================
#' Read a Parquet file into a data.table, optionally using Klimo metadata
#'
#' @param path Parquet path.
#' @param metadata One of:
#'   - "ignore": do not parse metadata
#'   - "return": parse and return metadata but do not change data
#'   - "apply": parse and apply selected behaviors (decode scaled, attach units)
#' @param decode_scaled If TRUE and metadata is available, attempt to decode scaled columns.
#' @param attach_units If TRUE and metadata is available, attach declared unit attributes.
#' @param unit_source "audit" uses declared_units (most direct); "spec" uses metric/imperial fields.
#' @param system When unit_source="spec", choose which units column to use.
#' @param ... Passed to arrow::read_parquet (e.g., col_select)
#'
#' @return
#' If metadata=="ignore": data.table
#' Else: list(dt=..., meta=..., spec_dt=..., audit_dt=...)
#'
#' @export
dp_read_parquet <- function(
    path,
    metadata = c("apply", "return", "ignore"),
    decode_scaled = TRUE,
    attach_units  = TRUE,
    unit_source = c("audit", "spec"),
    system = c("metric", "imperial"),
    ...
) {
  .klimo_require("arrow")
  .klimo_require("data.table")
  
  metadata <- match.arg(metadata)
  unit_source <- match.arg(unit_source)
  system <- match.arg(system)
  
  tab <- arrow::read_parquet(path, as_data_frame = TRUE, ...)
  dt  <- .klimo_dt(tab)
  
  if (metadata == "ignore") return(dt)
  
  meta <- dp_read_parquet_meta(path, parse = TRUE)
  spec_dt  <- meta$spec_dt
  audit_dt <- meta$audit_dt
  
  if (metadata == "apply") {
    if (isTRUE(decode_scaled) && !is.null(audit_dt)) {
      dt <- dp_decode_scaled(dt, audit_dt = audit_dt, prefer_scaled_status = TRUE, drop_scaled_cols = FALSE)
    }
    if (isTRUE(attach_units)) {
      if (unit_source == "audit") {
        dt <- dp_attach_units(dt, audit_dt = audit_dt, prefer = "declared_units")
      } else {
        dt <- dp_attach_units(dt, spec_dt = spec_dt, prefer = "spec", system = system)
      }
    }
  }
  
  list(dt = dt, meta = meta, spec_dt = spec_dt, audit_dt = audit_dt)
}
# this below may be a slightly improved version?
# dp_read_parquet <- function(
#     path,
#     metadata = c("apply", "return", "ignore"),
#     decode_scaled = TRUE,
#     attach_units  = TRUE,
#     unit_source = c("audit", "spec"),
#     system = c("metric", "imperial"),
#     ...
# ) {
#   .klimo_require("arrow")
#   .klimo_require("data.table")
#   
#   metadata <- match.arg(metadata)
#   unit_source <- match.arg(unit_source)
#   system <- match.arg(system)
#   
#   # ---- data (always read) ----
#   tab <- arrow::read_parquet(path, as_data_frame = TRUE, ...)
#   dt  <- .klimo_dt(tab)
#   
#   if (metadata == "ignore") return(dt)
#   
#   # ---- metadata (schema KV) ----
#   meta <- dp_read_parquet_meta(path, parse = TRUE)
#   spec_dt  <- meta$spec_dt
#   audit_dt <- meta$audit_dt
#   
#   # metadata="return" means: return meta, don't alter dt
#   if (metadata == "apply") {
#     if (isTRUE(decode_scaled) && !is.null(audit_dt) && nrow(audit_dt)) {
#       dt <- dp_decode_scaled(
#         dt,
#         audit_dt = audit_dt,
#         prefer_scaled_status = TRUE,
#         drop_scaled_cols = FALSE
#       )
#     }
#     
#     if (isTRUE(attach_units)) {
#       if (unit_source == "audit") {
#         dt <- dp_attach_units(dt, audit_dt = audit_dt, prefer = "declared_units")
#       } else {
#         dt <- dp_attach_units(dt, spec_dt = spec_dt, prefer = "spec", system = system)
#       }
#     }
#   }
#   
#   list(dt = dt, meta = meta, spec_dt = spec_dt, audit_dt = audit_dt)
# }




# =============================================================================
#' Open an Arrow Dataset plus a reference Klimo metadata bundle
#'
#' IMPORTANT: Arrow datasets do not reliably carry per-file schema metadata in a way that
#' supports your Klimo spec/audit workflow. The practical pattern is:
#'   - choose ONE reference file (e.g., first in directory)
#'   - read its metadata as the dataset-level metadata
#'   - optionally validate other files share the same fingerprint (if you store one)
#'
#' @param path Directory (partitioned dataset) OR vector of parquet files.
#' @param ref_file Optional explicit reference file; if NULL, uses the first file found.
#' @param ... Passed to arrow::open_dataset
#'
#' @return list(ds=<Arrow Dataset>, meta=<metadata bundle>, ref_file=<path>)
#' @export
dp_open_dataset_with_meta <- function(path, ref_file = NULL, ...) {
  .klimo_require("arrow")
  
  files <- NULL
  if (length(path) == 1L && dir.exists(path)) {
    files <- list.files(path, pattern = "\\.parquet$", full.names = TRUE, recursive = TRUE)
  } else {
    files <- as.character(path)
    files <- files[file.exists(files)]
  }
  if (!length(files)) .klimo_stop("dp_open_dataset_with_meta(): no parquet files found.")
  
  if (is.null(ref_file)) ref_file <- files[1]
  if (!file.exists(ref_file)) .klimo_stop("Reference file not found: %s", ref_file)
  
  ds <- arrow::open_dataset(path, ...)
  meta <- dp_read_parquet_meta(ref_file, parse = TRUE)
  
  list(ds = ds, meta = meta, ref_file = ref_file)
}
# 
# 
# 
# library(data.table)
# 
# dt <- dp_read_parquet("test.parquet", metadata = "ignore")
# dt
# 
# 
# meta <- dp_read_parquet_meta("test.parquet", parse = TRUE)
# meta$kv_dt
# meta$spec_dt
# meta$audit_dt
# 
# 
# res <- dp_read_parquet("test.parquet", metadata = "return")
# str(res)
# 
# 
# 
# res2 <- dp_read_parquet("test.parquet", metadata = "apply", attach_units = TRUE, unit_source = "audit")
# res2$dt
# attr(res2$dt$ta, "unit")  # should be "degC" based on audit declared_units
# 
# 
# 
# 
# 
# 
# 
# 
# bundle <- dp_open_dataset_with_meta("test.parquet")
# ds <- bundle$ds
# audit_dt <- bundle$meta$audit_dt
# 
# # lazy query
# res <- ds |>
#   dplyr::filter(lat > 35.8) |>
#   dplyr::select(ta, td) |>
#   dplyr::collect()
# 
# res_dt <- data.table::as.data.table(res)
# 
# # decode using metadata (in R)
# res_dt <- dp_decode_scaled(res_dt, audit_dt, drop_scaled_cols = FALSE, overwrite = FALSE)
# res_dt <- dp_attach_units(res_dt, audit_dt = audit_dt, prefer = "declared_units")
# 
# 
# 
# 
# 
# # res <- dp_open_dataset_with_meta("test.parquet", metadata = "return")
# # names(res)
# # # [1] "dt" "meta" "spec_dt" "audit_dt"
# # 
# # res$meta$kv_dt[]
# # res$spec_dt[]
# # res$audit_dt[]
# # 
# # 
# # res <- dp_read_parquet(
# #   "out_scaled_doc.parquet",
# #   metadata = "apply",
# #   decode_scaled = TRUE,
# #   attach_units  = TRUE,
# #   unit_source   = "audit"   # use declared_units
# # )
# # 
# # 
# # 
# # dt <- res$dt
# # attr(dt$ta, "unit")
# # 
# # 
# # 
# # 
# # 














