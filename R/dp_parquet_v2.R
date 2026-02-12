# R/dp_parquet_v2.R
# =============================================================================
# dataplane v2 — Parquet + Dataset metadata + Scaling (public-friendly)
# =============================================================================
# Public API (exported)
# - dp_spec() [alias], dp_spec_default(), dp_set_units(), dp_set_scale()
# - dp_tag_units(), dp_check_units()
# - dp_read_meta(), dp_write(), dp_read()
# - dp_write_dataset_meta(), dp_read_dataset_meta(), dp_write_dataset(), dp_open()
# - dp_detect(), dp_print_detect()
#
# Notes
# - Internal helpers are prefixed as .dp_* (not exported).
# - Metadata payload is stored as gzip+base64 of serialized objects:
#     <prefix>:spec_b64  and  <prefix>:audit_b64
#   (plus scalar keys like <prefix>:declared_system, <prefix>:spec_version, etc.)
# =============================================================================


# =============================================================================
# 0) Small utilities (internal)
# =============================================================================

# `%||%` <- function(x, y) {
#   if (is.null(x) || length(x) == 0L || is.na(x[[1]])) y else x[[1]]
# }
#' Null-coalescing operator
#'
#' @name null_coalesce
#' @aliases %||%
#' @rdname null_coalesce
#' @export
`%||%` <- function(x, y) if (!is.null(x)) x else y


.dp_require <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(sprintf("Required package '%s' is not installed.", pkg), call. = FALSE)
  }
}

.dp_stop <- function(...) stop(sprintf(...), call. = FALSE)
.dp_warn <- function(...) warning(sprintf(...), call. = FALSE, immediate. = TRUE)

.dp_is_scalar_chr <- function(x) is.character(x) && length(x) == 1L && !is.na(x) && nzchar(x)

.dp_trim <- function(x) sub("^\\s+|\\s+$", "", x)

.dp_norm_name <- function(x) {
  x <- tolower(as.character(x))
  x <- .dp_trim(x)
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  x
}

.dp_dt <- function(x) {
  .dp_require("data.table")
  if (inherits(x, "data.table")) return(x)
  
  if (inherits(x, "data.frame")) {
    x <- base::as.data.frame(x)
    keep <- c("names", "row.names", "class")
    at <- attributes(x)
    attributes(x) <- at[intersect(names(at), keep)]
    attr(x, "sorted") <- NULL
    return(data.table::as.data.table(x))
  }
  
  attr(x, "sorted") <- NULL
  data.table::as.data.table(x)
}

.dp_unit_attr_get <- function(x, unit_attr = "units") {
  u <- attr(x, unit_attr, exact = TRUE)
  if (is.null(u) || !length(u) || !nzchar(trimws(as.character(u)[1]))) NA_character_ else as.character(u)[1]
}


# =============================================================================
# 1) Metadata keys + kv pack/unpack (single system) (internal)
# =============================================================================

.dp_meta_keys <- function(prefix = "dp") {
  p <- as.character(prefix)[1]
  if (is.na(p) || !nzchar(p)) .dp_stop(".dp_meta_keys(): `prefix` must be a non-empty string.")
  
  list(
    prefix = p,
    
    writer         = paste0(p, ":writer"),
    written_at_utc = paste0(p, ":written_at_utc"),
    
    declared_system = paste0(p, ":declared_system"),
    spec_version    = paste0(p, ":spec_version"),
    
    # payloads (serialized -> gzip -> base64)
    spec_b64  = paste0(p, ":spec_b64"),
    audit_b64 = paste0(p, ":audit_b64"),
    
    # optional context (best-effort; not required)
    compression            = paste0(p, ":compression"),
    write_format           = paste0(p, ":write_format"),
    partitioning           = paste0(p, ":partitioning"),
    existing_data_behavior = paste0(p, ":existing_data_behavior"),
    sidecar_compression    = paste0(p, ":sidecar_compression")
  )
}

# Small helper to build dynamic kv pairs: .dp_kv_set(key1,val1,key2,val2,...)
# (internal; most users should not need this)
.dp_kv_set <- function(...) {
  args <- list(...)
  if (!length(args)) return(list())
  if (length(args) %% 2 != 0) {
    stop(".dp_kv_set(): must supply key1, value1, key2, value2, ...", call. = FALSE)
  }
  
  out <- list()
  for (i in seq(1, length(args), by = 2)) {
    k <- as.character(args[[i]])[1]
    if (is.na(k) || !nzchar(k)) next
    v <- as.character(args[[i + 1]])
    out[[k]] <- if (!length(v)) NA_character_ else v[1]
  }
  out
}

.dp_meta_pick_prefix <- function(kv_names, preferred = "dp") {
  kv_names <- as.character(kv_names %||% character())
  prefixes <- unique(c(preferred, fallbacks))
  prefixes <- prefixes[!is.na(prefixes) & nzchar(prefixes)]
  if (!length(prefixes)) return(preferred)
  
  for (p in prefixes) {
    keys <- .dp_meta_keys(p)
    if (any(kv_names %in% c(keys$spec_b64, keys$audit_b64, keys$declared_system, keys$spec_version))) {
      return(p)
    }
    if (any(startsWith(kv_names, paste0(p, ":")))) {
      # fallback: any "<p>:" suggests that prefix
      return(p)
    }
  }
  
  preferred
}

.dp_meta_pack_kv <- function(spec, audit_dt, extra = list(), prefix = "dp") {
  .dp_require("data.table")
  .dp_require("jsonlite")
  
  if (!inherits(spec, "dp_spec")) {
    .dp_stop(".dp_meta_pack_kv(): `spec` must be a <dp_spec>.")
  }
  
  spec <- .dp_spec_resolve_declared_units(spec)
  keys <- .dp_meta_keys(prefix)
  
  .enc_obj <- function(obj) {
    raw <- serialize(obj, NULL)
    raw <- memCompress(raw, type = "gzip")
    jsonlite::base64_enc(raw)
  }
  
  # normalize payload objects
  spec_fields <- spec$fields
  if (!data.table::is.data.table(spec_fields)) spec_fields <- data.table::as.data.table(spec_fields)
  audit_dt <- data.table::copy(.dp_dt(audit_dt))
  
  ds  <- (spec$declared_system %||% spec$system) %||% "custom"
  ver <- (spec$version %||% "2")
  
  kv <- list()
  kv[[keys$writer]]          <- "dataplane"
  kv[[keys$written_at_utc]]  <- format(Sys.time(), tz = "UTC", usetz = TRUE)
  kv[[keys$declared_system]] <- as.character(ds)[1]
  kv[[keys$spec_version]]    <- as.character(ver)[1]
  kv[[keys$spec_b64]]        <- .enc_obj(spec_fields)
  kv[[keys$audit_b64]]       <- .enc_obj(audit_dt)
  
  # merge extra (caller can override)
  if (is.list(extra) && length(extra)) {
    nms <- names(extra)
    for (i in seq_along(extra)) {
      nm <- nms[i]
      if (!is.null(nm) && nzchar(nm)) {
        vv <- as.character(extra[[i]])
        kv[[nm]] <- if (!length(vv)) NA_character_ else vv[1]
      }
    }
  }
  
  # normalize to character(1)
  kv <- lapply(kv, function(v) {
    v <- as.character(v)
    if (!length(v)) NA_character_ else v[1]
  })
  
  kv
}

.dp_meta_unpack_kv <- function(kv, preferred_prefix = "dp", parse = TRUE) {
  .dp_require("data.table")
  .dp_require("jsonlite")
  
  kv <- kv %||% list()
  if (!is.list(kv)) .dp_stop(".dp_meta_unpack_kv(): kv must be a list.")
  
  prefix <- .dp_meta_pick_prefix(names(kv), preferred = preferred_prefix)
  keys <- .dp_meta_keys(prefix)
  
  out <- list(
    prefix   = prefix,
    keys     = keys,
    kv       = kv,
    spec_dt  = NULL,
    audit_dt = NULL
  )
  
  if (!isTRUE(parse)) return(out)
  
  .dec_obj <- function(b64) {
    if (is.null(b64) || !length(b64) || is.na(b64) || !nzchar(b64)) return(NULL)
    raw <- jsonlite::base64_dec(as.character(b64)[1])
    raw <- memDecompress(raw, type = "gzip")
    unserialize(raw)
  }
  
  spec_dt  <- .dec_obj(kv[[keys$spec_b64]])
  audit_dt <- .dec_obj(kv[[keys$audit_b64]])
  
  if (!is.null(spec_dt))  out$spec_dt  <- data.table::as.data.table(spec_dt)
  if (!is.null(audit_dt)) out$audit_dt <- data.table::as.data.table(audit_dt)
  
  out
}


# =============================================================================
# 2) Spec object (dp_spec): declared units + scaling intent
# =============================================================================

.dp_spec_new <- function(fields_dt, declared_system = c("metric", "imperial", "custom"), version = "2") {
  .dp_require("data.table")
  
  declared_system <- match.arg(declared_system)
  fields_dt <- .dp_dt(fields_dt)
  
  if (!("column" %in% names(fields_dt))) .dp_stop("Spec table must contain 'column'.")
  
  fields_dt[, column := as.character(column)]
  fields_dt[, column_norm := .dp_norm_name(column)]
  
  if (!("concept" %in% names(fields_dt)))             fields_dt[, concept := NA_character_]
  if (!("metric_units" %in% names(fields_dt)))        fields_dt[, metric_units := NA_character_]
  if (!("imperial_units" %in% names(fields_dt)))      fields_dt[, imperial_units := NA_character_]
  if (!("declared_units" %in% names(fields_dt)))      fields_dt[, declared_units := NA_character_]
  if (!("writer_scale" %in% names(fields_dt)))        fields_dt[, writer_scale := as.numeric(NA)]
  
  # reserved / forward fields
  if (!("encoding" %in% names(fields_dt)))            fields_dt[, encoding := "none"]
  if (!("encoding_scale" %in% names(fields_dt)))      fields_dt[, encoding_scale := as.numeric(NA)]
  if (!("encoding_base_units" %in% names(fields_dt))) fields_dt[, encoding_base_units := NA_character_]
  
  fields_dt[, `:=`(
    concept = as.character(concept),
    metric_units = as.character(metric_units),
    imperial_units = as.character(imperial_units),
    declared_units = as.character(declared_units),
    writer_scale = suppressWarnings(as.numeric(writer_scale)),
    encoding = as.character(encoding),
    encoding_scale = suppressWarnings(as.numeric(encoding_scale)),
    encoding_base_units = as.character(encoding_base_units)
  )]
  
  fields_dt[is.na(encoding) | !nzchar(encoding), encoding := "none"]
  
  bad_enc <- setdiff(unique(fields_dt$encoding), c("none", "scaled_int"))
  if (length(bad_enc)) {
    .dp_stop(
      "Unsupported encoding value(s): %s. Allowed: 'none', 'scaled_int'.",
      paste(bad_enc, collapse = ", ")
    )
  }
  
  out <- list(fields = fields_dt, declared_system = declared_system, version = as.character(version))
  class(out) <- "dp_spec"
  out
}

# -----------------------------------------------------------------------------
#' Dataplane spec objects
#'
#' A `dp_spec` describes columns: concepts, unit systems, and optional scaling
#' intent (`writer_scale`) used for integer storage encoding.
#'
#' The typical workflow:
#' 1) Create a spec with [dp_spec_default()] (or [dp_spec()] as an alias)
#' 2) Edit with [dp_set_units()] and/or [dp_set_scale()]
#' 3) Use in I/O via [dp_write()] / [dp_read()] or for in-memory annotation via
#'    [dp_tag_units()] / [dp_check_units()].
#'
#' @name dp_spec
#' @rdname dp_spec
NULL

# -----------------------------------------------------------------------------
#' Print a Dataplane spec
#'
#' Compact printer for `dp_spec` objects.
#'
#' @param x A `dp_spec`.
#' @param ... Unused.
#'
#' @return The input `x` (invisibly).
#' @exportS3Method print dp_spec
print.dp_spec <- function(x, ...) {
  .dp_require("data.table")
  dt <- x$fields
  n <- nrow(dt)
  n_concept <- dt[!is.na(concept) & nzchar(concept), .N]
  n_decl <- dt[!is.na(declared_units) & nzchar(declared_units), .N]
  n_scale <- dt[is.finite(writer_scale) & writer_scale != 0, .N]
  cat(sprintf("<dp_spec> system=%s; version=%s\n", x$declared_system, x$version))
  cat(sprintf("  rows=%d; concept=%d; declared_units=%d; writer_scale=%d\n", n, n_concept, n_decl, n_scale))
  invisible(x)
}

.dp_spec_resolve_declared_units <- function(spec) {
  .dp_require("data.table")
  
  if (!inherits(spec, "dp_spec")) {
    .dp_stop(".dp_spec_resolve_declared_units(): `spec` must be a <dp_spec>.")
  }
  if (is.null(spec$fields) || !data.table::is.data.table(spec$fields) || !nrow(spec$fields)) {
    return(spec)
  }
  
  f <- data.table::copy(spec$fields)
  
  for (nm in c("declared_units", "metric_units", "imperial_units")) {
    if (!(nm %in% names(f))) f[, (nm) := NA_character_]
  }
  
  .trim <- function(x) {
    x <- as.character(x)
    x <- trimws(x)
    x[!nzchar(x)] <- NA_character_
    x
  }
  
  f[, `:=`(
    declared_units = .trim(declared_units),
    metric_units   = .trim(metric_units),
    imperial_units = .trim(imperial_units)
  )]
  
  sys <- (spec$declared_system %||% spec$system) %||% NA_character_
  sys <- as.character(sys)[1]
  sys <- if (!is.na(sys)) trimws(sys) else NA_character_
  
  miss <- which(is.na(f$declared_units))
  if (length(miss)) {
    if (identical(sys, "metric")) {
      f[miss, declared_units := metric_units]
    } else if (identical(sys, "imperial")) {
      f[miss, declared_units := imperial_units]
    } else {
      f[miss, declared_units := NA_character_]
    }
  }
  
  spec$fields <- f
  spec
}

# Minimal default concept catalog (optional; safe, no column-name hardcoding beyond synonyms)
.dp_spec_catalog <- function() {
  .dp_require("data.table")
  
  data.table::rbindlist(list(
    data.table::data.table(concept="air_temp", metric_units="degC", imperial_units="degF", writer_scale=100,
                           synonyms=list(c("ta","temp","temperature","airtemp","air_temp","ta_2m"))),
    data.table::data.table(concept="dewpoint", metric_units="degC", imperial_units="degF", writer_scale=100,
                           synonyms=list(c("td","dewpoint","dew_point"))),
    data.table::data.table(concept="relhum", metric_units="percent", imperial_units="percent", writer_scale=100,
                           synonyms=list(c("rh","relh","relhum","rel_hum","relative_humidity"))),
    data.table::data.table(concept="wind_speed", metric_units="m/s", imperial_units="mph", writer_scale=100,
                           synonyms=list(c("wind","wind10m","wspd","speed","gust"))),
    data.table::data.table(concept="wind_dir", metric_units="deg", imperial_units="deg", writer_scale=10,
                           synonyms=list(c("dd","dir","wdir","wind_dir","winddirection"))),
    data.table::data.table(concept="lat", metric_units="deg", imperial_units="deg", writer_scale=10000,
                           synonyms=list(c("lat","latitude"))),
    data.table::data.table(concept="lon", metric_units="deg", imperial_units="deg", writer_scale=10000,
                           synonyms=list(c("lon","longitude","lng"))),
    data.table::data.table(concept="wbgt", metric_units="degC", imperial_units="degF", writer_scale=100,
                           synonyms=list(c("wbgt","nwb","tg")))
  ), use.names = TRUE, fill = TRUE)
}

# -----------------------------------------------------------------------------
#' Create a default spec from a table
#'
#' Builds a `dp_spec` describing the columns in `x`. Dataplane uses a small
#' built-in concept catalog (synonyms) to populate known concepts/units/scales.
#' Unmatched columns are left as `NA` by design.
#'
#' @param x A `data.frame` or `data.table` (only column names are used).
#' @param declared_system Character scalar: `"metric"` or `"imperial"`. This
#'   controls how `declared_units` are inferred when not explicitly set.
#' @param include_unmatched Logical. If `TRUE` (default), keep all columns even if
#'   no concept match is found. If `FALSE`, only keep matched columns.
#'
#' @return A `dp_spec` object.
#' @export
#'
#' @examples
#' \dontrun{
#' library(data.table)
#' dt <- data.table(temp = 25.1, rh = 55)
#' sp <- dp_spec_default(dt, declared_system = "metric")
#' sp
#' sp$fields[]
#' }
dp_spec_default <- function(x, declared_system = c("metric", "imperial"), include_unmatched = TRUE) {
  .dp_require("data.table")
  declared_system <- match.arg(declared_system)
  
  x <- .dp_dt(x)
  cols <- names(x)
  cols_norm <- .dp_norm_name(cols)
  
  cat_dt <- .dp_spec_catalog()
  map <- cat_dt[, {
    syn <- unlist(synonyms, use.names = FALSE)
    if (!length(syn)) syn <- character()
    .(synonym = syn)
  }, by = .(concept, metric_units, imperial_units, writer_scale)]
  
  map[, synonym_norm := .dp_norm_name(synonym)]
  
  idx <- match(cols_norm, map$synonym_norm)
  matched <- !is.na(idx)
  
  out_dt <- data.table::data.table(
    column        = cols,
    concept       = ifelse(matched, map$concept[idx], NA_character_),
    metric_units  = ifelse(matched, map$metric_units[idx], NA_character_),
    imperial_units= ifelse(matched, map$imperial_units[idx], NA_character_),
    declared_units= NA_character_,
    writer_scale  = ifelse(matched, as.numeric(map$writer_scale[idx]), as.numeric(NA)),
    encoding      = "none",
    encoding_scale= as.numeric(NA),
    encoding_base_units = NA_character_
  )
  
  if (!isTRUE(include_unmatched)) {
    out_dt <- out_dt[!is.na(concept) & nzchar(concept)]
  }
  
  .dp_spec_new(out_dt, declared_system = declared_system, version = "2")
}

# -----------------------------------------------------------------------------
#' Create a Dataplane spec (alias)
#'
#' Alias for [dp_spec_default()].
#'
#' @inheritParams dp_spec_default
#' @return A `dp_spec` object.
#' @export
dp_spec <- function(x, declared_system = c("metric", "imperial"), include_unmatched = TRUE) {
  dp_spec_default(x = x, declared_system = declared_system, include_unmatched = include_unmatched)
}

# -----------------------------------------------------------------------------
#' Set units in a spec
#'
#' Updates unit fields within a `dp_spec` for one or more columns.
#'
#' Relationship to [dp_tag_units()]:
#' - `dp_set_units()` updates the *spec* (the contract / documentation)
#' - `dp_tag_units()` annotates an *in-memory R object* based on that spec
#'
#' @param spec A `dp_spec`.
#' @param columns Character vector of column names to edit.
#' @param metric_units Optional character scalar unit to set for selected columns.
#' @param imperial_units Optional character scalar unit to set for selected columns.
#' @param declared_units Optional character scalar unit override to set for selected columns.
#'
#' @return Updated `dp_spec`.
#' @export
#'
#' @examples
#' \dontrun{
#' library(data.table)
#' dt <- data.table(wind = 2.1)
#' sp <- dp_spec_default(dt, "metric")
#' sp <- dp_set_units(sp, "wind", metric_units = "m/s", imperial_units = "mph")
#' }
dp_set_units <- function(spec, columns, metric_units = NULL, imperial_units = NULL, declared_units = NULL) {
  .dp_require("data.table")
  if (!inherits(spec, "dp_spec")) .dp_stop("dp_set_units(): `spec` must be a <dp_spec>.")
  dt <- data.table::copy(spec$fields)
  
  stopifnot(is.character(columns), length(columns) >= 1L)
  hit <- .dp_norm_name(columns)
  dt[, .hit := column_norm %in% hit]
  
  if (!is.null(metric_units)) {
    if (!.dp_is_scalar_chr(metric_units)) .dp_stop("dp_set_units(): metric_units must be a character scalar.")
    dt[.hit == TRUE, metric_units := metric_units]
  }
  if (!is.null(imperial_units)) {
    if (!.dp_is_scalar_chr(imperial_units)) .dp_stop("dp_set_units(): imperial_units must be a character scalar.")
    dt[.hit == TRUE, imperial_units := imperial_units]
  }
  if (!is.null(declared_units)) {
    if (!.dp_is_scalar_chr(declared_units)) .dp_stop("dp_set_units(): declared_units must be a character scalar.")
    dt[.hit == TRUE, declared_units := declared_units]
  }
  
  dt[, .hit := NULL]
  spec$fields <- dt
  spec
}

# -----------------------------------------------------------------------------
#' Set scaling intent in a spec
#'
#' Sets `writer_scale` for selected columns in a `dp_spec`.
#'
#' This does not change your data by itself; it records the intent used by
#' [dp_scale_encode()] and by [dp_write(..., scale = TRUE)].
#'
#' @param spec A `dp_spec`.
#' @param columns Character vector of column names to edit.
#' @param writer_scale Numeric scalar scale factor (or `NA` to clear).
#'
#' @return Updated `dp_spec`.
#' @export
#'
#' @examples
#' \dontrun{
#' library(data.table)
#' dt <- data.table(temp = 25.12)
#' sp <- dp_spec_default(dt, "metric")
#' sp <- dp_set_scale(sp, "temp", 100)  # keep 2 decimals as integer storage
#' }
dp_set_scale <- function(spec, columns, writer_scale) {
  .dp_require("data.table")
  if (!inherits(spec, "dp_spec")) .dp_stop("dp_set_scale(): `spec` must be a <dp_spec>.")
  if (length(writer_scale) != 1L) .dp_stop("dp_set_scale(): writer_scale must be a scalar numeric (or NA).")
  
  dt <- data.table::copy(spec$fields)
  hit <- .dp_norm_name(columns)
  dt[, .hit := column_norm %in% hit]
  
  sc <- suppressWarnings(as.numeric(writer_scale))
  dt[.hit == TRUE, writer_scale := sc]
  
  dt[, .hit := NULL]
  spec$fields <- dt
  spec
}


# =============================================================================
# 3) Units: optional tagging + optional validation
# =============================================================================

.dp_units_expected_from_spec <- function(fields, system = c("declared", "metric", "imperial"), declared_system = NULL) {
  .dp_require("data.table")
  system <- match.arg(system)
  
  for (nm in c("declared_units", "metric_units", "imperial_units")) {
    if (!(nm %in% names(fields))) fields[, (nm) := NA_character_]
  }
  
  if (system == "declared") {
    expected <- as.character(fields$declared_units)
    
    # fallback only if declared_units is entirely empty
    if (!is.null(declared_system) &&
        (length(expected) == 0L || all(is.na(expected) | !nzchar(trimws(expected))))) {
      if (identical(declared_system, "metric")) {
        expected <- as.character(fields$metric_units)
      } else if (identical(declared_system, "imperial")) {
        expected <- as.character(fields$imperial_units)
      }
    }
  } else if (system == "metric") {
    expected <- as.character(fields$metric_units)
  } else {
    expected <- as.character(fields$imperial_units)
  }
  
  if (!length(expected)) expected <- rep(NA_character_, nrow(fields))
  expected
}

# -----------------------------------------------------------------------------
#' Tag unit attributes onto columns
#'
#' Attaches units as an attribute (default attribute name: `"units"`) onto
#' columns of an R table, based on a `dp_spec`.
#'
#' Intended use:
#' - in-memory annotation to reduce accidental unit confusion during analysis
#' - no file I/O
#'
#' @param dt A `data.frame` or `data.table`.
#' @param spec A `dp_spec` with a `fields` table.
#' @param only_missing Logical. If `TRUE`, only attach units when the attribute
#'   is currently missing.
#' @param system Which unit system to tag: `"declared"` (default), `"metric"`,
#'   or `"imperial"`.
#' @param unit_attr Attribute name to write (default `"units"`).
#'
#' @return A list with:
#' - `dt`: a copied `data.table` with attributes attached
#' - `did_tag`: an audit `data.table` describing what was tagged
#' @export
#'
#' @examples
#' \dontrun{
#' library(data.table)
#' dt <- data.table(temp = 25.1)
#' sp <- dp_spec_default(dt, "metric")
#' res <- dp_tag_units(dt, sp)
#' attr(res$dt$temp, "units")
#' }
dp_tag_units <- function(
    dt,
    spec,
    only_missing = TRUE,
    system = c("declared", "metric", "imperial"),
    unit_attr = "units"
) {
  .dp_require("data.table")
  dt0 <- .dp_dt(dt)
  
  if (!inherits(spec, "dp_spec") || is.null(spec$fields) || !data.table::is.data.table(spec$fields)) {
    .dp_stop("dp_tag_units(): `spec` must be a <dp_spec> with data.table `spec$fields`.")
  }
  
  f <- data.table::copy(spec$fields)
  if (!("column" %in% names(f))) .dp_stop("dp_tag_units(): `spec$fields` must include `column`.")
  
  system <- match.arg(system)
  declared_system <- tryCatch(spec$declared_system, error = function(e) NULL)
  
  expected <- .dp_units_expected_from_spec(fields = f, system = system, declared_system = declared_system)
  f[, expected_units := expected]
  
  out <- data.table::copy(dt0)  # no side effects
  dt_cols <- names(out)
  
  did <- data.table::data.table(
    column = dt_cols,
    tag_units = NA_character_,
    previous_units = NA_character_,
    did_tag = FALSE
  )
  
  m <- match(dt_cols, f$column)
  spec_units <- rep(NA_character_, length(dt_cols))
  spec_units[!is.na(m)] <- f$expected_units[m[!is.na(m)]]
  
  for (i in seq_along(dt_cols)) {
    col <- dt_cols[i]
    u_exp <- spec_units[i]
    if (is.na(u_exp) || !nzchar(trimws(u_exp))) next
    
    u_prev_chr <- .dp_unit_attr_get(out[[col]], unit_attr = unit_attr)
    
    if (isTRUE(only_missing) && !is.na(u_prev_chr)) {
      did[i, previous_units := u_prev_chr]
      next
    }
    
    attr(out[[col]], unit_attr) <- u_exp
    did[i, `:=`(tag_units = u_exp, previous_units = u_prev_chr, did_tag = TRUE)]
  }
  
  list(dt = out, did_tag = did)
}

# -----------------------------------------------------------------------------
#' Check unit attributes against a spec
#'
#' Compares unit attributes in a table (default attribute name `"units"`) against
#' expected units from a `dp_spec`. By default, this warns and returns a report.
#'
#' @param dt A `data.frame` or `data.table`.
#' @param spec A `dp_spec` with a `fields` table.
#' @param system Which unit system to validate against: `"declared"` (default),
#'   `"metric"`, or `"imperial"`.
#' @param unit_attr Attribute name to read (default `"units"`).
#' @param warn_max Maximum number of problems to show in the warning message.
#' @param stop_on_any Logical. If `TRUE`, stop if any problems are found.
#'
#' @return An audit `data.table` (invisibly) describing expected vs observed units
#' and a status code.
#' @export
#'
#' @examples
#' \dontrun{
#' library(data.table)
#' dt <- data.table(temp = 25.1)
#' sp <- dp_spec_default(dt, "metric")
#' dt2 <- dp_tag_units(dt, sp)$dt
#' dp_check_units(dt2, sp)
#' }
dp_check_units <- function(
    dt,
    spec,
    system = c("declared", "metric", "imperial"),
    unit_attr = "units",
    warn_max = 8L,
    stop_on_any = FALSE
) {
  .dp_require("data.table")
  dt0 <- .dp_dt(dt)
  
  if (!inherits(spec, "dp_spec") || is.null(spec$fields) || !data.table::is.data.table(spec$fields)) {
    .dp_stop("dp_check_units(): `spec` must be a <dp_spec> with data.table `spec$fields`.")
  }
  
  f <- data.table::copy(spec$fields)
  if (!("column" %in% names(f))) .dp_stop("dp_check_units(): `spec$fields` must include `column`.")
  
  system <- match.arg(system)
  declared_system <- tryCatch(spec$declared_system, error = function(e) NULL)
  
  expected <- .dp_units_expected_from_spec(fields = f, system = system, declared_system = declared_system)
  f[, expected_units := expected]
  
  dt_cols <- names(dt0)
  observed_units <- vapply(dt_cols, function(nm) {
    .dp_unit_attr_get(dt0[[nm]], unit_attr = unit_attr)
  }, character(1))
  
  audit <- data.table::data.table(
    column = as.character(f$column),
    expected_units = as.character(f$expected_units)
  )
  
  idx <- match(audit$column, dt_cols)
  audit[, observed_units := NA_character_]
  audit[!is.na(idx), observed_units := observed_units[idx[!is.na(idx)]]]
  
  .trim <- function(x) { x <- trimws(as.character(x)); x[!nzchar(x)] <- NA_character_; x }
  audit[, `:=`(expected_units = .trim(expected_units), observed_units = .trim(observed_units))]
  
  audit[, status := data.table::fcase(
    is.na(idx), "missing_column_in_dt",
    is.na(expected_units) & is.na(observed_units), "no_units",
    !is.na(expected_units) & is.na(observed_units), "missing_units",
    is.na(expected_units) & !is.na(observed_units), "unexpected_units",
    !is.na(expected_units) & !is.na(observed_units) & expected_units == observed_units, "ok",
    default = "mismatch"
  )]
  
  probs <- audit[status != "ok"]
  if (nrow(probs)) {
    n_show <- max(0L, as.integer(warn_max))
    if (n_show > 0L) {
      show <- probs[seq_len(min(n_show, nrow(probs)))]
      warning(
        paste0(
          "dp_check_units(): found unit/schema issues (showing ", nrow(show), " of ", nrow(probs), "):\n",
          paste0(
            "  - ", show$column, ": ", show$status,
            ifelse(!is.na(show$expected_units), paste0(" | expected=", show$expected_units), ""),
            ifelse(!is.na(show$observed_units), paste0(" | observed=", show$observed_units), ""),
            collapse = "\n"
          )
        ),
        call. = FALSE
      )
    }
    if (isTRUE(stop_on_any)) {
      .dp_stop("dp_check_units(): validation failed (stop_on_any=TRUE).")
    }
  }
  
  invisible(audit)
}


# =============================================================================
# 4) Scaling: encode to int storage columns + decode back (opt-in)
# =============================================================================

# -----------------------------------------------------------------------------
#' Encode numeric columns into integer storage columns
#'
#' Creates new integer storage columns (default suffix `"_i"`) by multiplying
#' numeric columns by their `writer_scale` in the spec.
#'
#' Intended use:
#' - stable, compact integer storage while retaining predictable decimal precision
#' - consistent round-trip with [dp_scale_decode()]
#'
#' @param dt A `data.frame` or `data.table`.
#' @param spec A `dp_spec` describing scaling intent (`writer_scale`).
#' @param suffix Suffix for storage columns (default `"_i"`).
#' @param drop_original Logical. If `TRUE`, drop the original float columns.
#' @param round_fn Rounding method: `"round"`, `"floor"`, or `"ceiling"`.
#'
#' @return A list with:
#' - `dt`: a copied `data.table` with storage columns added
#' - `scale_map`: a `data.table` describing what was scaled and where
#' @export
#'
#' @examples
#' \dontrun{
#' library(data.table)
#' dt <- data.table(temp = c(25.12, 25.44))
#' sp <- dp_spec_default(dt, "metric")
#' sp <- dp_set_scale(sp, "temp", 100)
#' enc <- dp_scale_encode(dt, sp)
#' enc$dt
#' }
dp_scale_encode <- function(
    dt,
    spec,
    suffix = "_i",
    drop_original = FALSE,
    round_fn = c("round", "floor", "ceiling")
) {
  .dp_require("data.table")
  dt <- data.table::copy(.dp_dt(dt))
  round_fn <- match.arg(round_fn)
  
  if (!inherits(spec, "dp_spec")) .dp_stop("dp_scale_encode(): `spec` must be a <dp_spec>.")
  spec <- .dp_spec_resolve_declared_units(spec)
  s <- spec$fields
  
  plan <- s[
    encoding == "none" &
      is.finite(writer_scale) & writer_scale != 0,
    .(
      column = as.character(column),
      writer_scale = as.numeric(writer_scale),
      storage_column = paste0(as.character(column), suffix),
      declared_units = as.character(declared_units)
    )
  ]
  
  if (!nrow(plan)) {
    plan[, did_scale := logical(0)]
    return(list(dt = dt, scale_map = plan))
  }
  
  plan[, did_scale := FALSE]
  
  for (i in seq_len(nrow(plan))) {
    col  <- plan$column[i]
    sc   <- plan$writer_scale[i]
    scol <- plan$storage_column[i]
    
    if (!col %in% names(dt)) next
    
    v <- dt[[col]]
    if (!(is.numeric(v) || is.integer(v))) next
    if (!is.finite(sc) || sc == 0) next
    
    vv <- switch(
      round_fn,
      round   = as.integer(round(v * sc)),
      floor   = as.integer(floor(v * sc)),
      ceiling = as.integer(ceiling(v * sc))
    )
    
    dt[[scol]] <- vv
    plan$did_scale[i] <- TRUE
    
    if (isTRUE(drop_original)) dt[[col]] <- NULL
  }
  
  list(dt = dt, scale_map = plan)
}

# -----------------------------------------------------------------------------
#' Decode scaled integer columns back to numeric columns (using audit metadata)
#'
#' If a writer created columns like `temp_i` with `writer_scale = 100`, this can
#' recreate `temp = temp_i / 100`. Optionally keeps or drops the integer storage
#' columns.
#'
#' `audit_dt` is typically obtained from metadata via [dp_read_meta()] or from
#' the `meta` element returned by [dp_read()].
#'
#' @param dt A `data.frame` or `data.table`.
#' @param audit_dt Parsed audit/plan table. Must contain:
#'   `column`, `writer_scale`, `storage_column`, `did_scale`.
#' @param keep_storage Logical. If `TRUE`, keep the integer storage columns.
#' @param overwrite Logical. If `TRUE`, overwrite existing base columns if present.
#'
#' @return A decoded `data.table` (modified copy).
#' @export
#'
#' @examples
#' \dontrun{
#' library(data.table)
#' dt <- data.table(temp_i = c(2512L, 2544L))
#' audit <- data.table(column="temp", writer_scale=100, storage_column="temp_i", did_scale=TRUE)
#' dp_scale_decode(dt, audit)
#' }
dp_scale_decode <- function(dt, audit_dt, keep_storage = TRUE, overwrite = FALSE) {
  .dp_require("data.table")
  dt <- data.table::copy(.dp_dt(dt))
  audit_dt <- .dp_dt(audit_dt)
  
  need <- c("column", "writer_scale", "storage_column", "did_scale")
  if (!all(need %in% names(audit_dt))) return(dt)
  
  # NOTE: do not use isTRUE() on a vector column; filter explicitly.
  did_scale_flag <- audit_dt[["did_scale"]]
  if (!is.logical(did_scale_flag)) did_scale_flag <- as.logical(did_scale_flag)
  
  plan <- audit_dt[
    (did_scale_flag %in% TRUE) &
      is.finite(writer_scale) & writer_scale != 0 &
      !is.na(storage_column) & nzchar(storage_column)
  ]
  if (!nrow(plan)) return(dt)
  
  for (i in seq_len(nrow(plan))) {
    base <- as.character(plan$column[i])
    scol <- as.character(plan$storage_column[i])
    sc   <- as.numeric(plan$writer_scale[i])
    
    if (!scol %in% names(dt)) next
    if (base %in% names(dt) && !isTRUE(overwrite)) next
    
    dt[[base]] <- as.numeric(dt[[scol]]) / sc
    if (!isTRUE(keep_storage)) dt[[scol]] <- NULL
  }
  
  dt
}


# =============================================================================
# 5) Parquet schema metadata read/write (single-file)
# =============================================================================

.dp_read_parquet_kv <- function(path) {
  .dp_require("arrow")
  
  # Try schema-first
  kv <- tryCatch({
    sch <- arrow::read_parquet_schema(path)
    sch$metadata
  }, error = function(e) NULL)
  
  # Fallback to ParquetFileReader
  if (is.null(kv)) {
    kv <- tryCatch({
      rdr <- arrow::ParquetFileReader$create(path)
      md  <- rdr$GetFileMetaData()
      kv2 <- md$key_value_metadata
      if (is.null(kv2)) return(list())
      kv2 <- as.data.frame(kv2, stringsAsFactors = FALSE)
      if (!all(c("key", "value") %in% names(kv2))) return(list())
      out <- as.list(kv2$value)
      names(out) <- kv2$key
      out
    }, error = function(e) NULL)
  }
  
  kv <- kv %||% list()
  
  # Normalize to list of character(1)
  out <- list()
  nms <- names(kv) %||% character()
  for (i in seq_along(kv)) {
    k <- nms[i] %||% ""
    if (!nzchar(k)) next
    v <- kv[[i]]
    if (is.raw(v)) v <- rawToChar(v)
    vv <- as.character(v)
    out[[k]] <- if (!length(vv)) NA_character_ else vv[1]
  }
  
  out
}

# -----------------------------------------------------------------------------
#' Read Dataplane metadata from a Parquet file (metadata-only)
#'
#' Reads schema key/value metadata from a Parquet file and (optionally) parses
#' the Dataplane payloads (`spec_dt`, `audit_dt`).
#'
#' @param path Path to a Parquet file.
#' @param parse Logical. If `TRUE`, parse serialized payloads into `spec_dt`
#'   and `audit_dt`.
#' @param preferred_prefix Preferred metadata prefix (default `"dp"`).
#'
#' @return A list with:
#' - `prefix`: detected prefix
#' - `keys`: resolved key names for the detected prefix
#' - `kv`: raw key/value list
#' - `spec_dt`: parsed spec table (or `NULL`)
#' - `audit_dt`: parsed audit table (or `NULL`)
#' @export
#'
#' @examples
#' \dontrun{
#' m <- dp_read_meta("file.parquet")
#' names(m$kv)
#' }
dp_read_meta <- function(path, parse = TRUE, preferred_prefix = "dp") {
  kv <- .dp_read_parquet_kv(path)
  .dp_meta_unpack_kv(kv, preferred_prefix = preferred_prefix, parse = parse)
}


# =============================================================================
# 6) Dataset sidecar meta (recommended for Arrow Datasets)
# =============================================================================

# -----------------------------------------------------------------------------
#' Write Dataplane dataset metadata sidecar
#'
#' Writes a tiny Parquet file in `dataset_path` (default file name: `dp_meta.parquet`)
#' containing Dataplane schema metadata in its Parquet schema key/value pairs.
#'
#' This provides a stable, single place to read dataset-level metadata without
#' scanning all data files.
#'
#' @param dataset_path Dataset directory path.
#' @param kv Named list of key/value metadata (character scalars recommended).
#' @param prefix Prefix used for default sidecar name (default `"dp"`).
#' @param overwrite Logical. If `FALSE` and sidecar exists, stop.
#' @param compression Parquet compression for the sidecar (default `"zstd"`).
#' @param sidecar_name Optional override for sidecar file name.
#'
#' @return The sidecar path (invisibly).
#' @export
#'
#' @examples
#' \dontrun{
#' kv <- list("dp:writer"="example")
#' dp_write_dataset_meta("dataset_dir", kv)
#' }
dp_write_dataset_meta <- function(dataset_path, kv, prefix = "dp", overwrite = TRUE, compression = "zstd", sidecar_name = NULL) {
  .dp_require("arrow")
  .dp_require("data.table")
  
  if (!dir.exists(dataset_path)) dir.create(dataset_path, recursive = TRUE, showWarnings = FALSE)
  
  if (is.null(sidecar_name)) sidecar_name <- paste0(prefix, "_meta.parquet")
  sidecar_path <- file.path(dataset_path, sidecar_name)
  
  if (file.exists(sidecar_path) && !isTRUE(overwrite)) {
    .dp_stop("dp_write_dataset_meta(): sidecar exists and overwrite=FALSE: %s", sidecar_path)
  }
  
  # Minimal payload; metadata is the important part
  meta_dt <- data.table::data.table(dp_meta = 1L)
  tab <- arrow::as_arrow_table(meta_dt)
  
  if (!is.list(kv)) .dp_stop("dp_write_dataset_meta(): `kv` must be a named list.")
  if (!length(names(kv) %||% character())) .dp_stop("dp_write_dataset_meta(): `kv` must be a named list (has no names).")
  
  if (!is.null(tab$ReplaceSchemaMetadata) && is.function(tab$ReplaceSchemaMetadata)) {
    tab <- tab$ReplaceSchemaMetadata(kv)
  } else {
    tab$metadata <- kv
  }
  
  arrow::write_parquet(tab, sink = sidecar_path, compression = compression)
  invisible(sidecar_path)
}

# -----------------------------------------------------------------------------
#' Read Dataplane dataset metadata sidecar
#'
#' Reads a dataset directory for a known sidecar metadata file (e.g., `dp_meta.parquet`)
#' and returns parsed Dataplane metadata.
#'
#' @param dataset_path Dataset directory path.
#' @param parse Logical. If `TRUE`, parse serialized payloads into `spec_dt`/`audit_dt`.
#' @param preferred_prefix Preferred metadata prefix (default `"dp"`).
#' @param sidecar_names Optional vector of sidecar names to look for.
#'
#' @return Same structure as [dp_read_meta()], plus `sidecar_path`.
#' @export
dp_read_dataset_meta <- function(dataset_path, parse = TRUE, preferred_prefix = "dp", sidecar_names = NULL) {
  .dp_require("arrow")
  
  if (is.null(sidecar_names)) {
    sidecar_names <- c(
      paste0(preferred_prefix, "_meta.parquet"),
      paste0("_", preferred_prefix, "_meta.parquet"),
      paste0(preferred_prefix, "_metadata.parquet"),
      paste0("_", preferred_prefix, "_metadata.parquet")
    )

    sidecar_names <- unique(sidecar_names)
  }
  
  candidates <- file.path(dataset_path, sidecar_names)
  candidates <- candidates[file.exists(candidates)]
  
  if (!length(candidates)) {
    out <- .dp_meta_unpack_kv(list(), preferred_prefix = preferred_prefix, parse = FALSE)
    out$sidecar_path <- NA_character_
    out$spec_dt <- NULL
    out$audit_dt <- NULL
    return(out)
  }
  
  meta <- dp_read_meta(
    path = candidates[1],
    parse = parse,
    preferred_prefix = preferred_prefix
  )
  meta$sidecar_path <- candidates[1]
  meta
}


# =============================================================================
# 7) Public I/O: dp_write() / dp_read()
# =============================================================================

# -----------------------------------------------------------------------------
#' Write a Parquet file with Dataplane metadata
#'
#' Writes `x` to a Parquet file and embeds Dataplane metadata in Parquet schema
#' key/value pairs. Optional behaviors:
#' - tag missing unit attributes before writing (in-memory)
#' - validate unit attributes against a spec (warning by default)
#' - encode selected numeric columns into integer storage columns
#'
#' @param x A `data.frame` or `data.table`.
#' @param path Output `.parquet` file path.
#' @param spec Either a `dp_spec` or one of `"metric"`, `"imperial"` (in which case a
#'   default spec is generated from column names).
#' @param mode `"document"` or `"validate"`. If `"validate"`, runs [dp_check_units()]
#'   before writing (warns by default).
#' @param scale Logical. If `TRUE`, apply [dp_scale_encode()] at write time.
#' @param suffix Suffix for storage columns (default `"_i"`).
#' @param drop_original Logical. If `TRUE` and `scale=TRUE`, drop base columns after encoding.
#' @param tag_missing_units Logical. If `TRUE`, apply [dp_tag_units()] before writing.
#' @param prefix Metadata key prefix (default `"dp"`).
#' @param compression Parquet compression (default `"zstd"`).
#' @param unit_attr Unit attribute name to use (default `"units"`).
#' @param warn_max Passed to [dp_check_units()].
#' @param ... Passed to [arrow::write_parquet()].
#'
#' @return Invisibly returns a list containing `path`, `spec`, `audit`, and `kv`.
#' @export
#'
#' @examples
#' \dontrun{
#' library(data.table)
#' dt <- data.table(temp = c(25.12, 25.44), rh = c(55, 52))
#' sp <- dp_spec_default(dt, "metric")
#' sp <- dp_set_scale(sp, "temp", 100)
#' out <- tempfile(fileext = ".parquet")
#' dp_write(dt, out, spec = sp, scale = TRUE)
#' res <- dp_read(out, attach_units = "declared")
#' res$dt
#' }
dp_write <- function(
    x,
    path,
    spec = c("metric", "imperial"),
    mode = c("document", "validate"),
    scale = FALSE,
    suffix = "_i",
    drop_original = FALSE,
    tag_missing_units = FALSE,
    prefix = "dp",
    compression = "zstd",
    unit_attr = "units",
    warn_max = 8L,
    ...
) {
  .dp_require("arrow")
  .dp_require("data.table")
  
  mode <- match.arg(mode)
  
  dt <- if (data.table::is.data.table(x)) data.table::copy(x) else data.table::as.data.table(x)
  
  if (!is.character(path) || length(path) != 1L || !nzchar(path)) {
    .dp_stop("dp_write(): `path` must be a non-empty file path.")
  }
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  # ---- spec ----
  sp <- if (inherits(spec, "dp_spec")) {
    spec
  } else {
    spec <- match.arg(spec)
    dp_spec_default(dt, declared_system = spec, include_unmatched = TRUE)
  }
  sp <- .dp_spec_resolve_declared_units(sp)
  
  # ---- optional unit tagging ----
  did_tag <- NULL
  if (isTRUE(tag_missing_units)) {
    tag <- dp_tag_units(dt, sp, only_missing = TRUE, system = "declared", unit_attr = unit_attr)
    dt <- tag$dt
    did_tag <- tag$did_tag
  }
  
  # ---- optional validation ----
  if (identical(mode, "validate")) {
    dp_check_units(dt, sp, system = "declared", unit_attr = unit_attr, warn_max = as.integer(warn_max), stop_on_any = FALSE)
  }
  
  # ---- optional scaling ----
  enc <- NULL
  if (isTRUE(scale)) {
    enc <- dp_scale_encode(dt, sp, suffix = suffix, drop_original = isTRUE(drop_original), round_fn = "round")
    dt <- enc$dt
  }
  
  # ---- build audit (useful for read/decode) ----
  f <- data.table::copy(sp$fields)
  for (nm in c("concept","declared_units","metric_units","imperial_units","writer_scale",
               "encoding","encoding_scale","encoding_base_units")) {
    if (!(nm %in% names(f))) f[, (nm) := NA]
  }
  
  audit <- f[, .(
    column              = as.character(column),
    concept             = as.character(concept),
    declared_units      = as.character(declared_units),
    metric_units        = as.character(metric_units),
    imperial_units      = as.character(imperial_units),
    writer_scale        = suppressWarnings(as.numeric(writer_scale)),
    encoding            = as.character(encoding),
    encoding_scale      = suppressWarnings(as.numeric(encoding_scale)),
    encoding_base_units = as.character(encoding_base_units)
  )]
  
  audit[, `:=`(
    storage_column = data.table::fifelse(
      isTRUE(scale) &
        (is.na(encoding) | encoding == "none") &
        is.finite(writer_scale) & writer_scale != 0,
      paste0(column, suffix),
      NA_character_
    ),
    did_scale      = FALSE,
    did_tag        = FALSE,
    observed_units = NA_character_,
    mode           = as.character(mode),
    scale_enabled  = isTRUE(scale),
    drop_original  = isTRUE(drop_original),
    suffix         = as.character(suffix)
  )]
  
  if (!is.null(enc) && !is.null(enc$scale_map) && nrow(enc$scale_map)) {
    audit[enc$scale_map, did_scale := i.did_scale, on = .(column)]
  }
  
  if (!is.null(did_tag) && nrow(did_tag)) {
    audit[did_tag, `:=`(
      did_tag = i.did_tag,
      observed_units = i.previous_units
    ), on = .(column)]
  } else {
    dt_cols <- names(dt)
    obs <- vapply(dt_cols, function(nm) .dp_unit_attr_get(dt[[nm]], unit_attr = unit_attr), character(1))
    audit[match(dt_cols, audit$column), observed_units := obs]
  }
  
  # ---- metadata kv ----
  keys <- .dp_meta_keys(prefix)
  extra <- .dp_kv_set(
    keys$writer,       "dp_write",
    keys$compression,  as.character(compression)[1],
    keys$write_format, "parquet"
  )
  kv <- .dp_meta_pack_kv(spec = sp, audit_dt = audit, extra = extra, prefix = prefix)
  
  # ---- write with schema metadata ----
  tab <- arrow::as_arrow_table(dt)
  if (!is.null(tab$ReplaceSchemaMetadata) && is.function(tab$ReplaceSchemaMetadata)) {
    tab <- tab$ReplaceSchemaMetadata(kv)
  } else {
    tab$metadata <- kv
  }
  
  arrow::write_parquet(tab, sink = path, compression = compression, ...)
  
  invisible(list(path = path, spec = sp, audit = audit, kv = kv))
}

# -----------------------------------------------------------------------------
#' Read a Parquet file written with Dataplane metadata
#'
#' Reads a Parquet file and returns both the data and parsed Dataplane metadata.
#' Optional behaviors:
#' - decode scaled integer storage columns back to numeric columns
#' - attach declared unit attributes using the stored spec
#'
#' @param path Path to a Parquet file.
#' @param decode_scaled Logical. If `TRUE`, decode scaled columns using the audit table.
#' @param keep_storage Logical. If `TRUE`, keep storage columns after decoding.
#' @param attach_units `"declared"` or `"none"`. If `"declared"`, attach unit attributes
#'   based on the stored spec.
#' @param preferred_prefix Preferred metadata prefix (default `"dp"`).
#' @param unit_attr Unit attribute name (default `"units"`).
#' @param ... Passed to [arrow::read_parquet()].
#'
#' @return A list with:
#' - `dt`: a `data.table`
#' - `meta`: parsed metadata from [dp_read_meta()]
#' @export
dp_read <- function(
    path,
    decode_scaled = TRUE,
    keep_storage = TRUE,
    attach_units = c("declared", "none"),
    preferred_prefix = "dp",
    unit_attr = "units",
    ...
) {
  .dp_require("arrow")
  .dp_require("data.table")
  
  attach_units <- match.arg(attach_units)
  
  meta <- dp_read_meta(
    path = path,
    parse = TRUE,
    preferred_prefix = preferred_prefix
  )
  
  dt <- arrow::read_parquet(path, as_data_frame = TRUE, ...)
  dt <- .dp_dt(dt)
  
  if (isTRUE(decode_scaled) && !is.null(meta$audit_dt) && nrow(meta$audit_dt)) {
    dt <- dp_scale_decode(dt, meta$audit_dt, keep_storage = isTRUE(keep_storage), overwrite = FALSE)
  }
  
  if (attach_units == "declared" && !is.null(meta$spec_dt) && nrow(meta$spec_dt)) {
    ds  <- meta$kv[[meta$keys$declared_system]] %||% "custom"
    ver <- meta$kv[[meta$keys$spec_version]] %||% "2"
    
    sp <- .dp_spec_new(
      fields_dt = meta$spec_dt,
      declared_system = as.character(ds)[1],
      version = as.character(ver)[1]
    )
    sp <- .dp_spec_resolve_declared_units(sp)
    
    dt <- dp_tag_units(dt, sp, only_missing = TRUE, system = "declared", unit_attr = unit_attr)$dt
  }
  
  list(dt = dt, meta = meta)
}


# =============================================================================
# 8) Public I/O: dp_open() / dp_write_dataset()
# =============================================================================

.dp_dataset_factory_options <- function(ignore_prefixes = c("_dp_"), exclude_invalid_files = TRUE) {
  .dp_require("arrow")
  
  ignore_prefixes <- as.character(ignore_prefixes)
  ignore_prefixes <- ignore_prefixes[!is.na(ignore_prefixes) & nzchar(ignore_prefixes)]
  if (!length(ignore_prefixes)) ignore_prefixes <- NULL
  
  if (exists("dataset_factory_options", where = asNamespace("arrow"), mode = "function")) {
    return(arrow::dataset_factory_options(
      selector_ignore_prefixes = ignore_prefixes,
      exclude_invalid_files = exclude_invalid_files
    ))
  }
  
  list(selector_ignore_prefixes = ignore_prefixes, exclude_invalid_files = exclude_invalid_files)
}

# -----------------------------------------------------------------------------
#' Open an Arrow Dataset and (optionally) read Dataplane sidecar metadata
#'
#' Opens a dataset directory via [arrow::open_dataset()] and optionally reads a
#' sidecar metadata file (e.g., `dp_meta.parquet`).
#'
#' @param path Dataset directory path.
#' @param metadata `"sidecar"` or `"none"`. If `"sidecar"`, also read dataset metadata.
#' @param ignore_prefixes File prefixes to ignore when opening dataset (default includes `_dp_`).
#' @param exclude_invalid_files Passed to Arrow dataset factory options.
#' @param preferred_prefix Preferred metadata prefix (default `"dp"`).
#' @param ... Passed to [arrow::open_dataset()].
#'
#' @return If `metadata="none"`, an Arrow Dataset. If `metadata="sidecar"`, a list:
#' - `ds`: Arrow Dataset
#' - `meta`: parsed sidecar metadata
#' @export
dp_open <- function(
    path,
    metadata = c("sidecar", "none"),
    ignore_prefixes = c("_dp_"),
    exclude_invalid_files = TRUE,
    preferred_prefix = "dp",
    ...
) {
  .dp_require("arrow")
  
  metadata <- match.arg(metadata)
  fo <- .dp_dataset_factory_options(ignore_prefixes = ignore_prefixes, exclude_invalid_files = exclude_invalid_files)
  
  fml <- names(formals(arrow::open_dataset))
  if ("factory_options" %in% fml) {
    ds <- arrow::open_dataset(path, factory_options = fo, ...)
  } else {
    ds <- arrow::open_dataset(path, ...)
  }
  
  if (metadata == "none") return(ds)
  
  meta <- dp_read_dataset_meta(
    dataset_path = path,
    parse = TRUE,
    preferred_prefix = preferred_prefix
  )
  
  list(ds = ds, meta = meta)
}

# -----------------------------------------------------------------------------
#' Write an Arrow Dataset and Dataplane sidecar metadata
#'
#' Writes an Arrow Dataset (directory of Parquet files) and writes a Dataplane
#' sidecar metadata Parquet file (default: `dp_meta.parquet`).
#'
#' Important: `scale_document=TRUE` documents intended storage columns (based on
#' `writer_scale`) but does not transform the dataset data. For actual scaling,
#' use [dp_write()] per-file or pre-transform the table before dataset writing.
#'
#' @param x An Arrow Table, RecordBatchReader, Dataset, or object acceptable to [arrow::write_dataset()].
#' @param path Dataset directory path.
#' @param spec Either a `dp_spec` or `"metric"`/`"imperial"` to create a default spec.
#' @param scale_document Logical. If `TRUE`, document intended storage columns in metadata.
#' @param suffix Storage suffix to document (default `"_i"`).
#' @param prefix Metadata prefix (default `"dp"`).
#' @param sidecar_overwrite Logical. Overwrite sidecar if present.
#' @param sidecar_compression Compression used for the sidecar Parquet file.
#' @param write_format Dataset format (currently only `"parquet"` supported here).
#' @param partitioning Optional partitioning passed to [arrow::write_dataset()].
#' @param existing_data_behavior Optional passed to [arrow::write_dataset()].
#' @param ... Additional args passed to [arrow::write_dataset()].
#'
#' @return Invisibly returns a list including dataset path and sidecar path.
#' @export
dp_write_dataset <- function(
    x,
    path,
    spec = c("metric", "imperial"),
    scale_document = FALSE,
    suffix = "_i",
    prefix = "dp",
    sidecar_overwrite = TRUE,
    sidecar_compression = "zstd",
    write_format = "parquet",
    partitioning = NULL,
    existing_data_behavior = NULL,
    ...
) {
  .dp_require("arrow")
  .dp_require("data.table")
  
  # ---- 1) Determine dataset columns without collecting ----
  cols <- NULL
  
  cols <- tryCatch({
    if (!is.null(x$schema) && !is.null(x$schema$names)) x$schema$names else NULL
  }, error = function(e) NULL)
  
  if (is.null(cols)) {
    cols <- tryCatch({
      if (!is.null(x$schema) && is.function(x$schema$names)) x$schema$names() else NULL
    }, error = function(e) NULL)
  }
  
  if (is.null(cols)) {
    cols <- tryCatch({
      if (!is.null(x$names)) x$names else NULL
    }, error = function(e) NULL)
  }
  
  if (is.null(cols)) {
    cols <- tryCatch({
      if (is.function(x$names)) x$names() else NULL
    }, error = function(e) NULL)
  }
  
  if (is.null(cols) || !length(cols)) {
    .dp_stop("dp_write_dataset(): could not determine schema column names without collecting.")
  }
  
  proxy <- data.table::data.table()
  for (nm in cols) proxy[[nm]] <- logical(0)
  
  # ---- 2) Resolve spec ----
  sp <- if (inherits(spec, "dp_spec")) {
    spec
  } else {
    spec <- match.arg(spec)
    dp_spec_default(proxy, declared_system = spec, include_unmatched = TRUE)
  }
  sp <- .dp_spec_resolve_declared_units(sp)
  
  # ---- 3) Build dataset-level audit (documentation-only) ----
  f <- data.table::copy(sp$fields)
  for (nm in c("concept","declared_units","metric_units","imperial_units","writer_scale",
               "encoding","encoding_scale","encoding_base_units")) {
    if (!(nm %in% names(f))) f[, (nm) := NA]
  }
  
  audit <- f[, .(
    column              = as.character(column),
    concept             = as.character(concept),
    declared_units      = as.character(declared_units),
    metric_units        = as.character(metric_units),
    imperial_units      = as.character(imperial_units),
    writer_scale        = suppressWarnings(as.numeric(writer_scale)),
    encoding            = as.character(encoding),
    encoding_scale      = suppressWarnings(as.numeric(encoding_scale)),
    encoding_base_units = as.character(encoding_base_units)
  )]
  
  audit[, `:=`(
    storage_column = data.table::fifelse(
      isTRUE(scale_document) &
        (is.na(encoding) | encoding == "none") &
        is.finite(writer_scale) & writer_scale != 0,
      paste0(column, suffix),
      NA_character_
    ),
    did_scale      = FALSE,
    did_tag        = FALSE,
    observed_units = NA_character_,
    mode           = "dataset_document",
    scale_enabled  = isTRUE(scale_document),
    drop_original  = NA,
    suffix         = as.character(suffix)
  )]
  
  # ---- 4) Write dataset itself ----
  fmt <- tolower(as.character(write_format)[1])
  if (!fmt %in% c("parquet")) {
    .dp_stop("dp_write_dataset(): unsupported write_format='%s' (only 'parquet').", fmt)
  }
  
  write_args <- list(
    dataset      = x,
    path         = path,
    format       = "parquet",
    partitioning = partitioning
  )
  if (!is.null(existing_data_behavior)) write_args$existing_data_behavior <- existing_data_behavior
  write_args <- c(write_args, list(...))
  
  do.call(arrow::write_dataset, write_args)
  
  # ---- 5) Write sidecar meta parquet (schema metadata) ----
  keys <- .dp_meta_keys(prefix)
  
  extra <- .dp_kv_set(
    keys$writer,                 "dp_write_dataset",
    keys$sidecar_compression,    as.character(sidecar_compression)[1],
    keys$write_format,           as.character(write_format)[1],
    keys$partitioning,           if (is.null(partitioning)) NA_character_ else paste(partitioning, collapse = ","),
    keys$existing_data_behavior, if (is.null(existing_data_behavior)) NA_character_ else as.character(existing_data_behavior)[1]
  )
  
  kv <- .dp_meta_pack_kv(spec = sp, audit_dt = audit, extra = extra, prefix = prefix)
  
  dp_write_dataset_meta(
    dataset_path = path,
    kv = kv,
    prefix = prefix,
    overwrite = isTRUE(sidecar_overwrite),
    compression = sidecar_compression,
    sidecar_name = paste0(prefix, "_meta.parquet")
  )
  
  invisible(list(
    path = path,
    sidecar_path = file.path(path, paste0(prefix, "_meta.parquet")),
    sidecar_written = TRUE,
    spec = sp,
    audit = audit,
    kv = kv
  ))
}


# =============================================================================
# 9) Detection helpers (file or dataset directory)
# =============================================================================

.dp_detect_from_keys <- function(
    keys,
    prefixes = c("dp"),
    min_score_detect = 2L,
    min_score_validate = 4L
) {
  keys <- as.character(keys)
  keys <- keys[!is.na(keys) & nzchar(keys)]
  if (!length(keys)) {
    return(list(
      is_dp = FALSE,
      score = 0L,
      confidence = "none",
      prefix_detected = NA_character_,
      evidence = character(),
      reason = "no_metadata_keys"
    ))
  }
  
  prefixes <- unique(as.character(prefixes))
  prefixes <- prefixes[!is.na(prefixes) & nzchar(prefixes)]
  if (!length(prefixes)) prefixes <- c("dp")
  
  sep <- "([._:])"
  pref_pat <- paste0("^(", paste(prefixes, collapse = "|"), ")", sep)
  pref_hits <- grepl(pref_pat, keys, perl = TRUE)
  pref_keys <- keys[pref_hits]
  
  if (!length(pref_keys)) {
    return(list(
      is_dp = FALSE,
      score = 0L,
      confidence = "none",
      prefix_detected = NA_character_,
      evidence = character(),
      reason = "no_dp_like_prefix_keys"
    ))
  }
  
  token_pat <- "(spec_b64|audit_b64|spec_version|declared_system|writer|written_at|scale|suffix|compression|partitioning)"
  token_hits <- grepl(token_pat, pref_keys, ignore.case = TRUE, perl = TRUE)
  token_keys <- pref_keys[token_hits]
  
  score <- 0L
  score <- score + min(3L, length(pref_keys))
  score <- score + min(3L, length(token_keys))
  
  prefix_detected <- NA_character_
  for (p in prefixes) {
    if (any(grepl(paste0("^", p, sep), pref_keys, perl = TRUE))) {
      prefix_detected <- p
      break
    }
  }
  
  confidence <- if (score >= min_score_validate) {
    "high"
  } else if (score >= min_score_detect) {
    "medium"
  } else {
    "low"
  }
  
  list(
    is_dp = score >= min_score_detect,
    score = score,
    confidence = confidence,
    prefix_detected = prefix_detected,
    evidence = unique(c(utils::head(token_keys, 8), utils::head(pref_keys, 8))),
    reason = sprintf("prefix_keys=%d; token_keys=%d; score=%d", length(pref_keys), length(token_keys), score)
  )
}

.dp_detect_parquet_file <- function(path, preferred_prefix = "dp", mode = c("detect", "validate")) {
  .dp_require("arrow")
  
  mode <- match.arg(mode)
  path <- normalizePath(path, winslash = "/", mustWork = TRUE)
  
  kv <- tryCatch(.dp_read_parquet_kv(path), error = function(e) list())
  prefixes <- unique(c(preferred_prefix))
  
  out <- .dp_detect_from_keys(names(kv), prefixes = prefixes, min_score_detect = 2L, min_score_validate = 4L)
  
  if (mode == "validate") {
    out$is_dp <- isTRUE(out$score >= 4L)
    if (!out$is_dp && out$confidence == "medium") out$confidence <- "low"
  }
  
  out$path <- path
  out$kind <- "file"
  out
}

.dp_detect_dataset_dir <- function(
    dir,
    preferred_prefix = "dp",
    mode = c("detect", "validate"),
    sidecar_names = NULL,
    scan_n = 5L,
    recursive = TRUE
) {
  .dp_require("arrow")
  
  mode <- match.arg(mode)
  dir <- normalizePath(dir, winslash = "/", mustWork = TRUE)
  
  # 1) Prefer sidecar
  if (is.null(sidecar_names)) {
    sidecar_names <- unique(c(
      paste0(preferred_prefix, "_meta.parquet"),
      paste0("_", preferred_prefix, "_meta.parquet"),
      paste0(preferred_prefix, "_metadata.parquet"),
      paste0("_", preferred_prefix, "_metadata.parquet")
    ))
  }
  
  sidecar_paths <- file.path(dir, sidecar_names)
  sidecar_paths <- sidecar_paths[file.exists(sidecar_paths)]
  
  if (length(sidecar_paths)) {
    sc <- sidecar_paths[1]
    file_res <- .dp_detect_parquet_file(
      path = sc,
      preferred_prefix = preferred_prefix,
      mode = mode
    )
    file_res$kind <- "dataset_sidecar"
    file_res$dataset_dir <- dir
    file_res$sidecar_path <- sc
    return(file_res)
  }
  
  # 2) Else sample parquet files
  files <- list.files(dir, pattern = "\\.parquet$", full.names = TRUE, recursive = isTRUE(recursive))
  finfo <- file.info(files)
  if (!is.null(finfo$isdir)) files <- files[!isTRUE(finfo$isdir)]
  
  if (!length(files)) {
    return(list(
      is_dp = FALSE,
      score = 0L,
      confidence = "none",
      prefix_detected = NA_character_,
      evidence = character(),
      reason = "no_parquet_files_found",
      kind = "dataset_dir",
      dataset_dir = dir
    ))
  }
  
  scan_n <- max(1L, as.integer(scan_n))
  files2 <- utils::head(files, scan_n)
  
  res_list <- lapply(files2, function(fp) {
    .dp_detect_parquet_file(
      path = fp,
      preferred_prefix = preferred_prefix,
      mode = mode
    )
  })
  
  votes <- vapply(res_list, function(z) isTRUE(z$is_dp), logical(1))
  dp_count <- sum(votes, na.rm = TRUE)
  n <- length(votes)
  
  is_dp_dir <- dp_count >= ceiling(n / 2)
  first_hit <- which(votes)[1]
  evidence <- if (!is.na(first_hit)) res_list[[first_hit]]$evidence else character()
  
  list(
    is_dp = is_dp_dir,
    score = dp_count,
    confidence = if (is_dp_dir) "medium" else "low",
    prefix_detected = if (!is.na(first_hit)) res_list[[first_hit]]$prefix_detected else NA_character_,
    evidence = evidence,
    reason = sprintf("sampled=%d; dp_votes=%d; majority_rule=%s", n, dp_count, is_dp_dir),
    kind = "dataset_dir",
    dataset_dir = dir,
    sampled_files = files2
  )
}

# -----------------------------------------------------------------------------
#' Detect whether a file or dataset contains Dataplane metadata
#'
#' Works on a single Parquet file or a dataset directory. For dataset directories,
#' this function prefers a Dataplane sidecar (e.g., `dp_meta.parquet`) and otherwise
#' samples Parquet files and applies a majority rule.
#'
#' @param path Path to a Parquet file or dataset directory.
#' @param preferred_prefix Preferred metadata prefix (default `"dp"`).
#' @param mode `"detect"` or `"validate"`. `"validate"` uses a stricter threshold.
#' @param scan_n For dataset directories without a sidecar, number of Parquet files to sample.
#' @param recursive For dataset directories, whether to scan recursively for Parquet files.
#'
#' @return A list describing detection outcome (confidence, evidence keys, etc.).
#' @export
#'
#' @examples
#' \dontrun{
#' dp_detect("file.parquet")
#' dp_detect("dataset_dir")
#' }
dp_detect <- function(
    path,
    preferred_prefix = "dp",
    mode = c("detect", "validate"),
    scan_n = 5L,
    recursive = TRUE
) {
  mode <- match.arg(mode)
  
  if (!file.exists(path)) {
    stop(sprintf("Path does not exist: %s", path), call. = FALSE)
  }
  
  if (isTRUE(file.info(path)$isdir)) {
    return(.dp_detect_dataset_dir(
      dir = path,
      preferred_prefix = preferred_prefix,
      mode = mode,
      scan_n = scan_n,
      recursive = recursive
    ))
  }
  
  .dp_detect_parquet_file(
    path = path,
    preferred_prefix = preferred_prefix,
    mode = mode
  )
}

# -----------------------------------------------------------------------------
#' Print a Dataplane detection result
#'
#' Convenience printer for objects returned by [dp_detect()].
#'
#' @param x Detection result list from [dp_detect()].
#'
#' @return The input `x` (invisibly).
#' @export
dp_print_detect <- function(x) {
  cat("\n")
  cat("dataplane detection\n")
  cat("-------------------\n")
  cat("is_dp:           ", x$is_dp, "\n", sep = "")
  cat("kind:            ", x$kind %||% NA_character_, "\n", sep = "")
  cat("confidence:      ", x$confidence %||% NA_character_, "\n", sep = "")
  cat("prefix_detected: ", x$prefix_detected %||% NA_character_, "\n", sep = "")
  cat("reason:          ", x$reason %||% NA_character_, "\n", sep = "")
  if (!is.null(x$path))         cat("path:            ", x$path, "\n", sep = "")
  if (!is.null(x$dataset_dir))  cat("dataset_dir:     ", x$dataset_dir, "\n", sep = "")
  if (!is.null(x$sidecar_path)) cat("sidecar_path:    ", x$sidecar_path, "\n", sep = "")
  if (!is.null(x$evidence) && length(x$evidence)) {
    cat("\nEvidence keys (sample):\n")
    cat(paste0("  - ", utils::head(x$evidence, 10)), sep = "\n")
    cat("\n")
  }
  invisible(x)
}
