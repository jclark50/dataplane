# =============================================================================
# dataplane v2 — Parquet + Dataset metadata + Scaling (public-friendly)
# =============================================================================
# Public API (no dp_ prefix; call via dataplane::write_parquet(), etc.)
# - write_parquet(), read_parquet()
# - open_dataset(), write_dataset()
# - spec_default(), set_units(), set_scale()
# - tag_units(), check_units()
# - scale_encode(), scale_decode()
# - read_parquet_meta(), read_dataset_meta(), write_dataset_meta()
# - detect(), print_detect()
#
# Notes
# - Internal helpers remain prefixed as .dp_* (not intended for callers).
# - No special column-name assumptions (no "scaled10" or similar).
# - Metadata payload is stored as gzip+base64 of serialized objects:
#     <prefix>:spec_b64  and  <prefix>:audit_b64
#   (plus a few scalar keys like <prefix>:declared_system, <prefix>:spec_version, etc.)
# =============================================================================


# =============================================================================
# 0) Small utilities (internal)
# =============================================================================

`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0L || is.na(x[[1]])) y else x[[1]]
}

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
# 1) Metadata keys + kv pack/unpack (single system)
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

# Small helper to build dynamic kv pairs: kv_set(key1,val1,key2,val2,...)
kv_set <- function(...) {
  args <- list(...)
  if (!length(args)) return(list())
  if (length(args) %% 2 != 0) {
    stop("kv_set(): must supply key1, value1, key2, value2, ...", call. = FALSE)
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

.dp_meta_pick_prefix <- function(kv_names, preferred = "dp", fallbacks = c("klimo")) {
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
  kv[[keys$writer]]         <- "dataplane"
  kv[[keys$written_at_utc]] <- format(Sys.time(), tz = "UTC", usetz = TRUE)
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

.dp_meta_unpack_kv <- function(kv, preferred_prefix = "dp", legacy_prefixes = c("klimo"), parse = TRUE) {
  .dp_require("data.table")
  .dp_require("jsonlite")
  
  kv <- kv %||% list()
  if (!is.list(kv)) .dp_stop(".dp_meta_unpack_kv(): kv must be a list.")
  
  prefix <- .dp_meta_pick_prefix(names(kv), preferred = preferred_prefix, fallbacks = legacy_prefixes)
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
# 2) Spec object (dp_spec): declared units + scaling intent (no column-name heuristics)
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
    .dp_stop("Unsupported encoding value(s): %s. Allowed: 'none', 'scaled_int'.",
             paste(bad_enc, collapse = ", "))
  }
  
  out <- list(fields = fields_dt, declared_system = declared_system, version = as.character(version))
  class(out) <- "dp_spec"
  out
}

#' @export
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
# Public: spec_default()
# -----------------------------------------------------------------------------
spec_default <- function(x, declared_system = c("metric", "imperial"), include_unmatched = TRUE) {
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
# Public: set_units() and set_scale()
# -----------------------------------------------------------------------------
set_units <- function(spec, columns, metric_units = NULL, imperial_units = NULL, declared_units = NULL) {
  .dp_require("data.table")
  if (!inherits(spec, "dp_spec")) .dp_stop("set_units(): `spec` must be a <dp_spec>.")
  dt <- data.table::copy(spec$fields)
  
  stopifnot(is.character(columns), length(columns) >= 1L)
  hit <- .dp_norm_name(columns)
  dt[, .hit := column_norm %in% hit]
  
  if (!is.null(metric_units)) {
    if (!.dp_is_scalar_chr(metric_units)) .dp_stop("set_units(): metric_units must be a character scalar.")
    dt[.hit == TRUE, metric_units := metric_units]
  }
  if (!is.null(imperial_units)) {
    if (!.dp_is_scalar_chr(imperial_units)) .dp_stop("set_units(): imperial_units must be a character scalar.")
    dt[.hit == TRUE, imperial_units := imperial_units]
  }
  if (!is.null(declared_units)) {
    if (!.dp_is_scalar_chr(declared_units)) .dp_stop("set_units(): declared_units must be a character scalar.")
    dt[.hit == TRUE, declared_units := declared_units]
  }
  
  dt[, .hit := NULL]
  spec$fields <- dt
  spec
}

set_scale <- function(spec, columns, writer_scale) {
  .dp_require("data.table")
  if (!inherits(spec, "dp_spec")) .dp_stop("set_scale(): `spec` must be a <dp_spec>.")
  if (length(writer_scale) != 1L) .dp_stop("set_scale(): writer_scale must be a scalar numeric (or NA).")
  
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
# Public: tag_units()
# -----------------------------------------------------------------------------
tag_units <- function(
    dt,
    spec,
    only_missing = TRUE,
    system = c("declared", "metric", "imperial"),
    unit_attr = "units"
) {
  .dp_require("data.table")
  dt0 <- .dp_dt(dt)
  
  if (!inherits(spec, "dp_spec") || is.null(spec$fields) || !data.table::is.data.table(spec$fields)) {
    .dp_stop("tag_units(): `spec` must be a <dp_spec> with data.table `spec$fields`.")
  }
  
  f <- data.table::copy(spec$fields)
  if (!("column" %in% names(f))) .dp_stop("tag_units(): `spec$fields` must include `column`.")
  
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
# Public: check_units()
# -----------------------------------------------------------------------------
check_units <- function(
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
    .dp_stop("check_units(): `spec` must be a <dp_spec> with data.table `spec$fields`.")
  }
  
  f <- data.table::copy(spec$fields)
  if (!("column" %in% names(f))) .dp_stop("check_units(): `spec$fields` must include `column`.")
  
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
          "check_units(): found unit/schema issues (showing ", nrow(show), " of ", nrow(probs), "):\n",
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
      .dp_stop("check_units(): validation failed (stop_on_any=TRUE).")
    }
  }
  
  invisible(audit)
}

# =============================================================================
# 4) Scaling: encode to int storage columns + decode back (opt-in)
# =============================================================================

scale_encode <- function(
    dt,
    spec,
    suffix = "_i",
    drop_original = FALSE,
    round_fn = c("round", "floor", "ceiling")
) {
  .dp_require("data.table")
  dt <- data.table::copy(.dp_dt(dt))
  round_fn <- match.arg(round_fn)
  
  if (!inherits(spec, "dp_spec")) .dp_stop("scale_encode(): `spec` must be a <dp_spec>.")
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

scale_decode <- function(dt, audit_dt, keep_storage = TRUE, overwrite = FALSE) {
  .dp_require("data.table")
  dt <- data.table::copy(.dp_dt(dt))
  audit_dt <- .dp_dt(audit_dt)
  
  need <- c("column", "writer_scale", "storage_column", "did_scale")
  if (!all(need %in% names(audit_dt))) return(dt)
  
  plan <- audit_dt[
    isTRUE(did_scale) &
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
      if (!all(c("key","value") %in% names(kv2))) return(list())
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
# Public: read_parquet_meta()
# -----------------------------------------------------------------------------
read_parquet_meta <- function(path, parse = TRUE, preferred_prefix = "dp", legacy_prefixes = c("klimo")) {
  kv <- .dp_read_parquet_kv(path)
  .dp_meta_unpack_kv(kv, preferred_prefix = preferred_prefix, legacy_prefixes = legacy_prefixes, parse = parse)
}

# =============================================================================
# 6) Dataset sidecar meta (recommended for Arrow Datasets)
# =============================================================================

# -----------------------------------------------------------------------------
# Public: write_dataset_meta()
# - Writes a tiny Parquet file in dataset_path (default: "<prefix>_meta.parquet")
# - Stores kv in schema metadata (so it can be read without collecting data)
# -----------------------------------------------------------------------------
write_dataset_meta <- function(dataset_path, kv, prefix = "dp", overwrite = TRUE, compression = "zstd", sidecar_name = NULL) {
  .dp_require("arrow")
  .dp_require("data.table")
  
  if (!dir.exists(dataset_path)) dir.create(dataset_path, recursive = TRUE, showWarnings = FALSE)
  
  if (is.null(sidecar_name)) sidecar_name <- paste0(prefix, "_meta.parquet")
  sidecar_path <- file.path(dataset_path, sidecar_name)
  
  if (file.exists(sidecar_path) && !isTRUE(overwrite)) {
    .dp_stop("write_dataset_meta(): sidecar exists and overwrite=FALSE: %s", sidecar_path)
  }
  
  # Minimal payload; metadata is the important part
  meta_dt <- data.table::data.table(dp_meta = 1L)
  tab <- arrow::as_arrow_table(meta_dt)
  
  if (!is.list(kv)) .dp_stop("write_dataset_meta(): `kv` must be a named list.")
  if (!length(names(kv) %||% character())) .dp_stop("write_dataset_meta(): `kv` must be a named list (has no names).")
  
  if (!is.null(tab$ReplaceSchemaMetadata) && is.function(tab$ReplaceSchemaMetadata)) {
    tab <- tab$ReplaceSchemaMetadata(kv)
  } else {
    tab$metadata <- kv
  }
  
  arrow::write_parquet(tab, sink = sidecar_path, compression = compression)
  invisible(sidecar_path)
}

# -----------------------------------------------------------------------------
# Public: read_dataset_meta()
# -----------------------------------------------------------------------------
read_dataset_meta <- function(dataset_path, parse = TRUE, preferred_prefix = "dp", legacy_prefixes = c("klimo"), sidecar_names = NULL) {
  .dp_require("arrow")
  
  if (is.null(sidecar_names)) {
    sidecar_names <- c(
      paste0(preferred_prefix, "_meta.parquet"),
      paste0("_", preferred_prefix, "_meta.parquet"),
      paste0(preferred_prefix, "_metadata.parquet"),
      paste0("_", preferred_prefix, "_metadata.parquet")
    )
    for (p in legacy_prefixes) {
      sidecar_names <- c(sidecar_names, paste0(p, "_meta.parquet"), paste0("_", p, "_meta.parquet"))
    }
    sidecar_names <- unique(sidecar_names)
  }
  
  candidates <- file.path(dataset_path, sidecar_names)
  candidates <- candidates[file.exists(candidates)]
  
  if (!length(candidates)) {
    out <- .dp_meta_unpack_kv(list(), preferred_prefix = preferred_prefix, legacy_prefixes = legacy_prefixes, parse = FALSE)
    out$sidecar_path <- NA_character_
    out$spec_dt <- NULL
    out$audit_dt <- NULL
    return(out)
  }
  
  meta <- read_parquet_meta(
    path = candidates[1],
    parse = parse,
    preferred_prefix = preferred_prefix,
    legacy_prefixes = legacy_prefixes
  )
  meta$sidecar_path <- candidates[1]
  meta
}

# =============================================================================
# 7) Public I/O: write_parquet() / read_parquet()
# =============================================================================

# -----------------------------------------------------------------------------
# Public: write_parquet()
# -----------------------------------------------------------------------------
write_parquet <- function(
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
    .dp_stop("write_parquet(): `path` must be a non-empty file path.")
  }
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  # ---- spec ----
  sp <- if (inherits(spec, "dp_spec")) {
    spec
  } else {
    spec <- match.arg(spec)
    spec_default(dt, declared_system = spec, include_unmatched = TRUE)
  }
  sp <- .dp_spec_resolve_declared_units(sp)
  
  # ---- optional unit tagging ----
  did_tag <- NULL
  if (isTRUE(tag_missing_units)) {
    tag <- tag_units(dt, sp, only_missing = TRUE, system = "declared", unit_attr = unit_attr)
    dt <- tag$dt
    did_tag <- tag$did_tag
  }
  
  # ---- optional validation ----
  if (identical(mode, "validate")) {
    check_units(dt, sp, system = "declared", unit_attr = unit_attr, warn_max = as.integer(warn_max), stop_on_any = FALSE)
  }
  
  # ---- optional scaling ----
  enc <- NULL
  if (isTRUE(scale)) {
    enc <- scale_encode(dt, sp, suffix = suffix, drop_original = isTRUE(drop_original), round_fn = "round")
    dt <- enc$dt
  }
  
  # ---- build audit (lightweight, useful on read/decode) ----
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
    # observed_units: what was present before tagging
    audit[did_tag, `:=`(
      did_tag = i.did_tag,
      observed_units = i.previous_units
    ), on = .(column)]
  } else {
    # still record observed units (for a quick glance)
    dt_cols <- names(dt)
    obs <- vapply(dt_cols, function(nm) .dp_unit_attr_get(dt[[nm]], unit_attr = unit_attr), character(1))
    audit[match(dt_cols, audit$column), observed_units := obs]
  }
  
  # ---- metadata kv ----
  keys <- .dp_meta_keys(prefix)
  extra <- kv_set(
    keys$writer,      "write_parquet",
    keys$compression, as.character(compression)[1],
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
# Public: read_parquet()
# -----------------------------------------------------------------------------
read_parquet <- function(
    path,
    decode_scaled = TRUE,
    keep_storage = TRUE,
    attach_units = c("declared", "none"),
    preferred_prefix = "dp",
    legacy_prefixes = c("klimo"),
    unit_attr = "units",
    ...
) {
  .dp_require("arrow")
  .dp_require("data.table")
  
  attach_units <- match.arg(attach_units)
  
  meta <- read_parquet_meta(
    path = path,
    parse = TRUE,
    preferred_prefix = preferred_prefix,
    legacy_prefixes = legacy_prefixes
  )
  
  dt <- arrow::read_parquet(path, as_data_frame = TRUE, ...)
  dt <- .dp_dt(dt)
  
  if (isTRUE(decode_scaled) && !is.null(meta$audit_dt) && nrow(meta$audit_dt)) {
    dt <- scale_decode(dt, meta$audit_dt, keep_storage = isTRUE(keep_storage), overwrite = FALSE)
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
    
    dt <- tag_units(dt, sp, only_missing = TRUE, system = "declared", unit_attr = unit_attr)$dt
  }
  
  list(dt = dt, meta = meta)
}

# =============================================================================
# 8) Public I/O: open_dataset() / write_dataset()
# =============================================================================

.dp_dataset_factory_options <- function(ignore_prefixes = c("_dp_", "_klimo_"), exclude_invalid_files = TRUE) {
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
# Public: open_dataset()
# -----------------------------------------------------------------------------
open_dataset <- function(
    path,
    metadata = c("sidecar", "none"),
    ignore_prefixes = c("_dp_", "_klimo_"),
    exclude_invalid_files = TRUE,
    preferred_prefix = "dp",
    legacy_prefixes = c("klimo"),
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
  
  meta <- read_dataset_meta(
    dataset_path = path,
    parse = TRUE,
    preferred_prefix = preferred_prefix,
    legacy_prefixes = legacy_prefixes
  )
  
  list(ds = ds, meta = meta)
}

# -----------------------------------------------------------------------------
# Public: write_dataset()
# - Writes Arrow Dataset + sidecar meta file "<prefix>_meta.parquet"
# - scale_document only *documents* intended storage columns; does not transform data
# -----------------------------------------------------------------------------
write_dataset <- function(
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
    .dp_stop("write_dataset(): could not determine schema column names without collecting.")
  }
  
  proxy <- data.table::data.table()
  for (nm in cols) proxy[[nm]] <- logical(0)
  
  # ---- 2) Resolve spec ----
  sp <- if (inherits(spec, "dp_spec")) {
    spec
  } else {
    spec <- match.arg(spec)
    spec_default(proxy, declared_system = spec, include_unmatched = TRUE)
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
    .dp_stop("write_dataset(): unsupported write_format='%s' (only 'parquet').", fmt)
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
  
  extra <- kv_set(
    keys$writer,                "write_dataset",
    keys$sidecar_compression,   as.character(sidecar_compression)[1],
    keys$write_format,          as.character(write_format)[1],
    keys$partitioning,          if (is.null(partitioning)) NA_character_ else paste(partitioning, collapse = ","),
    keys$existing_data_behavior, if (is.null(existing_data_behavior)) NA_character_ else as.character(existing_data_behavior)[1]
  )
  
  kv <- .dp_meta_pack_kv(spec = sp, audit_dt = audit, extra = extra, prefix = prefix)
  
  write_dataset_meta(
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
    prefixes = c("dp", "klimo"),
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
    evidence = unique(c(head(token_keys, 8), head(pref_keys, 8))),
    reason = sprintf("prefix_keys=%d; token_keys=%d; score=%d", length(pref_keys), length(token_keys), score)
  )
}

.dp_detect_parquet_file <- function(path, preferred_prefix = "dp", legacy_prefixes = c("klimo"), mode = c("detect", "validate")) {
  .dp_require("arrow")
  
  mode <- match.arg(mode)
  path <- normalizePath(path, winslash = "/", mustWork = TRUE)
  
  kv <- tryCatch(.dp_read_parquet_kv(path), error = function(e) list())
  prefixes <- unique(c(preferred_prefix, legacy_prefixes))
  
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
    legacy_prefixes = c("klimo"),
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
    for (p in legacy_prefixes) {
      sidecar_names <- unique(c(sidecar_names, paste0(p, "_meta.parquet"), paste0("_", p, "_meta.parquet")))
    }
  }
  
  sidecar_paths <- file.path(dir, sidecar_names)
  sidecar_paths <- sidecar_paths[file.exists(sidecar_paths)]
  
  if (length(sidecar_paths)) {
    sc <- sidecar_paths[1]
    file_res <- .dp_detect_parquet_file(
      path = sc,
      preferred_prefix = preferred_prefix,
      legacy_prefixes = legacy_prefixes,
      mode = mode
    )
    file_res$kind <- "dataset_sidecar"
    file_res$dataset_dir <- dir
    file_res$sidecar_path <- sc
    return(file_res)
  }
  
  # 2) Else sample parquet files
  files <- list.files(dir, pattern = "\\.parquet$", full.names = TRUE, recursive = isTRUE(recursive))
  files <- files[file.info(files)$isdir %||% rep(FALSE, length(files)) == FALSE]
  
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
  files2 <- head(files, scan_n)
  
  res_list <- lapply(files2, function(fp) {
    .dp_detect_parquet_file(
      path = fp,
      preferred_prefix = preferred_prefix,
      legacy_prefixes = legacy_prefixes,
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
# Public: detect() (file OR directory)
# -----------------------------------------------------------------------------
detect <- function(
    path,
    preferred_prefix = "dp",
    legacy_prefixes = c("klimo"),
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
      legacy_prefixes = legacy_prefixes,
      mode = mode,
      scan_n = scan_n,
      recursive = recursive
    ))
  }
  
  .dp_detect_parquet_file(
    path = path,
    preferred_prefix = preferred_prefix,
    legacy_prefixes = legacy_prefixes,
    mode = mode
  )
}

# -----------------------------------------------------------------------------
# Public: print_detect()
# -----------------------------------------------------------------------------
print_detect <- function(x) {
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
    cat(paste0("  - ", head(x$evidence, 10)), sep = "\n")
    cat("\n")
  }
  invisible(x)
}


# # =============================================================================
# # dataplane v2 — Parquet + Dataset metadata + Scaling (public-friendly)
# # =============================================================================
# # Design objectives
# # - Small, stable public API:
# #     dp_write_parquet(), dp_read_parquet(), dp_open_dataset(), dp_write_dataset()
# # - Scaling is first-class, but opt-in and explicit:
# #     scale=TRUE; drop_original=FALSE by default
# # - Units are metadata-first (document / validate), not conversion-first
# # - Arrow Datasets store metadata in a sidecar file (reliable across Arrow versions)
# # - Metadata namespace defaults to "dp:*" but reads legacy "klimo:*" seamlessly
# #
# # Notes
# # - This file is intentionally self-contained for review.
# # - In a package, split into multiple R/ files and export only the public functions.
# # =============================================================================
# 
# 
# # =============================================================================
# # 0) Dependencies + small internal utilities
# # =============================================================================
# 
# .dp_require <- function(pkg) {
#   if (!requireNamespace(pkg, quietly = TRUE)) {
#     stop(sprintf("Required package '%s' is not installed.", pkg), call. = FALSE)
#   }
# }
# 
# # -----------------------------------------------------------------------------
# # Low-level: read Parquet file key/value metadata (no table read/collect)
# # -----------------------------------------------------------------------------
# .dp_parquet_kv <- function(path) {
#   .dp_require("arrow")
#   
#   rdr <- arrow::ParquetFileReader$create(path)
#   md  <- rdr$GetFileMetaData()
#   
#   kv <- md$key_value_metadata
#   if (is.null(kv)) return(NULL)
#   
#   # Arrow returns a data.frame-like with cols key/value in most versions
#   kv <- as.data.frame(kv, stringsAsFactors = FALSE)
#   
#   if (!all(c("key", "value") %in% names(kv))) {
#     # Defensive: if something weird, attempt to coerce
#     nms <- tolower(names(kv))
#     names(kv) <- nms
#     if (!all(c("key", "value") %in% names(kv))) return(NULL)
#   }
#   
#   kv
# }
# 
# # -----------------------------------------------------------------------------
# # Heuristic scoring: does a kv table look like dp_* metadata?
# # -----------------------------------------------------------------------------
# .dp_detect_from_kv <- function(
#     kv,
#     prefixes = c("dp", "klimo"),
#     min_score_detect = 2L,
#     min_score_validate = 4L
# ) {
#   if (is.null(kv) || !nrow(kv)) {
#     return(list(
#       is_dp = FALSE,
#       score = 0L,
#       confidence = "none",
#       prefix_detected = NA_character_,
#       evidence = character(),
#       reason = "no_kv_metadata"
#     ))
#   }
#   
#   keys <- as.character(kv$key)
#   keys <- keys[!is.na(keys) & nzchar(keys)]
#   if (!length(keys)) {
#     return(list(
#       is_dp = FALSE,
#       score = 0L,
#       confidence = "none",
#       prefix_detected = NA_character_,
#       evidence = character(),
#       reason = "empty_kv_keys"
#     ))
#   }
#   
#   prefixes <- unique(as.character(prefixes))
#   prefixes <- prefixes[!is.na(prefixes) & nzchar(prefixes)]
#   if (!length(prefixes)) prefixes <- c("dp")
#   
#   # What this does:
#   # - Looks for keys that begin with dp/klimo prefixes and look like dp schema/audit metadata
#   # - Avoids false positives by requiring BOTH:
#   #     (a) prefix hit, and
#   #     (b) at least one "semantic" token like spec_version/declared_system/audit/fields/writer
#   #
#   # This is intentionally tolerant to separators: dp_spec_version, dp.spec_version, dp:spec_version, etc.
#   
#   # Prefix pattern: start-of-string + prefix + separator
#   sep <- "([._:])"
#   pref_pat <- paste0("^(", paste(prefixes, collapse = "|"), ")", sep)
#   
#   pref_hits <- grepl(pref_pat, keys, perl = TRUE)
#   pref_keys <- keys[pref_hits]
#   
#   if (!length(pref_keys)) {
#     return(list(
#       is_dp = FALSE,
#       score = 0L,
#       confidence = "none",
#       prefix_detected = NA_character_,
#       evidence = character(),
#       reason = "no_dp_like_prefix_keys"
#     ))
#   }
#   
#   # Tokens that commonly appear in dp metadata keys/fields
#   token_pat <- "(spec|spec_version|declared_system|writer|written_at|audit|fields|schema|encoding|scale|suffix)"
#   token_hits <- grepl(token_pat, pref_keys, ignore.case = TRUE, perl = TRUE)
#   token_keys <- pref_keys[token_hits]
#   
#   # Score components (simple + explainable)
#   score <- 0L
#   score <- score + min(3L, length(pref_keys))          # up to 3 points for dp-like prefix keys
#   score <- score + min(3L, length(token_keys))         # up to 3 points for semantic tokens
#   
#   # If they wrote a GeoParquet file, it may also contain "geo" metadata;
#   # that is NOT dp-specific, so we ignore it for scoring.
#   
#   # Guess which prefix is present
#   prefix_detected <- NA_character_
#   for (p in prefixes) {
#     if (any(grepl(paste0("^", p, sep), pref_keys, perl = TRUE))) {
#       prefix_detected <- p
#       break
#     }
#   }
#   
#   confidence <- if (score >= min_score_validate) {
#     "high"
#   } else if (score >= min_score_detect) {
#     "medium"
#   } else {
#     "low"
#   }
#   
#   list(
#     is_dp = score >= min_score_detect,
#     score = score,
#     confidence = confidence,
#     prefix_detected = prefix_detected,
#     evidence = unique(c(head(token_keys, 8), head(pref_keys, 8))),
#     reason = sprintf(
#       "prefix_keys=%d; token_keys=%d; score=%d",
#       length(pref_keys), length(token_keys), score
#     )
#   )
# }
# 
# 
# # -----------------------------------------------------------------------------
# # Detect dp-ness for a single parquet file
# # -----------------------------------------------------------------------------
# dp_is_dp_parquet_file <- function(
#     path,
#     preferred_prefix = "dp",
#     legacy_prefixes = c("klimo"),
#     mode = c("detect", "validate")
# ) {
#   .dp_require("arrow")
#   
#   mode <- match.arg(mode)
#   path <- normalizePath(path, winslash = "/", mustWork = TRUE)
#   
#   kv <- tryCatch(.dp_parquet_kv(path), error = function(e) NULL)
#   
#   prefixes <- unique(c(preferred_prefix, legacy_prefixes))
#   out <- .dp_detect_from_kv(
#     kv = kv,
#     prefixes = prefixes,
#     min_score_detect = 2L,
#     min_score_validate = 4L
#   )
#   
#   # In validate mode, require higher confidence/score
#   if (mode == "validate") {
#     out$is_dp <- isTRUE(out$score >= 4L)
#     if (!out$is_dp && out$confidence == "medium") out$confidence <- "low"
#   }
#   
#   out$path <- path
#   out$kind <- "file"
#   out
# }
# 
# # -----------------------------------------------------------------------------
# # Dataset directory detection:
# #  1) Prefer a sidecar metadata parquet (dp_meta.parquet, _dp_meta.parquet, etc.)
# #  2) Else sample parquet files under the directory and vote
# # -----------------------------------------------------------------------------
# dp_is_dp_dataset_dir <- function(
#     dir,
#     preferred_prefix = "dp",
#     legacy_prefixes = c("klimo"),
#     mode = c("detect", "validate"),
#     sidecar_names = NULL,
#     scan_n = 5L,
#     recursive = TRUE
# ) {
#   .dp_require("arrow")
#   
#   mode <- match.arg(mode)
#   dir <- normalizePath(dir, winslash = "/", mustWork = TRUE)
#   
#   prefixes <- unique(c(preferred_prefix, legacy_prefixes))
#   
#   # Default sidecar candidates (tuned to your earlier dp_write_dataset pattern)
#   if (is.null(sidecar_names)) {
#     sidecar_names <- unique(c(
#       paste0(preferred_prefix, "_meta.parquet"),
#       paste0("_", preferred_prefix, "_meta.parquet"),
#       paste0(preferred_prefix, "_metadata.parquet"),
#       paste0("_", preferred_prefix, "_metadata.parquet")
#     ))
#   }
#   
#   sidecar_paths <- file.path(dir, sidecar_names)
#   sidecar_paths <- sidecar_paths[file.exists(sidecar_paths)]
#   
#   if (length(sidecar_paths)) {
#     # What this does:
#     # - Reads only metadata of the sidecar parquet file (NOT dataset partitions)
#     # - If the sidecar contains dp-like keys, we treat the dataset as dp-conforming
#     sc <- sidecar_paths[1]
#     file_res <- dp_is_dp_parquet_file(
#       path = sc,
#       preferred_prefix = preferred_prefix,
#       legacy_prefixes = legacy_prefixes,
#       mode = mode
#     )
#     
#     file_res$kind <- "dataset_sidecar"
#     file_res$dataset_dir <- dir
#     file_res$sidecar_path <- sc
#     return(file_res)
#   }
#   
#   # No sidecar: sample parquet files under the directory
#   files <- list.files(dir, pattern = "\\.parquet$", full.names = TRUE, recursive = isTRUE(recursive))
#   files <- files[file.info(files)$isdir %||% rep(FALSE, length(files)) == FALSE]
#   if (!length(files)) {
#     return(list(
#       is_dp = FALSE,
#       score = 0L,
#       confidence = "none",
#       prefix_detected = NA_character_,
#       evidence = character(),
#       reason = "no_parquet_files_found",
#       kind = "dataset_dir",
#       dataset_dir = dir
#     ))
#   }
#   
#   scan_n <- max(1L, as.integer(scan_n))
#   files2 <- head(files, scan_n)
#   
#   res_list <- lapply(files2, function(fp) {
#     dp_is_dp_parquet_file(
#       path = fp,
#       preferred_prefix = preferred_prefix,
#       legacy_prefixes = legacy_prefixes,
#       mode = mode
#     )
#   })
#   
#   votes <- vapply(res_list, function(z) isTRUE(z$is_dp), logical(1))
#   dp_count <- sum(votes, na.rm = TRUE)
#   n <- length(votes)
#   
#   # Aggregate confidence: if most files say dp, call it dp
#   is_dp_dir <- dp_count >= ceiling(n / 2)
#   
#   # Combine evidence from the first positive hit (if any)
#   first_hit <- which(votes)[1]
#   evidence <- if (!is.na(first_hit)) res_list[[first_hit]]$evidence else character()
#   
#   list(
#     is_dp = is_dp_dir,
#     score = dp_count,                    # here "score" is votes
#     confidence = if (is_dp_dir) "medium" else "low",
#     prefix_detected = if (!is.na(first_hit)) res_list[[first_hit]]$prefix_detected else NA_character_,
#     evidence = evidence,
#     reason = sprintf("sampled=%d; dp_votes=%d; majority_rule=%s", n, dp_count, is_dp_dir),
#     kind = "dataset_dir",
#     dataset_dir = dir,
#     sampled_files = files2
#   )
# }
# 
# # -----------------------------------------------------------------------------
# # Unified front-door helper: file OR directory
# # -----------------------------------------------------------------------------
# # dp_is_dp_path
# dp_detect <- function(
#     path,
#     preferred_prefix = "dp",
#     legacy_prefixes = c("klimo"),
#     mode = c("detect", "validate"),
#     scan_n = 5L,
#     recursive = TRUE
# ) {
#   mode <- match.arg(mode)
#   
#   if (!file.exists(path)) {
#     stop(sprintf("Path does not exist: %s", path), call. = FALSE)
#   }
#   
#   if (isTRUE(file.info(path)$isdir)) {
#     return(dp_is_dp_dataset_dir(
#       dir = path,
#       preferred_prefix = preferred_prefix,
#       legacy_prefixes = legacy_prefixes,
#       mode = mode,
#       scan_n = scan_n,
#       recursive = recursive
#     ))
#   }
#   
#   # file
#   return(dp_is_dp_parquet_file(
#     path = path,
#     preferred_prefix = preferred_prefix,
#     legacy_prefixes = legacy_prefixes,
#     mode = mode
#   ))
# }
# 
# # -----------------------------------------------------------------------------
# # Pretty printer (optional)
# # -----------------------------------------------------------------------------
# # dp_print_dp_detection
# dp_print_detect <- function(x) {
#   cat("\n")
#   cat("dp detection\n")
#   cat("-----------\n")
#   cat("is_dp:           ", x$is_dp, "\n", sep = "")
#   cat("kind:            ", x$kind %||% NA_character_, "\n", sep = "")
#   cat("confidence:      ", x$confidence %||% NA_character_, "\n", sep = "")
#   cat("prefix_detected: ", x$prefix_detected %||% NA_character_, "\n", sep = "")
#   cat("reason:          ", x$reason %||% NA_character_, "\n", sep = "")
#   if (!is.null(x$path))        cat("path:            ", x$path, "\n", sep = "")
#   if (!is.null(x$dataset_dir)) cat("dataset_dir:     ", x$dataset_dir, "\n", sep = "")
#   if (!is.null(x$sidecar_path))cat("sidecar_path:    ", x$sidecar_path, "\n", sep = "")
#   if (!is.null(x$evidence) && length(x$evidence)) {
#     cat("\nEvidence keys (sample):\n")
#     cat(paste0("  - ", head(x$evidence, 10)), sep = "\n")
#     cat("\n")
#   }
#   invisible(x)
# }
# 
# # =============================================================================
# # Examples
# # =============================================================================
# 
# # -----------------------------------------------------------------------------
# # Example 1: Single parquet file
# # -----------------------------------------------------------------------------
# # What this does:
# # - Checks parquet file-level key/value metadata
# # - Decides whether dp/klimo metadata is present
# #
# # res1 <- dp_is_dp_path("some_file.parquet", mode="detect")
# # dp_print_dp_detection(res1)
# 
# # -----------------------------------------------------------------------------
# # Example 2: Dataset directory (partitioned dataset)
# # -----------------------------------------------------------------------------
# # What this does:
# # - First, looks for dp_meta.parquet (or a few common sidecar names)
# # - If none found, samples up to scan_n parquet files under the directory and votes
# #
# # res2 <- dp_is_dp_path("some_dataset_dir", mode="detect", scan_n=5)
# # dp_print_dp_detection(res2)
# 
# # -----------------------------------------------------------------------------
# # Example 3: “Validate” mode (stricter)
# # -----------------------------------------------------------------------------
# # What this does:
# # - Uses the same checks but requires more dp-like signal before saying TRUE
# #
# # res3 <- dp_is_dp_path("some_file_or_dir", mode="validate")
# # dp_print_dp_detection(res3)
# 
# .dp_stop <- function(...) stop(sprintf(...), call. = FALSE)
# .dp_warn <- function(...) warning(sprintf(...), call. = FALSE, immediate. = TRUE)
# 
# `%||%` <- function(x, y) {
#   if (is.null(x) || length(x) == 0L || is.na(x[[1]])) y else x[[1]]
# }
# 
# .dp_is_scalar_chr <- function(x) is.character(x) && length(x) == 1L && !is.na(x) && nzchar(x)
# 
# .dp_trim <- function(x) sub("^\\s+|\\s+$", "", x)
# 
# .dp_norm_name <- function(x) {
#   x <- tolower(as.character(x))
#   x <- .dp_trim(x)
#   x <- gsub("[^a-z0-9]+", "_", x)
#   x <- gsub("^_+|_+$", "", x)
#   x
# }
# 
# .dp_dt <- function(x) {
#   .dp_require("data.table")
#   
#   if (inherits(x, "data.table")) return(x)
#   
#   if (inherits(x, "data.frame")) {
#     # Strip attributes that can trip Arrow / data.table edge cases
#     x <- base::as.data.frame(x)
#     keep <- c("names", "row.names", "class")
#     at <- attributes(x)
#     attributes(x) <- at[intersect(names(at), keep)]
#     attr(x, "sorted") <- NULL
#     return(data.table::as.data.table(x))
#   }
#   
#   attr(x, "sorted") <- NULL
#   data.table::as.data.table(x)
# }
# 
# .dp_json <- function(x, pretty = FALSE) {
#   .dp_require("jsonlite")
#   jsonlite::toJSON(
#     x,
#     auto_unbox = TRUE,
#     null = "null",
#     digits = NA,
#     pretty = isTRUE(pretty)
#   )
# }
# 
# .dp_from_json <- function(x) {
#   .dp_require("jsonlite")
#   jsonlite::fromJSON(x, simplifyVector = TRUE)
# }
# 
# .dp_collect_unit_attrs <- function(dt) {
#   .dp_require("data.table")
#   cols <- names(dt)
#   obs <- vapply(cols, function(nm) {
#     u <- attr(dt[[nm]], "unit", exact = TRUE)
#     if (is.null(u)) NA_character_ else as.character(u)[1]
#   }, character(1))
#   data.table::data.table(column = cols, observed_units = obs)
# }
# 
# 
# # =============================================================================
# # 1) Metadata keys + prefix handling
# # =============================================================================
# 
# .dp_meta_keys <- function(prefix) {
#   stopifnot(.dp_is_scalar_chr(prefix))
#   list(
#     spec_version    = sprintf("%s:spec_version", prefix),
#     declared_system = sprintf("%s:declared_system", prefix),
#     field_spec_json = sprintf("%s:field_spec_json", prefix),
#     audit_json      = sprintf("%s:audit_json", prefix),
#     writer          = sprintf("%s:writer", prefix),
#     meta_storage    = sprintf("%s:meta_storage", prefix),
#     tag_units       = sprintf("%s:tag_missing_units", prefix),
#     scale_doc_only  = sprintf("%s:scale_documented_only", prefix)
#   )
# }
# 
# .dp_meta_guess_prefix <- function(kv, preferred = "dp", fallbacks = c("klimo")) {
#   # Determine which prefix exists in the key-value metadata.
#   # Returns a single prefix string (preferred if present; else first fallback present; else preferred).
#   if (is.null(kv) || !length(kv)) return(preferred)
#   
#   prefixes <- c(preferred, fallbacks)
#   for (p in prefixes) {
#     kk <- .dp_meta_keys(p)$field_spec_json
#     if (!is.null(kv[[kk]]) && nzchar(as.character(kv[[kk]])[1])) return(p)
#   }
#   preferred
# }
# 
# .dp_meta_pack_kv <- function(spec, audit_dt, extra = list(), prefix = "dp") {
#   .dp_require("data.table")
#   
#   spec <- .dp_spec_resolve_declared_units(spec)
#   s <- data.table::copy(spec$fields)
#   if ("column_norm" %in% names(s)) s[, column_norm := NULL]
#   
#   audit_dt <- data.table::copy(.dp_dt(audit_dt))
#   keys <- .dp_meta_keys(prefix)
#   
#   kv <- extra
#   
#   kv[[keys$spec_version]] <- as.character(spec$version)[1]
#   kv[[keys$spec_fields]]  <- .dp_dt_to_json(s)         # whatever you currently do
#   kv[[keys$audit]]        <- .dp_dt_to_json(audit_dt)  # whatever you currently do
#   
#   # any other keys...
#   kv
# }
# 
# .dp_meta_kv_to_dt <- function(kv) {
#   .dp_require("data.table")
#   
#   if (is.null(kv) || !length(kv)) {
#     out <- data.table::data.table(meta_key = character(), meta_value = character())
#     data.table::setkeyv(out, "meta_key")
#     return(out)
#   }
#   
#   out <- data.table::data.table(
#     meta_key   = as.character(names(kv)),
#     meta_value = vapply(kv, function(v) {
#       if (is.null(v)) return(NA_character_)
#       if (is.raw(v))  return(rawToChar(v))
#       as.character(v)[1]
#     }, character(1))
#   )
#   data.table::setkeyv(out, "meta_key")
#   out
# }
# 
# 
# # =============================================================================
# # 2) Spec object (dp_spec): declared units + scaling intent
# # =============================================================================
# # Spec is intentionally minimal and tolerant:
# # - It documents intent: concept, declared units, and writer_scale
# # - It does not enforce conversion; validation is optional
# #
# # fields_dt expected columns (minimal):
# # - column
# # Optional:
# # - concept, metric_units, imperial_units, declared_units
# # - writer_scale (numeric; scale factor for storage int columns)
# # - encoding, encoding_scale, encoding_base_units (reserved for future)
# # =============================================================================
# 
# .dp_spec_new <- function(fields_dt, declared_system = c("metric", "imperial", "custom"), version = "2") {
#   .dp_require("data.table")
#   
#   declared_system <- match.arg(declared_system)
#   fields_dt <- .dp_dt(fields_dt)
#   
#   if (!("column" %in% names(fields_dt))) .dp_stop("Spec table must contain 'column'.")
#   
#   fields_dt[, column := as.character(column)]
#   fields_dt[, column_norm := .dp_norm_name(column)]
#   
#   if (!("concept" %in% names(fields_dt)))             fields_dt[, concept := NA_character_]
#   if (!("metric_units" %in% names(fields_dt)))        fields_dt[, metric_units := NA_character_]
#   if (!("imperial_units" %in% names(fields_dt)))      fields_dt[, imperial_units := NA_character_]
#   if (!("declared_units" %in% names(fields_dt)))      fields_dt[, declared_units := NA_character_]
#   
#   if (!("writer_scale" %in% names(fields_dt)))        fields_dt[, writer_scale := as.numeric(NA)]
#   
#   # Reserved encoding fields (kept for forward-compat)
#   if (!("encoding" %in% names(fields_dt)))            fields_dt[, encoding := "none"]
#   if (!("encoding_scale" %in% names(fields_dt)))      fields_dt[, encoding_scale := as.numeric(NA)]
#   if (!("encoding_base_units" %in% names(fields_dt))) fields_dt[, encoding_base_units := NA_character_]
#   
#   fields_dt[, `:=`(
#     concept = as.character(concept),
#     metric_units = as.character(metric_units),
#     imperial_units = as.character(imperial_units),
#     declared_units = as.character(declared_units),
#     writer_scale = suppressWarnings(as.numeric(writer_scale)),
#     encoding = as.character(encoding),
#     encoding_scale = suppressWarnings(as.numeric(encoding_scale)),
#     encoding_base_units = as.character(encoding_base_units)
#   )]
#   
#   fields_dt[is.na(encoding) | !nzchar(encoding), encoding := "none"]
#   
#   bad_enc <- setdiff(unique(fields_dt$encoding), c("none", "scaled_int"))
#   if (length(bad_enc)) {
#     .dp_stop("Unsupported encoding value(s): %s. Allowed: 'none', 'scaled_int'.", paste(bad_enc, collapse = ", "))
#   }
#   
#   out <- list(fields = fields_dt, declared_system = declared_system, version = as.character(version))
#   class(out) <- "dp_spec"
#   out
# }
# 
# print.dp_spec <- function(x, ...) {
#   .dp_require("data.table")
#   dt <- x$fields
#   n <- nrow(dt)
#   n_concept <- dt[!is.na(concept) & nzchar(concept), .N]
#   n_decl <- dt[!is.na(declared_units) & nzchar(declared_units), .N]
#   n_scale <- dt[is.finite(writer_scale) & writer_scale != 0, .N]
#   cat(sprintf("<dp_spec> system=%s; version=%s\n", x$declared_system, x$version))
#   cat(sprintf("  rows=%d; concept=%d; declared_units=%d; writer_scale=%d\n", n, n_concept, n_decl, n_scale))
#   invisible(x)
# }
# 
# .dp_spec_resolve_declared_units <- function(spec) {
#   if (!requireNamespace("data.table", quietly = TRUE)) {
#     stop(".dp_spec_resolve_declared_units(): package 'data.table' is required.", call. = FALSE)
#   }
#   
#   if (!inherits(spec, "dp_spec")) {
#     stop(".dp_spec_resolve_declared_units(): `spec` must be a <dp_spec>.", call. = FALSE)
#   }
#   
#   if (is.null(spec$fields) || !data.table::is.data.table(spec$fields) || !nrow(spec$fields)) {
#     return(spec)
#   }
#   
#   f <- data.table::copy(spec$fields)
#   
#   # Ensure expected columns exist
#   for (nm in c("declared_units", "metric_units", "imperial_units")) {
#     if (!(nm %in% names(f))) f[, (nm) := NA_character_]
#   }
#   
#   # Normalize to character + trim blanks to NA
#   .trim <- function(x) {
#     x <- as.character(x)
#     x <- trimws(x)
#     x[!nzchar(x)] <- NA_character_
#     x
#   }
#   
#   f[, `:=`(
#     declared_units = .trim(declared_units),
#     metric_units   = .trim(metric_units),
#     imperial_units = .trim(imperial_units)
#   )]
#   
#   # Determine the declared system as a SCALAR
#   sys <- spec$declared_system
#   if (is.null(sys) || !length(sys) || is.na(sys) || !nzchar(as.character(sys)[1])) {
#     sys <- spec$system
#   }
#   sys <- as.character(sys)[1]
#   
#   # Fill missing declared_units based on declared system (scalar branch; vector assignment)
#   miss <- which(is.na(f$declared_units))
#   if (length(miss)) {
#     if (identical(sys, "metric")) {
#       f[miss, declared_units := metric_units]
#     } else if (identical(sys, "imperial")) {
#       f[miss, declared_units := imperial_units]
#     } else {
#       # leave missing if system unknown
#       f[miss, declared_units := NA_character_]
#     }
#   }
#   
#   spec$fields <- f
#   spec
# }
# 
# 
# # Optional convenience: a small default catalog
# .dp_spec_catalog <- function() {
#   .dp_require("data.table")
#   
#   data.table::rbindlist(list(
#     data.table::data.table(concept="air_temp", metric_units="degC", imperial_units="degF", writer_scale=100,
#                            synonyms=list(c("ta","temp","temperature","airtemp","air_temp","ta_2m"))),
#     data.table::data.table(concept="dewpoint", metric_units="degC", imperial_units="degF", writer_scale=100,
#                            synonyms=list(c("td","dewpoint","dew_point"))),
#     data.table::data.table(concept="relhum", metric_units="percent", imperial_units="percent", writer_scale=100,
#                            synonyms=list(c("rh","relh","relhum","rel_hum","relative_humidity"))),
#     data.table::data.table(concept="wind_speed", metric_units="m/s", imperial_units="mph", writer_scale=100,
#                            synonyms=list(c("wind","wind10m","wspd","speed","gust"))),
#     data.table::data.table(concept="wind_dir", metric_units="deg", imperial_units="deg", writer_scale=10,
#                            synonyms=list(c("dd","dir","wdir","wind_dir","winddirection"))),
#     data.table::data.table(concept="lat", metric_units="deg", imperial_units="deg", writer_scale=10000,
#                            synonyms=list(c("lat","latitude"))),
#     data.table::data.table(concept="lon", metric_units="deg", imperial_units="deg", writer_scale=10000,
#                            synonyms=list(c("lon","longitude","lng"))),
#     data.table::data.table(concept="wbgt", metric_units="degC", imperial_units="degF", writer_scale=100,
#                            synonyms=list(c("wbgt","nwb","tg")))
#   ), use.names = TRUE, fill = TRUE)
# }
# 
# 
# # dp_spec
# dp_spec <- function(x, declared_system = c("metric", "imperial"), include_unmatched = TRUE) {
#   .dp_require("data.table")
#   declared_system <- match.arg(declared_system)
#   
#   x <- .dp_dt(x)
#   cols <- names(x)
#   cols_norm <- .dp_norm_name(cols)
#   
#   cat_dt <- .dp_spec_catalog()
#   map <- cat_dt[, {
#     syn <- unlist(synonyms, use.names = FALSE)
#     if (!length(syn)) syn <- character()
#     .(synonym = syn)
#   }, by = .(concept, metric_units, imperial_units, writer_scale)]
#   
#   map[, synonym_norm := .dp_norm_name(synonym)]
#   
#   idx <- match(cols_norm, map$synonym_norm)
#   matched <- !is.na(idx)
#   
#   out_dt <- data.table::data.table(
#     column = cols,
#     concept = ifelse(matched, map$concept[idx], NA_character_),
#     metric_units = ifelse(matched, map$metric_units[idx], NA_character_),
#     imperial_units = ifelse(matched, map$imperial_units[idx], NA_character_),
#     declared_units = NA_character_,
#     writer_scale = ifelse(matched, as.numeric(map$writer_scale[idx]), as.numeric(NA)),
#     encoding = "none",
#     encoding_scale = as.numeric(NA),
#     encoding_base_units = NA_character_
#   )
#   
#   if (!isTRUE(include_unmatched)) {
#     out_dt <- out_dt[!is.na(concept) & nzchar(concept)]
#   }
#   
#   .dp_spec_new(out_dt, declared_system = declared_system, version = "2")
# }
# 
# 
# dp_spec_set_units <- function(spec, columns, metric_units = NULL, imperial_units = NULL, declared_units = NULL) {
#   .dp_require("data.table")
#   if (!inherits(spec, "dp_spec")) .dp_stop("spec must be a dp_spec.")
#   dt <- data.table::copy(spec$fields)
#   
#   stopifnot(is.character(columns), length(columns) >= 1L)
#   hit <- .dp_norm_name(columns)
#   dt[, .hit := column_norm %in% hit]
#   
#   if (!is.null(metric_units)) {
#     if (!.dp_is_scalar_chr(metric_units)) .dp_stop("metric_units must be a character scalar.")
#     dt[.hit == TRUE, metric_units := metric_units]
#   }
#   if (!is.null(imperial_units)) {
#     if (!.dp_is_scalar_chr(imperial_units)) .dp_stop("imperial_units must be a character scalar.")
#     dt[.hit == TRUE, imperial_units := imperial_units]
#   }
#   if (!is.null(declared_units)) {
#     if (!.dp_is_scalar_chr(declared_units)) .dp_stop("declared_units must be a character scalar.")
#     dt[.hit == TRUE, declared_units := declared_units]
#   }
#   
#   dt[, .hit := NULL]
#   spec$fields <- dt
#   spec
# }
# 
# 
# # dp_set_scale
# dp_set_scale <- function(spec, columns, writer_scale) {
#   .dp_require("data.table")
#   if (!inherits(spec, "dp_spec")) .dp_stop("spec must be a dp_spec.")
#   if (length(writer_scale) != 1L) .dp_stop("writer_scale must be a scalar numeric (or NA).")
#   
#   dt <- data.table::copy(spec$fields)
#   hit <- .dp_norm_name(columns)
#   dt[, .hit := column_norm %in% hit]
#   
#   sc <- suppressWarnings(as.numeric(writer_scale))
#   dt[.hit == TRUE, writer_scale := sc]
#   
#   dt[, .hit := NULL]
#   spec$fields <- dt
#   spec
# }
# 
# 
# # =============================================================================
# # 3) Units: optional attribute tagging + optional validation
# # =============================================================================
# 
# #  
# dp_tag_units <- function(
#     dt,
#     spec,
#     only_missing = TRUE,
#     system = c("declared", "metric", "imperial"),
#     unit_attr = "units"
# ) {
#   .dp_require("data.table")
#   dt0 <- .dp_dt(dt)
#   
#   if (!inherits(spec, "dp_spec") || is.null(spec$fields) || !data.table::is.data.table(spec$fields)) {
#     .dp_stop("dp_units_tag_from_spec(): `spec` must be a <dp_spec> with a data.table `spec$fields`.")
#   }
#   
#   f <- data.table::copy(spec$fields)
#   if (!("column" %in% names(f))) .dp_stop("dp_units_tag_from_spec(): `spec$fields` must include `column`.")
#   
#   system <- match.arg(system)
#   declared_system <- tryCatch(spec$declared_system, error = function(e) NULL)
#   
#   expected <- .dp_units_expected_from_spec(
#     fields = f,
#     system = system,
#     declared_system = declared_system
#   )
#   
#   f[, expected_units := expected]
#   
#   out <- data.table::copy(dt0)  # no side-effects
#   dt_cols <- names(out)
#   
#   did <- data.table::data.table(
#     column = dt_cols,
#     tag_units = NA_character_,
#     previous_units = NA_character_,
#     did_tag = FALSE
#   )
#   
#   # Map spec expected units by column name
#   m <- match(dt_cols, f$column)
#   spec_units <- rep(NA_character_, length(dt_cols))
#   spec_units[!is.na(m)] <- f$expected_units[m[!is.na(m)]]
#   
#   for (i in seq_along(dt_cols)) {
#     col <- dt_cols[i]
#     u_exp <- spec_units[i]
#     if (is.na(u_exp) || !nzchar(trimws(u_exp))) next
#     
#     u_prev <- attr(out[[col]], unit_attr, exact = TRUE)
#     u_prev_chr <- if (is.null(u_prev) || !length(u_prev) || !nzchar(trimws(as.character(u_prev)[1]))) NA_character_ else as.character(u_prev)[1]
#     
#     if (isTRUE(only_missing) && !is.na(u_prev_chr)) {
#       did[i, previous_units := u_prev_chr]
#       next
#     }
#     
#     attr(out[[col]], unit_attr) <- u_exp
#     did[i, `:=`(tag_units = u_exp, previous_units = u_prev_chr, did_tag = TRUE)]
#   }
#   
#   list(dt = out, did_tag = did)
# }
# 
# # dp_check_units
# dp_check_units <- function(
#     dt,
#     spec,
#     system = c("declared", "metric", "imperial"),
#     unit_attr = "units",
#     warn_max = 8L,
#     stop_on_any = FALSE
# ) {
#   .dp_require("data.table")
#   dt0 <- .dp_dt(dt)
#   
#   if (!inherits(spec, "dp_spec") || is.null(spec$fields) || !data.table::is.data.table(spec$fields)) {
#     .dp_stop("dp_check_units(): `spec` must be a <dp_spec> with a data.table `spec$fields`.")
#   }
#   
#   f <- data.table::copy(spec$fields)
#   if (!("column" %in% names(f))) .dp_stop("dp_check_units(): `spec$fields` must include `column`.")
#   
#   system <- match.arg(system)
#   declared_system <- tryCatch(spec$declared_system, error = function(e) NULL)
#   
#   expected <- .dp_units_expected_from_spec(
#     fields = f,
#     system = system,
#     declared_system = declared_system
#   )
#   
#   f[, expected_units := expected]
#   
#   dt_cols <- names(dt0)
#   observed_units <- vapply(dt_cols, function(nm) {
#     u <- attr(dt0[[nm]], unit_attr, exact = TRUE)
#     if (is.null(u) || !length(u)) NA_character_ else as.character(u)[1]
#   }, character(1))
#   
#   audit <- data.table::data.table(
#     column = as.character(f$column),
#     expected_units = as.character(f$expected_units)
#   )
#   
#   idx <- match(audit$column, dt_cols)
#   audit[, observed_units := NA_character_]
#   audit[!is.na(idx), observed_units := observed_units[idx[!is.na(idx)]]]
#   
#   .trim <- function(x) { x <- trimws(as.character(x)); x[!nzchar(x)] <- NA_character_; x }
#   audit[, `:=`(expected_units = .trim(expected_units), observed_units = .trim(observed_units))]
#   
#   audit[, status := data.table::fcase(
#     is.na(idx), "missing_column_in_dt",
#     is.na(expected_units) & is.na(observed_units), "no_units",
#     !is.na(expected_units) & is.na(observed_units), "missing_units",
#     is.na(expected_units) & !is.na(observed_units), "unexpected_units",
#     !is.na(expected_units) & !is.na(observed_units) & expected_units == observed_units, "ok",
#     default = "mismatch"
#   )]
#   
#   probs <- audit[status != "ok"]
#   if (nrow(probs)) {
#     n_show <- max(0L, as.integer(warn_max))
#     if (n_show > 0L) {
#       show <- probs[seq_len(min(n_show, nrow(probs)))]
#       warning(
#         paste0(
#           "dp_check_units(): found unit/schema issues (showing ", nrow(show), " of ", nrow(probs), "):\n",
#           paste0(
#             "  - ", show$column, ": ", show$status,
#             ifelse(!is.na(show$expected_units), paste0(" | expected=", show$expected_units), ""),
#             ifelse(!is.na(show$observed_units), paste0(" | observed=", show$observed_units), ""),
#             collapse = "\n"
#           )
#         ),
#         call. = FALSE
#       )
#     }
#     if (isTRUE(stop_on_any)) {
#       .dp_stop("dp_check_units(): validation failed (set stop_on_any=FALSE to warn only).")
#     }
#   }
#   
#   invisible(audit)
# }
# 
# .dp_units_expected_from_spec <- function(fields, system = c("declared", "metric", "imperial"), declared_system = NULL) {
#   .dp_require("data.table")
#   system <- match.arg(system)
#   
#   # Ensure columns exist
#   for (nm in c("declared_units", "metric_units", "imperial_units")) {
#     if (!(nm %in% names(fields))) fields[, (nm) := NA_character_]
#   }
#   
#   if (system == "declared") {
#     expected <- as.character(fields$declared_units)
#     
#     # Optional fallback if declared_units is empty
#     if (!is.null(declared_system) &&
#         (length(expected) == 0L || all(is.na(expected) | !nzchar(trimws(expected))))) {
#       if (identical(declared_system, "metric")) {
#         expected <- as.character(fields$metric_units)
#       } else if (identical(declared_system, "imperial")) {
#         expected <- as.character(fields$imperial_units)
#       }
#     }
#   } else if (system == "metric") {
#     expected <- as.character(fields$metric_units)
#   } else {
#     expected <- as.character(fields$imperial_units)
#   }
#   
#   if (!length(expected)) expected <- rep(NA_character_, nrow(fields))
#   expected
# }
# 
# # =============================================================================
# # 4) Scaling: encode to int columns + decode back (opt-in)
# # =============================================================================
# # Convention: storage column = paste0(column, suffix), default suffix "_i"
# # Default behavior: keep original numeric columns unless drop_original=TRUE
# # =============================================================================
# 
# dp_scale_encode <- function(dt, spec, suffix = "_i", drop_original = FALSE,
#                             round_fn = c("round", "floor", "ceiling")) {
#   .dp_require("data.table")
#   dt <- data.table::copy(.dp_dt(dt))
#   round_fn <- match.arg(round_fn)
#   
#   if (!inherits(spec, "dp_spec")) .dp_stop("spec must be a dp_spec.")
#   spec <- .dp_spec_resolve_declared_units(spec)
#   s <- spec$fields
#   
#   plan <- s[
#     encoding == "none" &
#       is.finite(writer_scale) & writer_scale != 0,
#     .(
#       column = as.character(column),
#       writer_scale = as.numeric(writer_scale),
#       storage_column = paste0(as.character(column), suffix),
#       declared_units = as.character(declared_units)
#     )
#   ]
#   if (!nrow(plan)) {
#     plan[, did_scale := logical(0)]
#     return(list(dt = dt, scale_map = plan))
#   }
#   
#   plan[, did_scale := FALSE]
#   
#   for (i in seq_len(nrow(plan))) {
#     col  <- plan$column[i]
#     sc   <- plan$writer_scale[i]
#     scol <- plan$storage_column[i]
#     
#     if (!col %in% names(dt)) next
#     
#     v <- dt[[col]]
#     if (!(is.numeric(v) || is.integer(v))) next
#     if (!is.finite(sc) || sc == 0) next
#     
#     vv <- switch(
#       round_fn,
#       round   = as.integer(round(v * sc)),
#       floor   = as.integer(floor(v * sc)),
#       ceiling = as.integer(ceiling(v * sc))
#     )
#     
#     dt[[scol]] <- vv
#     plan$did_scale[i] <- TRUE
#     
#     if (isTRUE(drop_original)) dt[[col]] <- NULL
#   }
#   
#   list(dt = dt, scale_map = plan)
# }
# 
# dp_scale_decode <- function(dt, audit_dt, keep_storage = TRUE, overwrite = FALSE) {
#   .dp_require("data.table")
#   dt <- data.table::copy(.dp_dt(dt))
#   audit_dt <- .dp_dt(audit_dt)
#   
#   need <- c("column", "writer_scale", "storage_column", "did_scale")
#   if (!all(need %in% names(audit_dt))) return(dt)
#   
#   plan <- audit_dt[
#     isTRUE(did_scale) &
#       is.finite(writer_scale) & writer_scale != 0 &
#       !is.na(storage_column) & nzchar(storage_column)
#   ]
#   if (!nrow(plan)) return(dt)
#   
#   for (i in seq_len(nrow(plan))) {
#     base <- as.character(plan$column[i])
#     scol <- as.character(plan$storage_column[i])
#     sc   <- as.numeric(plan$writer_scale[i])
#     
#     if (!scol %in% names(dt)) next
#     if (base %in% names(dt) && !isTRUE(overwrite)) next
#     
#     dt[[base]] <- as.numeric(dt[[scol]]) / sc
#     if (!isTRUE(keep_storage)) dt[[scol]] <- NULL
#   }
#   
#   dt
# }
# 
# 
# # =============================================================================
# # 5) Parquet metadata read/write (single-file)
# # =============================================================================
# 
# .dp_read_parquet_schema_kv <- function(path) {
#   .dp_require("arrow")
#   
#   pf <- arrow::ParquetFileReader$create(path)
#   md <- pf$metadata
#   sch <- md$schema
#   kv <- sch$metadata
#   if (is.null(kv)) return(list())
#   
#   out <- lapply(kv, function(v) {
#     if (is.null(v)) return(NA_character_)
#     if (is.raw(v))  return(rawToChar(v))
#     as.character(v)[1]
#   })
#   names(out) <- names(kv)
#   out
# }
# 
# dp_read_parquet_meta <- function(path, parse = TRUE, preferred_prefix = "dp", legacy_prefixes = c("klimo")) {
#   .dp_require("data.table")
#   
#   kv <- .dp_read_parquet_schema_kv(path)
#   kv_dt <- .dp_meta_kv_to_dt(kv)
#   
#   chosen <- .dp_meta_guess_prefix(kv, preferred = preferred_prefix, fallbacks = legacy_prefixes)
#   keys <- .dp_meta_keys(chosen)
#   
#   out <- list(
#     kv = kv,
#     kv_dt = kv_dt,
#     prefix = chosen,
#     spec_dt = NULL,
#     audit_dt = NULL
#   )
#   
#   if (isTRUE(parse)) {
#     js_spec  <- kv[[keys$field_spec_json]]
#     js_audit <- kv[[keys$audit_json]]
#     
#     if (!is.null(js_spec) && nzchar(js_spec))  out$spec_dt  <- .dp_dt(.dp_from_json(js_spec))
#     if (!is.null(js_audit) && nzchar(js_audit)) out$audit_dt <- .dp_dt(.dp_from_json(js_audit))
#   }
#   
#   out
# }
# 
# 
# # =============================================================================
# # 6) Dataset metadata sidecar (recommended for Arrow Datasets)
# # =============================================================================
# 
# .dp_meta_sidecar_name <- function() "_dp_meta.parquet"
# .dp_sidecar_path <- function(dataset_path) file.path(dataset_path, .dp_meta_sidecar_name())
# 
# dp_write_dataset_meta <- function(dataset_path, kv, overwrite = TRUE, compression = "zstd") {
#   .dp_require("arrow")
#   .dp_require("data.table")
#   
#   if (!dir.exists(dataset_path)) dir.create(dataset_path, recursive = TRUE, showWarnings = FALSE)
#   
#   sidecar <- .dp_sidecar_path(dataset_path)
#   if (file.exists(sidecar) && !isTRUE(overwrite)) .dp_stop("Sidecar exists and overwrite=FALSE: %s", sidecar)
#   
#   dt_kv <- .dp_meta_kv_to_dt(kv)
#   tab <- arrow::Table$create(dt_kv)
#   arrow::write_parquet(tab, sink = sidecar, compression = compression)
#   
#   invisible(sidecar)
# }
# 
# dp_read_dataset_meta <- function(dataset_path, parse = TRUE, preferred_prefix = "dp", legacy_prefixes = c("klimo")) {
#   .dp_require("arrow")
#   .dp_require("data.table")
#   
#   sidecar <- .dp_sidecar_path(dataset_path)
#   if (!file.exists(sidecar)) {
#     return(list(
#       kv = list(),
#       kv_dt = data.table::data.table(meta_key=character(), meta_value=character()),
#       prefix = preferred_prefix,
#       spec_dt = NULL,
#       audit_dt = NULL,
#       sidecar_path = NULL
#     ))
#   }
#   
#   dt_kv <- arrow::read_parquet(sidecar, as_data_frame = TRUE)
#   dt_kv <- .dp_dt(dt_kv)
#   
#   # Back-compat for older column names if needed
#   if (!all(c("meta_key","meta_value") %in% names(dt_kv))) {
#     if (all(c("key","value") %in% names(dt_kv))) {
#       data.table::setnames(dt_kv, c("key","value"), c("meta_key","meta_value"))
#     }
#   }
#   if (!all(c("meta_key","meta_value") %in% names(dt_kv))) {
#     .dp_stop("Sidecar has unexpected columns: %s", paste(names(dt_kv), collapse = ", "))
#   }
#   
#   data.table::setkeyv(dt_kv, "meta_key")
#   kv <- as.list(dt_kv$meta_value)
#   names(kv) <- dt_kv$meta_key
#   
#   chosen <- .dp_meta_guess_prefix(kv, preferred = preferred_prefix, fallbacks = legacy_prefixes)
#   keys <- .dp_meta_keys(chosen)
#   
#   out <- list(
#     kv = kv,
#     kv_dt = dt_kv,
#     prefix = chosen,
#     spec_dt = NULL,
#     audit_dt = NULL,
#     sidecar_path = sidecar
#   )
#   
#   if (isTRUE(parse)) {
#     js_spec  <- kv[[keys$field_spec_json]]
#     js_audit <- kv[[keys$audit_json]]
#     
#     if (!is.null(js_spec) && nzchar(js_spec))  out$spec_dt  <- .dp_dt(.dp_from_json(js_spec))
#     if (!is.null(js_audit) && nzchar(js_audit)) out$audit_dt <- .dp_dt(.dp_from_json(js_audit))
#   }
#   
#   out
# }
# 
# 
# # =============================================================================
# # 7) Public API: dp_write_parquet() / dp_read_parquet()
# # =============================================================================
# 
# dp_write_parquet <- function(
#     x,
#     path,
#     spec = c("metric", "imperial"),
#     mode = c("document", "validate"),
#     scale = FALSE,
#     suffix = "_i",
#     drop_original = FALSE,
#     tag_missing_units = FALSE,
#     prefix = "dp",
#     compression = "zstd",
#     unit_attr = "units",
#     warn_max = 8L,
#     ...
# ) {
#   if (!requireNamespace("arrow", quietly = TRUE)) {
#     stop("dp_write_parquet(): package 'arrow' is required.", call. = FALSE)
#   }
#   if (!requireNamespace("data.table", quietly = TRUE)) {
#     stop("dp_write_parquet(): package 'data.table' is required.", call. = FALSE)
#   }
#   
#   mode <- match.arg(mode)
#   
#   dt <- if (data.table::is.data.table(x)) x else data.table::as.data.table(x)
#   
#   if (!is.character(path) || length(path) != 1L || !nzchar(path)) {
#     stop("dp_write_parquet(): `path` must be a non-empty file path.", call. = FALSE)
#   }
#   dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
#   
#   # ---- spec normalization ----
#   sp <- NULL
#   if (inherits(spec, "dp_spec")) {
#     sp <- spec
#   } else {
#     spec <- match.arg(spec)
#     if (!exists("dp_spec", mode = "function", inherits = TRUE)) {
#       stop("dp_write_parquet(): dp_spec() not found.", call. = FALSE)
#     }
#     sp <- dp_spec(dt, declared_system = spec, include_unmatched = TRUE)
#   }
#   
#   # CRITICAL: resolve declared_units safely (no fifelse bug)
#   sp <- .dp_spec_resolve_declared_units(sp)
#   
#   # ---- optional unit tagging (metadata only) ----
#   if (isTRUE(tag_missing_units)) {
#     if (!exists("dp_units_tag_from_spec", mode = "function", inherits = TRUE)) {
#       stop("dp_write_parquet(): dp_units_tag_from_spec() not found.", call. = FALSE)
#     }
#     tag <- dp_units_tag_from_spec(dt, sp, only_missing = TRUE, unit_attr = unit_attr)
#     dt <- tag$dt
#   }
#   
#   # ---- optional validation ----
#   if (identical(mode, "validate")) {
#     if (!exists("dp_check_units", mode = "function", inherits = TRUE)) {
#       stop("dp_write_parquet(): dp_check_units() not found.", call. = FALSE)
#     }
#     dp_check_units(
#       dt = dt,
#       spec = sp,
#       system = "declared",
#       unit_attr = unit_attr,
#       warn_max = as.integer(warn_max),
#       stop_on_any = FALSE
#     )
#   }
#   
#   # ---- optional scaling ----
#   # NOTE: if dp_scale_encode() exists, use it; otherwise skip scaling.
#   if (isTRUE(scale)) {
#     if (!exists("dp_scale_encode", mode = "function", inherits = TRUE)) {
#       stop("dp_write_parquet(): scale=TRUE but dp_scale_encode() not found.", call. = FALSE)
#     }
#     enc <- dp_scale_encode(
#       dt = dt,
#       spec = sp,
#       suffix = suffix,
#       drop_original = isTRUE(drop_original),
#       round_fn = "round"
#     )
#     dt <- enc$dt
#   }
#   
#   # ---- metadata keys (avoid `list(keys$writer = ...)` which is invalid R) ----
#   # If you have .dp_meta_keys(), use it; else minimal fallback.
#   keys <- NULL
#   if (exists(".dp_meta_keys", mode = "function", inherits = TRUE)) {
#     keys <- tryCatch(.dp_meta_keys(prefix), error = function(e) NULL)
#   }
#   if (is.null(keys) || !is.list(keys) || !length(keys)) {
#     keys <- list(
#       writer         = paste0(prefix, ":writer"),
#       written_at_utc = paste0(prefix, ":written_at_utc"),
#       mode           = paste0(prefix, ":mode"),
#       declared_sys   = paste0(prefix, ":declared_system"),
#       scale_enabled  = paste0(prefix, ":scale_enabled"),
#       drop_original  = paste0(prefix, ":drop_original"),
#       suffix         = paste0(prefix, ":suffix"),
#       compression    = paste0(prefix, ":compression")
#     )
#   }
#   
#   kv <- list()
#   kv[[keys$writer]]         <- "dp_write_parquet_fixed"
#   kv[[keys$written_at_utc]] <- format(Sys.time(), tz = "UTC", usetz = TRUE)
#   kv[[keys$mode]]           <- as.character(mode)
#   
#   declared_sys <- sp$declared_system
#   if (is.null(declared_sys) || !length(declared_sys)) declared_sys <- sp$system
#   kv[[keys$declared_sys]]   <- as.character(declared_sys)[1]
#   
#   kv[[keys$scale_enabled]]  <- as.character(isTRUE(scale))
#   kv[[keys$drop_original]]  <- as.character(isTRUE(drop_original))
#   kv[[keys$suffix]]         <- as.character(suffix)
#   kv[[keys$compression]]    <- as.character(compression)
#   
#   tab <- arrow::as_arrow_table(dt)
#   tab$metadata <- kv
#   
#   arrow::write_parquet(
#     x = tab,
#     sink = path,
#     compression = compression,
#     ...
#   )
#   
#   invisible(list(path = path, spec = sp, metadata = kv))
# }
# 
# 
# dp_read_parquet <- function(
#     path,
#     decode_scaled = TRUE,
#     keep_storage = TRUE,
#     attach_units = c("declared", "none"),
#     preferred_prefix = "dp",
#     legacy_prefixes = c("klimo"),
#     ...
# ) {
#   .dp_require("arrow")
#   .dp_require("data.table")
#   
#   attach_units <- match.arg(attach_units)
#   
#   meta <- dp_read_parquet_meta(
#     path = path,
#     parse = TRUE,
#     preferred_prefix = preferred_prefix,
#     legacy_prefixes = legacy_prefixes
#   )
#   
#   dt <- arrow::read_parquet(path, as_data_frame = TRUE, ...)
#   dt <- .dp_dt(dt)
#   
#   if (isTRUE(decode_scaled) && !is.null(meta$audit_dt) && nrow(meta$audit_dt)) {
#     dt <- dp_scale_decode(dt, meta$audit_dt, keep_storage = isTRUE(keep_storage), overwrite = FALSE)
#   }
#   
#   if (attach_units == "declared" && !is.null(meta$spec_dt) && nrow(meta$spec_dt)) {
#     sp <- .dp_spec_new(
#       fields_dt = meta$spec_dt,
#       declared_system = meta$kv[[.dp_meta_keys(meta$prefix)$declared_system]] %||% "custom",
#       version = meta$kv[[.dp_meta_keys(meta$prefix)$spec_version]] %||% "2"
#     )
#     sp <- .dp_spec_resolve_declared_units(sp)
#     dt <- dp_units_tag_from_spec(dt, sp, only_missing = TRUE)$dt
#   }
#   
#   list(dt = dt, meta = meta)
# }
# 
# 
# # =============================================================================
# # 8) Public API: dp_open_dataset() / dp_write_dataset()
# # =============================================================================
# # - dp_open_dataset(): returns Arrow Dataset (and optional sidecar metadata)
# # - dp_write_dataset(): writes Arrow Dataset and writes sidecar metadata
# #
# # Scaling for datasets:
# # - "yes" to document-only scaling: we record the intended storage columns (col+suffix)
# # - This function does not apply scaling unless the caller already created those columns
# # =============================================================================
# 
# # =============================================================================
# # dp parquet/dataset IO — corrected versions (meta keys + declared units)
# # =============================================================================
# 
# `%||%` <- function(x, y) if (is.null(x) || length(x) == 0L) y else x
# 
# .dp_require <- function(pkg) {
#   if (!requireNamespace(pkg, quietly = TRUE)) {
#     stop(sprintf("Package '%s' is required.", pkg), call. = FALSE)
#   }
# }
# 
# .dp_stop <- function(msg) stop(msg, call. = FALSE)
# 
# .dp_dt <- function(x) {
#   .dp_require("data.table")
#   if (data.table::is.data.table(x)) return(x)
#   data.table::as.data.table(x)
# }
# 
# # -----------------------------------------------------------------------------
# # 1) FIX: resolve declared_units without scalar fifelse
# # -----------------------------------------------------------------------------
# .dp_spec_resolve_declared_units <- function(spec) {
#   .dp_require("data.table")
#   
#   if (!inherits(spec, "dp_spec")) {
#     .dp_stop(".dp_spec_resolve_declared_units(): `spec` must be a <dp_spec> object.")
#   }
#   if (is.null(spec$fields) || !data.table::is.data.table(spec$fields)) {
#     return(spec)
#   }
#   
#   f <- data.table::copy(spec$fields)
#   
#   # Ensure unit columns exist
#   for (nm in c("declared_units", "metric_units", "imperial_units")) {
#     if (!(nm %in% names(f))) f[, (nm) := NA_character_]
#   }
#   
#   # Determine declared system (scalar)
#   sys <- NULL
#   sys <- (spec$declared_system %||% spec$system) %||% NA_character_
#   sys <- as.character(sys)[1]
#   sys <- if (!is.na(sys)) trimws(sys) else NA_character_
#   
#   # Choose the fallback vector ONCE (no fifelse with scalar test)
#   fallback_vec <- rep(NA_character_, nrow(f))
#   if (identical(sys, "metric")) {
#     fallback_vec <- as.character(f$metric_units)
#   } else if (identical(sys, "imperial")) {
#     fallback_vec <- as.character(f$imperial_units)
#   } else {
#     # Unknown declared system -> do not invent declared_units
#     fallback_vec <- rep(NA_character_, nrow(f))
#   }
#   
#   # Fill declared_units only where missing/blank
#   du <- as.character(f$declared_units)
#   du <- trimws(du)
#   du[du == ""] <- NA_character_
#   
#   fb <- as.character(fallback_vec)
#   fb <- trimws(fb)
#   fb[fb == ""] <- NA_character_
#   
#   f[, declared_units := data.table::fifelse(!is.na(du), du, fb)]
#   
#   spec$fields <- f
#   spec
# }
# 
# # -----------------------------------------------------------------------------
# # 2) Metadata keys: guaranteed, stable names
# # -----------------------------------------------------------------------------
# .dp_meta_keys <- function(prefix = "dp") {
#   p <- as.character(prefix)[1]
#   if (is.na(p) || !nzchar(p)) .dp_stop(".dp_meta_keys(): `prefix` must be a non-empty string.")
#   
#   list(
#     prefix = p,
#     
#     writer         = paste0(p, ":writer"),
#     written_at_utc = paste0(p, ":written_at_utc"),
#     
#     declared_system = paste0(p, ":declared_system"),
#     spec_version    = paste0(p, ":spec_version"),
#     
#     # embedded payloads (serialized + gz + base64)
#     spec_b64  = paste0(p, ":spec_b64"),
#     audit_b64 = paste0(p, ":audit_b64"),
#     
#     # optional dataset-write context
#     sidecar_compression    = paste0(p, ":sidecar_compression"),
#     write_format           = paste0(p, ":write_format"),
#     partitioning           = paste0(p, ":partitioning"),
#     existing_data_behavior = paste0(p, ":existing_data_behavior")
#   )
# }
# 
# # -----------------------------------------------------------------------------
# # 3) Pack/unpack kv (FIX: dynamic key assignment; no list(keys$k = ...))
# # -----------------------------------------------------------------------------
# .dp_meta_pack_kv <- function(spec, audit_dt, extra = list(), prefix = "dp") {
#   .dp_require("data.table")
#   .dp_require("jsonlite")
#   
#   if (!inherits(spec, "dp_spec")) {
#     .dp_stop(".dp_meta_pack_kv(): `spec` must be a <dp_spec> object.")
#   }
#   
#   spec <- .dp_spec_resolve_declared_units(spec)
#   
#   keys <- .dp_meta_keys(prefix)
#   
#   kv <- list()
#   kv[[keys$written_at_utc]] <- format(Sys.time(), tz = "UTC", usetz = TRUE)
#   kv[[keys$writer]]         <- "dp_meta_pack_kv"
#   
#   # Declared system + version (best-effort)
#   ds <- (spec$declared_system %||% spec$system) %||% "custom"
#   kv[[keys$declared_system]] <- as.character(ds)[1]
#   
#   ver <- spec$version %||% "2"
#   kv[[keys$spec_version]] <- as.character(ver)[1]
#   
#   # Serialize -> gzip -> base64 (keeps types and is compact)
#   .enc_obj <- function(obj) {
#     raw <- serialize(obj, NULL)
#     raw <- memCompress(raw, type = "gzip")
#     jsonlite::base64_enc(raw)
#   }
#   
#   # Store spec fields + audit
#   spec_fields <- spec$fields
#   if (!data.table::is.data.table(spec_fields)) spec_fields <- data.table::as.data.table(spec_fields)
#   
#   audit_dt <- data.table::copy(.dp_dt(audit_dt))
#   
#   kv[[keys$spec_b64]]  <- .enc_obj(spec_fields)
#   kv[[keys$audit_b64]] <- .enc_obj(audit_dt)
#   
#   # Merge extra (caller can override defaults)
#   if (is.list(extra) && length(extra)) {
#     nms <- names(extra)
#     for (i in seq_along(extra)) {
#       nm <- nms[i]
#       if (!is.null(nm) && nzchar(nm)) {
#         kv[[nm]] <- as.character(extra[[i]])[1]
#       }
#     }
#   }
#   
#   # Normalize all values to character(1)
#   kv <- lapply(kv, function(v) {
#     v <- as.character(v)
#     if (!length(v)) NA_character_ else v[1]
#   })
#   
#   kv
# }
# 
# .dp_meta_unpack_kv <- function(kv, prefix = "dp", parse = TRUE) {
#   .dp_require("data.table")
#   .dp_require("jsonlite")
#   
#   kv <- kv %||% list()
#   if (!is.list(kv)) .dp_stop(".dp_meta_unpack_kv(): kv must be a list.")
#   
#   keys <- .dp_meta_keys(prefix)
#   
#   out <- list(
#     prefix   = prefix,
#     kv       = kv,
#     spec_dt  = NULL,
#     audit_dt = NULL
#   )
#   
#   if (!isTRUE(parse)) return(out)
#   
#   .dec_obj <- function(b64) {
#     if (is.null(b64) || !length(b64) || is.na(b64) || !nzchar(b64)) return(NULL)
#     raw <- jsonlite::base64_dec(as.character(b64)[1])
#     raw <- memDecompress(raw, type = "gzip")
#     unserialize(raw)
#   }
#   
#   spec_dt  <- .dec_obj(kv[[keys$spec_b64]])
#   audit_dt <- .dec_obj(kv[[keys$audit_b64]])
#   
#   if (!is.null(spec_dt))  out$spec_dt  <- data.table::as.data.table(spec_dt)
#   if (!is.null(audit_dt)) out$audit_dt <- data.table::as.data.table(audit_dt)
#   
#   out
# }
# 
# # -----------------------------------------------------------------------------
# # 4) Read parquet metadata robustly (prefix detection + parse)
# # -----------------------------------------------------------------------------
# dp_read_parquet_meta <- function(
#     path,
#     parse = TRUE,
#     preferred_prefix = "dp",
#     legacy_prefixes = c("klimo")
# ) {
#   .dp_require("arrow")
#   
#   schema <- arrow::read_parquet_schema(path)
#   kv_raw <- schema$metadata
#   
#   if (is.null(kv_raw) || !length(kv_raw)) {
#     return(list(prefix = preferred_prefix, kv = list(), spec_dt = NULL, audit_dt = NULL))
#   }
#   
#   # Determine prefix by presence of any keys starting with "<prefix>:"
#   prefixes <- c(preferred_prefix, legacy_prefixes)
#   prefixes <- unique(prefixes[!is.na(prefixes) & nzchar(prefixes)])
#   
#   pick <- preferred_prefix
#   nms <- names(kv_raw) %||% character()
#   
#   for (p in prefixes) {
#     if (any(startsWith(nms, paste0(p, ":")))) { pick <- p; break }
#   }
#   
#   unpack <- .dp_meta_unpack_kv(kv_raw, prefix = pick, parse = parse)
#   unpack
# }
# 
# # -----------------------------------------------------------------------------
# # 5) Dataset sidecar meta reader (uses same parquet-meta parser)
# # -----------------------------------------------------------------------------
# dp_read_dataset_meta <- function(
#     dataset_path,
#     parse = TRUE,
#     preferred_prefix = "dp",
#     legacy_prefixes = c("klimo")
# ) {
#   sidecars <- c(
#     file.path(dataset_path, paste0(preferred_prefix, "_meta.parquet")),
#     unlist(lapply(legacy_prefixes, function(p) file.path(dataset_path, paste0(p, "_meta.parquet"))), use.names = FALSE)
#   )
#   
#   sidecars <- sidecars[file.exists(sidecars)]
#   if (!length(sidecars)) {
#     return(list(prefix = preferred_prefix, kv = list(), spec_dt = NULL, audit_dt = NULL, sidecar_path = NA_character_))
#   }
#   
#   meta <- dp_read_parquet_meta(
#     path = sidecars[1],
#     parse = parse,
#     preferred_prefix = preferred_prefix,
#     legacy_prefixes = legacy_prefixes
#   )
#   meta$sidecar_path <- sidecars[1]
#   meta
# }
# 
# # -----------------------------------------------------------------------------
# # 6) dp_read_parquet (safe metadata access; attach_units path fixed)
# # -----------------------------------------------------------------------------
# dp_read_parquet <- function(
#     path,
#     decode_scaled = TRUE,
#     keep_storage = TRUE,
#     attach_units = c("declared", "none"),
#     preferred_prefix = "dp",
#     legacy_prefixes = c("klimo"),
#     ...
# ) {
#   .dp_require("arrow")
#   .dp_require("data.table")
#   
#   attach_units <- match.arg(attach_units)
#   
#   meta <- dp_read_parquet_meta(
#     path = path,
#     parse = TRUE,
#     preferred_prefix = preferred_prefix,
#     legacy_prefixes = legacy_prefixes
#   )
#   
#   dt <- arrow::read_parquet(path, as_data_frame = TRUE, ...)
#   dt <- .dp_dt(dt)
#   
#   # Decode scaled storage columns -> numeric columns (requires audit_dt)
#   if (isTRUE(decode_scaled) && !is.null(meta$audit_dt) && nrow(meta$audit_dt)) {
#     if (!exists("dp_scale_decode", mode = "function", inherits = TRUE)) {
#       .dp_stop("dp_read_parquet(): decode_scaled=TRUE but dp_scale_decode() not found.")
#     }
#     dt <- dp_scale_decode(dt, meta$audit_dt, keep_storage = isTRUE(keep_storage), overwrite = FALSE)
#   }
#   
#   # Attach units from spec (requires spec_dt + dp_units_tag_from_spec)
#   if (attach_units == "declared" && !is.null(meta$spec_dt) && nrow(meta$spec_dt)) {
#     if (!exists(".dp_spec_new", mode = "function", inherits = TRUE)) {
#       .dp_stop("dp_read_parquet(): attach_units='declared' but .dp_spec_new() not found.")
#     }
#     if (!exists("dp_units_tag_from_spec", mode = "function", inherits = TRUE)) {
#       .dp_stop("dp_read_parquet(): attach_units='declared' but dp_units_tag_from_spec() not found.")
#     }
#     
#     keys <- .dp_meta_keys(meta$prefix %||% preferred_prefix)
#     
#     declared_system <- meta$kv[[keys$declared_system]] %||% "custom"
#     version         <- meta$kv[[keys$spec_version]]    %||% "2"
#     
#     sp <- .dp_spec_new(
#       fields_dt = meta$spec_dt,
#       declared_system = as.character(declared_system)[1],
#       version = as.character(version)[1]
#     )
#     sp <- .dp_spec_resolve_declared_units(sp)
#     
#     dt <- dp_units_tag_from_spec(dt, sp, only_missing = TRUE)$dt
#   }
#   
#   list(dt = dt, meta = meta)
# }
# 
# # -----------------------------------------------------------------------------
# # 7) Dataset open (Arrow) + optional meta sidecar
# # -----------------------------------------------------------------------------
# .dp_dataset_factory_options <- function(ignore_prefixes = c("_dp_", "_klimo_"), exclude_invalid_files = TRUE) {
#   .dp_require("arrow")
#   
#   ignore_prefixes <- as.character(ignore_prefixes)
#   ignore_prefixes <- ignore_prefixes[!is.na(ignore_prefixes) & nzchar(ignore_prefixes)]
#   if (!length(ignore_prefixes)) ignore_prefixes <- NULL
#   
#   if (exists("dataset_factory_options", where = asNamespace("arrow"), mode = "function")) {
#     return(arrow::dataset_factory_options(
#       selector_ignore_prefixes = ignore_prefixes,
#       exclude_invalid_files = exclude_invalid_files
#     ))
#   }
#   
#   list(selector_ignore_prefixes = ignore_prefixes, exclude_invalid_files = exclude_invalid_files)
# }
# 
# dp_open_dataset <- function(
#     path,
#     metadata = c("sidecar", "none"),
#     ignore_prefixes = c("_dp_", "_klimo_"),
#     exclude_invalid_files = TRUE,
#     preferred_prefix = "dp",
#     legacy_prefixes = c("klimo"),
#     ...
# ) {
#   .dp_require("arrow")
#   
#   metadata <- match.arg(metadata)
#   fo <- .dp_dataset_factory_options(ignore_prefixes = ignore_prefixes, exclude_invalid_files = exclude_invalid_files)
#   
#   fml <- names(formals(arrow::open_dataset))
#   if ("factory_options" %in% fml) {
#     ds <- arrow::open_dataset(path, factory_options = fo, ...)
#   } else {
#     ds <- arrow::open_dataset(path, ...)
#   }
#   
#   if (metadata == "none") return(ds)
#   
#   meta <- dp_read_dataset_meta(
#     dataset_path = path,
#     parse = TRUE,
#     preferred_prefix = preferred_prefix,
#     legacy_prefixes = legacy_prefixes
#   )
#   
#   list(ds = ds, meta = meta)
# }
# 
# # -----------------------------------------------------------------------------
# # 8) dp_write_dataset (writes dataset + meta sidecar parquet w/ metadata kv)
# # -----------------------------------------------------------------------------
# # -----------------------------------------------------------------------------
# # dp_kv_set(): tiny utility for dynamic metadata keys
# # - Use when keys are not literal names.
# # - Returns a list suitable for .dp_meta_pack_kv(extra=...)
# # -----------------------------------------------------------------------------
# dp_kv_set <- function(...) {
#   args <- list(...)
#   if (!length(args)) return(list())
#   
#   # Accept pairs like: dp_kv_set(keys$writer, "x", keys$write_format, "parquet")
#   if (length(args) %% 2 != 0) {
#     stop("dp_kv_set(): must supply an even number of arguments: key1, value1, key2, value2, ...", call. = FALSE)
#   }
#   
#   out <- list()
#   for (i in seq(1, length(args), by = 2)) {
#     k <- as.character(args[[i]])[1]
#     v <- args[[i + 1]]
#     
#     if (is.na(k) || !nzchar(k)) next
#     
#     # normalize to character(1) to match your kv contract
#     vv <- as.character(v)
#     out[[k]] <- if (!length(vv)) NA_character_ else vv[1]
#   }
#   
#   out
# }
# 
# # -----------------------------------------------------------------------------
# # dp_write_dataset(): writes Arrow Dataset + dp sidecar meta parquet
# # -----------------------------------------------------------------------------
# dp_write_dataset <- function(
#     x,
#     path,
#     spec = c("metric", "imperial"),
#     scale_document = FALSE,
#     suffix = "_i",
#     prefix = "dp",
#     sidecar_overwrite = TRUE,
#     sidecar_compression = "zstd",
#     write_format = "parquet",
#     partitioning = NULL,
#     existing_data_behavior = NULL,
#     ...
# ) {
#   .dp_require("arrow")
#   .dp_require("data.table")
#   
#   # ----------------------------
#   # 1) Determine dataset columns WITHOUT collecting
#   # ----------------------------
#   cols <- NULL
#   
#   cols <- tryCatch({
#     if (!is.null(x$schema) && !is.null(x$schema$names)) x$schema$names else NULL
#   }, error = function(e) NULL)
#   
#   if (is.null(cols)) {
#     cols <- tryCatch({
#       if (!is.null(x$schema) && is.function(x$schema$names)) x$schema$names() else NULL
#     }, error = function(e) NULL)
#   }
#   
#   if (is.null(cols)) {
#     cols <- tryCatch({
#       if (!is.null(x$names)) x$names else NULL
#     }, error = function(e) NULL)
#   }
#   
#   if (is.null(cols)) {
#     cols <- tryCatch({
#       if (is.function(x$names)) x$names() else NULL
#     }, error = function(e) NULL)
#   }
#   
#   if (is.null(cols) || !length(cols)) {
#     .dp_stop("dp_write_dataset(): could not determine schema column names without collecting.")
#   }
#   
#   # Proxy dt for spec inference (0 rows)
#   proxy <- data.table::data.table()
#   for (nm in cols) proxy[[nm]] <- logical(0)
#   
#   # ----------------------------
#   # 2) Resolve spec
#   # ----------------------------
#   if (inherits(spec, "dp_spec")) {
#     sp <- spec
#   } else {
#     spec <- match.arg(spec)
#     if (!exists("dp_spec", mode = "function", inherits = TRUE)) {
#       .dp_stop("dp_write_dataset(): dp_spec() not found.")
#     }
#     sp <- dp_spec(proxy, declared_system = spec, include_unmatched = TRUE)
#   }
#   sp <- .dp_spec_resolve_declared_units(sp)
#   
#   # ----------------------------
#   # 3) Build dataset-level audit (DOCUMENTATION ONLY)
#   # ----------------------------
#   f <- data.table::copy(sp$fields)
#   for (nm in c("concept","declared_units","metric_units","imperial_units","writer_scale",
#                "encoding","encoding_scale","encoding_base_units")) {
#     if (!(nm %in% names(f))) f[, (nm) := NA]
#   }
#   
#   audit <- f[, .(
#     column              = as.character(column),
#     concept             = as.character(concept),
#     declared_units      = as.character(declared_units),
#     metric_units        = as.character(metric_units),
#     imperial_units      = as.character(imperial_units),
#     writer_scale        = suppressWarnings(as.numeric(writer_scale)),
#     encoding            = as.character(encoding),
#     encoding_scale      = suppressWarnings(as.numeric(encoding_scale)),
#     encoding_base_units = as.character(encoding_base_units)
#   )]
#   
#   audit[, `:=`(
#     storage_column = data.table::fifelse(
#       isTRUE(scale_document) &
#         (is.na(encoding) | encoding == "none") &
#         is.finite(writer_scale) & writer_scale != 0,
#       paste0(column, suffix),
#       NA_character_
#     ),
#     did_scale      = FALSE,
#     did_tag        = FALSE,
#     observed_units = NA_character_,
#     mode           = "dataset_document",
#     scale_enabled  = isTRUE(scale_document),
#     drop_original  = NA,
#     suffix         = as.character(suffix)
#   )]
#   
#   # ----------------------------
#   # 4) Write dataset itself
#   # ----------------------------
#   fmt <- tolower(as.character(write_format)[1])
#   if (!fmt %in% c("parquet")) {
#     .dp_stop(sprintf("dp_write_dataset(): unsupported write_format='%s' (only 'parquet').", fmt))
#   }
#   
#   write_args <- list(
#     dataset      = x,
#     path         = path,
#     format       = "parquet",
#     partitioning = partitioning
#   )
#   if (!is.null(existing_data_behavior)) write_args$existing_data_behavior <- existing_data_behavior
#   write_args <- c(write_args, list(...))
#   
#   do.call(arrow::write_dataset, write_args)
#   
#   # ----------------------------
#   # 5) Write sidecar meta parquet (metadata-only file)
#   # ----------------------------
#   sidecar_path <- file.path(path, paste0(prefix, "_meta.parquet"))
#   
#   if (file.exists(sidecar_path) && !isTRUE(sidecar_overwrite)) {
#     return(invisible(list(
#       path = path,
#       sidecar_path = sidecar_path,
#       sidecar_written = FALSE,
#       spec = sp,
#       audit = audit
#     )))
#   }
#   
#   keys <- .dp_meta_keys(prefix)
#   
#   # Dynamic-key extra kv (FIXED)
#   extra <- dp_kv_set(
#     keys$writer,                  "dp_write_dataset_v3",
#     keys$sidecar_compression,      as.character(sidecar_compression)[1],
#     keys$write_format,             as.character(write_format)[1],
#     keys$partitioning,             if (is.null(partitioning)) NA_character_ else paste(partitioning, collapse = ","),
#     keys$existing_data_behavior,   if (is.null(existing_data_behavior)) NA_character_ else as.character(existing_data_behavior)[1]
#   )
#   
#   kv <- .dp_meta_pack_kv(spec = sp, audit_dt = audit, extra = extra, prefix = prefix)
#   
#   # Minimal payload; metadata is the important part
#   meta_dt <- data.table::data.table(dp_meta = 1L)
#   tab <- arrow::as_arrow_table(meta_dt)
#   
#   # Attach kv into schema metadata
#   if (!is.null(tab$ReplaceSchemaMetadata) && is.function(tab$ReplaceSchemaMetadata)) {
#     tab <- tab$ReplaceSchemaMetadata(kv)
#   } else {
#     # Fallback: Table$metadata setter exists in some Arrow versions
#     tab$metadata <- kv
#   }
#   
#   arrow::write_parquet(tab, sink = sidecar_path, compression = sidecar_compression)
#   
#   invisible(list(
#     path = path,
#     sidecar_path = sidecar_path,
#     sidecar_written = TRUE,
#     spec = sp,
#     audit = audit,
#     kv = kv
#   ))
# }
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
