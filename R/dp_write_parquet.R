# =============================================================================
# jj / Klimo Parquet I/O — WRITING (units + scale spec + audit metadata)
# =============================================================================
# This file defines:
#   - dp_write_parquet(): main writer (hybrid validate/convert + audit metadata)
#   - dp_spec_preview(): inspect how a spec matches a dataset (units + scaling plan)
#   - dp_spec_default(): build a default spec tailored to the columns present
#   - dp_spec_set_units(): easy post-hoc edits to a spec’s units
#   - dp_spec_set_scale(): easy post-hoc edits to a spec’s scale
#
# S3 objects:
#   - class "klimo_spec" (list with $spec_dt + $system + $version)
#
# Notes:
#   - All tabular objects are data.table.
#   - Metadata is written into Parquet key_value_metadata (strings).
# =============================================================================

# ---- dependencies (package-safe) ---------------------------------------------

.klimo_require <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(sprintf("Required package '%s' is not installed.", pkg), call. = FALSE)
  }
}

# ---- internal: small utilities ----------------------------------------------

.klimo_is_scalar_chr <- function(x) is.character(x) && length(x) == 1L && !is.na(x)

.klimo_trim <- function(x) sub("^\\s+|\\s+$", "", x)

.klimo_norm_name <- function(x) {
  # normalize for matching:
  #   - lowercase
  #   - trim
  #   - collapse non-alnum to single underscore
  x <- tolower(as.character(x))
  x <- .klimo_trim(x)
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  x
}

.klimo_dt <- function(x) {
  .klimo_require("data.table")
  if (inherits(x, "data.table")) return(x)
  data.table::as.data.table(x)
}

.klimo_stop <- function(...) stop(sprintf(...), call. = FALSE)

.klimo_warn <- function(...) warning(sprintf(...), call. = FALSE, immediate. = TRUE)

.klimo_msg <- function(...) message(sprintf(...))

.klimo_json <- function(x, pretty = FALSE) {
  .klimo_require("jsonlite")
  jsonlite::toJSON(
    x,
    auto_unbox = TRUE,
    null = "null",
    digits = NA,
    pretty = isTRUE(pretty)
  )
}

.klimo_from_json <- function(x) {
  .klimo_require("jsonlite")
  jsonlite::fromJSON(x, simplifyVector = TRUE)
}



#' Infer spec system from presence of metric/imperial units columns
#' @keywords internal
.klimo_infer_spec_system <- function(spec_dt) {
  # Returns one of: "metric", "imperial", "custom"
  has_m_col <- "metric_units" %in% names(spec_dt)
  has_i_col <- "imperial_units" %in% names(spec_dt)
  
  # If neither column exists, cannot infer
  if (!has_m_col && !has_i_col) return("custom")
  
  m_any <- FALSE
  i_any <- FALSE
  
  if (has_m_col) {
    mu <- as.character(spec_dt[["metric_units"]])
    m_any <- any(!is.na(mu) & nzchar(mu))
  }
  if (has_i_col) {
    iu <- as.character(spec_dt[["imperial_units"]])
    i_any <- any(!is.na(iu) & nzchar(iu))
  }
  
  if (m_any && !i_any) return("metric")
  if (i_any && !m_any) return("imperial")
  "custom"
}


# =============================================================================
# Spec object (S3): klimo_spec
# =============================================================================

#' Create a Klimo field specification object (S3)
#'
#' A `klimo_spec` stores a per-column specification table (as a `data.table`)
#' plus a declared unit system (`"metric"` or `"imperial"`). The spec is used by
#' `dp_write_parquet()` to:
#'   - determine declared units per column,
#'   - optionally validate/convert values to match declared units,
#'   - optionally create scaled integer columns (writer scaling),
#'   - describe already-encoded columns (e.g., lat_idx_10) without re-scaling,
#'   - write spec + audit metadata into Parquet schema key_value_metadata.
#'
#' Required column:
#' - column
#'
#' Recommended columns:
#' - concept
#' - metric_units, imperial_units (if you want both systems available)
#' - declared_units (optional; if present, used directly)
#'
#' Scaling / encoding:
#' - writer_scale: numeric multiplier used to create a new integer column at write time
#' - encoding: "none" or "scaled_int"
#' - encoding_scale: numeric scale used in storage (e.g., 10 means value_on_disk = true_value * 10)
#' - encoding_base_units: units of the decoded quantity (e.g., "deg")
#'
#' @param spec_dt data.table/data.frame describing the spec.
#' @param declared_system "metric" or "imperial".
#' @param version version string stored in metadata.
#' @return klimo_spec object
#' @keywords internal
#' @keywords internal
.klimo_spec_new <- function(
    spec_dt,
    declared_system = c("metric", "imperial", "custom"),
    version = "1",
    system = NULL,      # legacy alias
    ...
) {
  .klimo_require("data.table")
  
  # ---- handle legacy arg name: system= ----
  if (!is.null(system) && is.null(declared_system)) {
    declared_system <- system
  }
  if (!is.null(system) && !missing(declared_system)) {
    # If both provided, prefer declared_system but warn once (optional)
    # .klimo_warn(".klimo_spec_new(): both declared_system= and system= provided; using declared_system.")
  }
  
  # match.arg, but also allow NULL passed through
  declared_system <- match.arg(as.character(declared_system), c("metric", "imperial", "custom"))
  
  spec_dt <- .klimo_dt(spec_dt)
  if (!("column" %in% names(spec_dt))) .klimo_stop("spec_dt must contain a 'column' field.")
  
  spec_dt[, column := as.character(column)]
  spec_dt[, column_norm := .klimo_norm_name(column)]
  
  # Ensure expected fields exist
  if (!("concept" %in% names(spec_dt)))            spec_dt[, concept := NA_character_]
  if (!("metric_units" %in% names(spec_dt)))       spec_dt[, metric_units := NA_character_]
  if (!("imperial_units" %in% names(spec_dt)))     spec_dt[, imperial_units := NA_character_]
  if (!("declared_units" %in% names(spec_dt)))     spec_dt[, declared_units := NA_character_]
  
  # Backward compatibility: suggested_scale -> writer_scale
  if (!("writer_scale" %in% names(spec_dt))) {
    if ("suggested_scale" %in% names(spec_dt)) {
      spec_dt[, writer_scale := suppressWarnings(as.numeric(suggested_scale))]
    } else {
      spec_dt[, writer_scale := as.numeric(NA)]
    }
  }
  
  # Encoding (already-scaled storage)
  if (!("encoding" %in% names(spec_dt)))             spec_dt[, encoding := "none"]
  if (!("encoding_scale" %in% names(spec_dt)))       spec_dt[, encoding_scale := as.numeric(NA)]
  if (!("encoding_base_units" %in% names(spec_dt)))  spec_dt[, encoding_base_units := NA_character_]
  
  # Types
  spec_dt[, concept := as.character(concept)]
  spec_dt[, metric_units := as.character(metric_units)]
  spec_dt[, imperial_units := as.character(imperial_units)]
  spec_dt[, declared_units := as.character(declared_units)]
  spec_dt[, writer_scale := suppressWarnings(as.numeric(writer_scale))]
  spec_dt[, encoding := as.character(encoding)]
  spec_dt[, encoding_scale := suppressWarnings(as.numeric(encoding_scale))]
  spec_dt[, encoding_base_units := as.character(encoding_base_units)]
  
  # Normalize encoding values
  spec_dt[is.na(encoding) | !nzchar(encoding), encoding := "none"]
  bad_enc <- setdiff(unique(spec_dt$encoding), c("none", "scaled_int"))
  if (length(bad_enc)) {
    .klimo_stop(
      "Unsupported encoding value(s): %s. Allowed: 'none', 'scaled_int'.",
      paste(bad_enc, collapse = ", ")
    )
  }
  
  # If encoding is scaled_int, enforce encoding_scale is present and finite/non-zero
  idx_enc <- which(spec_dt$encoding == "scaled_int")
  if (length(idx_enc)) {
    bad <- idx_enc[!is.finite(spec_dt$encoding_scale[idx_enc]) | spec_dt$encoding_scale[idx_enc] == 0]
    if (length(bad)) {
      .klimo_stop(
        "Spec has encoding='scaled_int' but missing/invalid encoding_scale for column(s): %s",
        paste(spec_dt$column[bad], collapse = ", ")
      )
    }
  }
  
  out <- list(spec_dt = spec_dt, declared_system = declared_system, version = as.character(version))
  class(out) <- "klimo_spec"
  out
}


#' @export
print.klimo_spec <- function(x, ...) {
  .klimo_require("data.table")
  dt <- x$spec_dt
  n <- nrow(dt)
  n_concept <- dt[!is.na(concept) & nzchar(concept), .N]
  n_units_m <- dt[!is.na(metric_units) & nzchar(metric_units), .N]
  n_units_i <- dt[!is.na(imperial_units) & nzchar(imperial_units), .N]
  n_scale <- dt[!is.na(suggested_scale), .N]
  cat(sprintf(
    "<klimo_spec> system=%s; version=%s\n  rows=%d; concept=%d; metric_units=%d; imperial_units=%d; suggested_scale=%d\n",
    x$system, x$version, n, n_concept, n_units_m, n_units_i, n_scale
  ))
  invisible(x)
}

# =============================================================================
# Default spec catalog + builder (tailored to dataset columns)
# =============================================================================

# Internal catalog: concepts + units + default suggested scale + synonyms.
# You can extend this over time safely.
.klimo_spec_catalog <- function() {
  .klimo_require("data.table")
  
  dt <- data.table::rbindlist(list(
    data.table::data.table(concept="air_temp", metric_units="degC", imperial_units="degF", suggested_scale=100L,
                           synonyms=list(c("ta","temp","temperature","airtemp","air_temp","ta_2m","ta10m","ta_in"))),
    data.table::data.table(concept="dewpoint", metric_units="degC", imperial_units="degF", suggested_scale=100L,
                           synonyms=list(c("td","dewpoint","dew_point","td_in"))),
    data.table::data.table(concept="relhum", metric_units="percent", imperial_units="percent", suggested_scale=100L,
                           synonyms=list(c("rh","relh","relhum","rel_hum","RH","rh_in"))),
    
    
    data.table::data.table(concept="wind_speed", metric_units="m/s", imperial_units="mph", suggested_scale=100L,
                           synonyms=list(c("wind","speed","wind2m","wind10m","speed2m","speed10m","gust","gust10m",'wind10m_mph','gust10m_mph'))),
    
    
    data.table::data.table(concept="wind_dir", metric_units="deg", imperial_units="deg", suggested_scale=10L,
                           synonyms=list(c("dd","dir","wdir","wdir10m","WDIR","DD"))),
    
    
    data.table::data.table(concept="visibility", metric_units="m", imperial_units="m", suggested_scale=1L,
                           synonyms=list(c("vis","visibility","VIS"))),
    data.table::data.table(concept="lat", metric_units="deg", imperial_units="deg", suggested_scale=10000L,
                           synonyms=list(c("lat","latitude"))),
    data.table::data.table(concept="lon", metric_units="deg", imperial_units="deg", suggested_scale=10000L,
                           synonyms=list(c("lon","longitude","lng"))),
    data.table::data.table(concept="solar_radiation", metric_units="W/m^2", imperial_units="W/m^2", suggested_scale=1L,
                           synonyms=list(c("solar","solarsun","solarshade","solrad","srad","FSRDINS","DSRDINS"))),
    data.table::data.table(concept="cloud_total", metric_units="percent", imperial_units="percent", suggested_scale=100L,
                           synonyms=list(c("tcdc","cldfrac","cldfraq","CLDFRAC"))),
    data.table::data.table(concept="cloud_low", metric_units="percent", imperial_units="percent", suggested_scale=100L,
                           synonyms=list(c("lcdc"))),
    data.table::data.table(concept="cloud_mid", metric_units="percent", imperial_units="percent", suggested_scale=100L,
                           synonyms=list(c("mcdc"))),
    data.table::data.table(concept="cloud_high", metric_units="percent", imperial_units="percent", suggested_scale=100L,
                           synonyms=list(c("hcdc"))),
    data.table::data.table(concept="sfcr", metric_units="m", imperial_units="m", suggested_scale=1000L,
                           synonyms=list(c("sfcr","z0","roughness","roughness_length"))),
    data.table::data.table(concept="pressure", metric_units="hPa", imperial_units="hPa", suggested_scale=10L,
                           synonyms=list(c("pres","pressure","mslp","slp"))),
    data.table::data.table(concept="wbgt", metric_units="degC", imperial_units="degF", suggested_scale=100L,
                           synonyms=list(c("wbgt","nwb","tg","wb")))
  ), use.names = TRUE, fill = TRUE)
  
  # harden list column
  dt[, synonyms := lapply(synonyms, function(z) if (is.null(z)) character() else as.character(z))]
  dt
}


#' Build a default Klimo spec tailored to a dataset
#'
#' `dp_spec_default()` takes a dataset (data.frame/data.table) and returns a `klimo_spec`
#' whose rows match the dataset columns. Columns not recognized by the catalog are kept
#' with `NA` concept/units/scale (so you can still attach partial metadata).
#'
#' @param x A data.frame/data.table. Only column names are used.
#' @param system `"metric"` or `"imperial"`. This is a *label* used when selecting
#'   declared units (metric_units vs imperial_units) by downstream functions.
#' @param include_unmatched If TRUE, include all columns (unmatched ones get NA fields).
#'   If FALSE, return only the matched subset.
#'
#' @return A `klimo_spec` object.
#' @export
dp_spec_default <- function(x, system = c("metric", "imperial"), include_unmatched = TRUE) {
  .klimo_require("data.table")
  system <- match.arg(system)
  x <- .klimo_dt(x)
  
  cols <- names(x)
  cols_norm <- .klimo_norm_name(cols)
  
  cat_dt <- .klimo_spec_catalog()
  
  # Expand catalog synonyms robustly (no .N hacks)
  map <- cat_dt[, {
    syn <- unlist(synonyms, use.names = FALSE)
    if (!length(syn)) syn <- character()
    .(synonym = syn)
  }, by = .(concept, metric_units, imperial_units, suggested_scale)]
  
  map[, synonym_norm := .klimo_norm_name(synonym)]
  
  # First match wins (match() gives first occurrence)
  idx <- match(cols_norm, map$synonym_norm)
  matched <- !is.na(idx)
  
  out_dt <- data.table::data.table(
    column = cols,
    column_norm = cols_norm,
    concept = ifelse(matched, map$concept[idx], NA_character_),
    metric_units = ifelse(matched, map$metric_units[idx], NA_character_),
    imperial_units = ifelse(matched, map$imperial_units[idx], NA_character_),
    suggested_scale = ifelse(matched, as.integer(map$suggested_scale[idx]), as.integer(NA))
  )
  
  if (!isTRUE(include_unmatched)) {
    out_dt <- out_dt[!is.na(concept) & nzchar(concept)]
  }
  
  .klimo_spec_new(out_dt, system = system, version = "1")
}

# =============================================================================
# Spec editing helpers (exported)
# =============================================================================

#' Update units in a Klimo spec (post-hoc)
#'
#' Convenience helper to modify the units for one or more columns after building a spec.
#' This does *not* convert any data; it just edits the spec.
#'
#' @param spec A `klimo_spec` object.
#' @param columns Character vector of column names to edit. Matching is case-insensitive.
#' @param metric_units,imperial_units Replacement units (character scalar).
#'
#' @return The modified `klimo_spec`.
#' @export
dp_spec_set_units <- function(spec, columns, metric_units = NULL, imperial_units = NULL) {
  if (!inherits(spec, "klimo_spec")) .klimo_stop("spec must be a klimo_spec.")
  dt <- data.table::copy(spec$spec_dt)
  
  stopifnot(is.character(columns), length(columns) >= 1L)
  target_norm <- .klimo_norm_name(columns)
  dt[, .hit := column_norm %in% target_norm]
  
  if (!any(dt$.hit)) {
    .klimo_warn("dp_spec_set_units(): no columns matched. Provided: %s", paste(columns, collapse = ", "))
  }
  
  if (!is.null(metric_units)) {
    if (!.klimo_is_scalar_chr(metric_units)) .klimo_stop("metric_units must be a character scalar.")
    dt[.hit == TRUE, metric_units := ..metric_units]
  }
  if (!is.null(imperial_units)) {
    if (!.klimo_is_scalar_chr(imperial_units)) .klimo_stop("imperial_units must be a character scalar.")
    dt[.hit == TRUE, imperial_units := ..imperial_units]
  }
  
  dt[, .hit := NULL]
  spec$spec_dt <- dt
  spec
}


#' Update scaling in a Klimo spec (post-hoc)
#'
#' Convenience helper to modify `suggested_scale` for one or more columns after
#' building a spec. This edits the spec only; it does not scale any data.
#'
#' @param spec A `klimo_spec` object.
#' @param columns Character vector of column names to edit. Matching is case-insensitive
#'   (via normalized column names).
#' @param suggested_scale Integer scalar scale factor (e.g., `100L`) or `NA` to clear.
#'
#' @return The modified `klimo_spec` object.
#' @export
dp_spec_set_scale <- function(spec, columns, suggested_scale) {
  if (!inherits(spec, "klimo_spec")) .klimo_stop("spec must be a klimo_spec.")
  .klimo_require("data.table")
  
  dt <- data.table::copy(spec$spec_dt)
  
  stopifnot(is.character(columns), length(columns) >= 1L)
  target_norm <- .klimo_norm_name(columns)
  dt[, .hit := column_norm %in% target_norm]
  
  if (!any(dt$.hit)) {
    .klimo_warn(
      "dp_spec_set_scale(): no columns matched. Provided: %s",
      paste(columns, collapse = ", ")
    )
  }
  
  if (length(suggested_scale) != 1L) {
    .klimo_stop("suggested_scale must be a scalar (integer or NA).")
  }
  
  new_scale <- suggested_scale
  if (!is.na(new_scale)) new_scale <- as.integer(new_scale)
  
  dt[.hit == TRUE, suggested_scale := new_scale]
  
  dt[, .hit := NULL]
  spec$spec_dt <- dt
  spec
}
# dp_spec_set_scale <- function(spec, columns, suggested_scale) {
#   if (!inherits(spec, "klimo_spec")) .klimo_stop("spec must be a klimo_spec.")
#   dt <- data.table::copy(spec$spec_dt)
#   
#   stopifnot(is.character(columns), length(columns) >= 1L)
#   target_norm <- .klimo_norm_name(columns)
#   dt[, .hit := column_norm %in% target_norm]
#   
#   if (!any(dt$.hit)) {
#     .klimo_warn("dp_spec_set_scale(): no columns matched. Provided: %s", paste(columns, collapse = ", "))
#   }
#   
#   if (length(suggested_scale) != 1L) .klimo_stop("suggested_scale must be a scalar (integer or NA).")
#   if (!is.na(suggested_scale)) suggested_scale <- as.integer(suggested_scale)
#   
#   dt[.hit == TRUE, suggested_scale := ..suggested_scale]
#   
#   dt[, .hit := NULL]
#   spec$spec_dt <- dt
#   spec
# }


# =============================================================================
# Spec normalization (accept "metric"/"imperial"/NULL/custom table/klimo_spec)
# =============================================================================

#' Normalize field_spec input into a klimo_spec (or NULL)
#' @keywords internal
.klimo_normalize_field_spec <- function(x, field_spec, declared_system = c("metric", "imperial")) {
  # Returns klimo_spec or NULL
  declared_system <- match.arg(declared_system)
  
  if (is.null(field_spec)) return(NULL)
  
  if (.klimo_is_scalar_chr(field_spec)) {
    if (field_spec %in% c("metric", "imperial")) {
      # default spec builder must return a data.table with at least 'column',
      # and ideally concept + metric_units + imperial_units + (optional) suggested_scale/writer_scale
      spec_dt <- dp_spec_default(x, system = field_spec, include_unmatched = TRUE)$spec_dt
      return(.klimo_spec_new(spec_dt, declared_system = field_spec, version = "1"))
    }
    .klimo_stop("field_spec must be NULL, 'metric', 'imperial', a klimo_spec, or a data.table/data.frame.")
  }
  
  if (inherits(field_spec, "klimo_spec")) {
    # Trust its declared_system; ignore declared_system arg for normalization
    return(field_spec)
  }
  
  if (inherits(field_spec, "data.table") || inherits(field_spec, "data.frame")) {
    # If user provided a dt/df, treat it as a custom spec table but STILL needs a declared_system.
    # We default to declared_system (argument) rather than inventing.
    return(.klimo_spec_new(field_spec, declared_system = declared_system, version = "1"))
  }
  
  .klimo_stop("Unsupported field_spec type: %s", paste(class(field_spec), collapse = "/"))
}



# =============================================================================
# Preview: resolve declared units + show scaling/encoding plan
# =============================================================================

#' Preview how a spec applies to a dataset (coverage + units + scaling/encoding)
#'
#' @param x data.frame/data.table
#' @param field_spec klimo_spec or spec dt/df or "metric"/"imperial"
#' @param declared_system Only used if field_spec is a dt/df (not a klimo_spec).
#' @param scaled Whether writer scaling is requested (controls scale_status output).
#' @param scaled_suffix Suffix for writer-scaled integer columns.
#' @return data.table preview
#' @export
dp_spec_preview <- function(
    x,
    field_spec,
    declared_system = c("metric", "imperial"),
    scaled = FALSE,
    scaled_suffix = "_i"
) {
  .klimo_require("data.table")
  
  x <- .klimo_dt(x)
  declared_system <- match.arg(declared_system)
  
  spec <- .klimo_normalize_field_spec(x, field_spec, declared_system = declared_system)
  if (is.null(spec)) {
    # minimal preview without spec
    out <- data.table::data.table(column = names(x))
    out[, `:=`(
      concept = NA_character_,
      declared_units = NA_character_,
      metric_units = NA_character_,
      imperial_units = NA_character_,
      writer_scale = as.numeric(NA),
      encoding = "none",
      encoding_scale = as.numeric(NA),
      encoding_base_units = NA_character_,
      scale_status = "none",
      scaled_column = NA_character_
    )]
    out[, column_norm := .klimo_norm_name(column)]
    return(out[])
  }
  
  s <- data.table::copy(spec$spec_dt)
  
  # Ensure unique normalized key to avoid ambiguous joins
  # If duplicates exist, keep first and warn
  if (anyDuplicated(s$column_norm)) {
    dups <- unique(s$column[duplicated(s$column_norm)])
    .klimo_warn(".klimo_spec_new(): duplicate column names after normalization; keeping first. Duplicates: %s",
                paste(dups, collapse = ", "))
    data.table::setkey(s, column_norm)
    s <- s[!duplicated(column_norm)]
  }
  
  # Base: one row per column in x
  out <- data.table::data.table(column = names(x))
  out[, column_norm := .klimo_norm_name(column)]
  
  # Join spec
  data.table::setkey(s, column_norm)
  data.table::setkey(out, column_norm)
  out <- s[out]
  
  # Resolve declared_units:
  # - if declared_units is already provided, use it;
  # - else use metric_units or imperial_units based on spec$declared_system
  sys <- spec$declared_system
  out[, declared_units := data.table::fifelse(
    !is.na(declared_units) & nzchar(declared_units),
    declared_units,
    if (identical(sys, "metric")) metric_units else imperial_units
  )]
  
  # Writer scaling plan (only if scaled=TRUE and encoding is not scaled_int)
  out[, scaled_column := NA_character_]
  out[, scale_status := "none"]
  
  if (isTRUE(scaled)) {
    out[is.finite(writer_scale) & writer_scale != 0 & encoding == "none", `:=`(
      scaled_column = paste0(column, scaled_suffix),
      scale_status  = "writer_scale_planned"
    )]
    out[encoding == "scaled_int", scale_status := "already_encoded"]
  } else {
    out[encoding == "scaled_int", scale_status := "already_encoded"]
  }
  
  # Final ordering
  out[, .(
    column,
    concept,
    declared_units,
    metric_units,
    imperial_units,
    writer_scale,
    encoding,
    encoding_scale,
    encoding_base_units,
    scale_status,
    scaled_column,
    column_norm
  )][]
}



# =============================================================================
# Metadata assembly (Option 5: declared vs observed vs status)
# =============================================================================

.klimo_collect_unit_observed <- function(x) {
  # x is data.table
  .klimo_require("data.table")
  cols <- names(x)
  obs <- vapply(cols, function(nm) {
    u <- attr(x[[nm]], "unit", exact = TRUE)
    if (is.null(u)) NA_character_ else as.character(u)
  }, character(1))
  data.table::data.table(column = cols, observed_units = obs)
}

# .klimo_build_audit_dt <- function(preview_dt, observed_dt, unit_mode, did_convert_map, did_scale_map) {
#   .klimo_require("data.table")
#   
#   dt <- data.table::copy(preview_dt)
#   dt <- dt[, .(column, concept, declared_units = units, suggested_scale, scale_status, scaled_column)]
#   
#   # join observed units
#   dt <- dt[observed_dt, on = "column"]
#   if (!("observed_units" %in% names(dt))) dt[, observed_units := NA_character_]
#   
#   # unit status (declared vs observed)
#   dt[, unit_status := data.table::data.table::fifelse(
#     is.na(declared_units) | !nzchar(declared_units), "no_declared_unit",
#     data.table::fifelse(
#       is.na(observed_units) | !nzchar(observed_units), "missing_observed_unit",
#       data.table::fifelse(observed_units == declared_units, "ok", "mismatch")
#     )
#   )]
#   
#   # ---- conversions ----
#   dt[, did_convert := FALSE]
#   if (!is.null(did_convert_map) && nrow(did_convert_map)) {
#     # join did_convert (name is did_convert, NOT i.did_convert)
#     dt <- dt[did_convert_map, on = "column"]
#     if (!("did_convert" %in% names(dt))) dt[, did_convert := FALSE]
#     dt[is.na(did_convert), did_convert := FALSE]
#     dt[did_convert == TRUE, unit_status := "converted"]
#   }
#   
#   # ---- scaling ----
#   dt[, did_scale := FALSE]
#   if (!is.null(did_scale_map) && nrow(did_scale_map)) {
#     dt <- dt[did_scale_map, on = "column"]
#     if (!("did_scale" %in% names(dt))) dt[, did_scale := FALSE]
#     dt[is.na(did_scale), did_scale := FALSE]
#     dt[did_scale == TRUE, scale_status := "scaled"]
#   }
#   
#   dt[, unit_mode := as.character(unit_mode)]
#   dt[]
# }


# =============================================================================
# Audit builder: stable fields; no implicit references to "unit_status"
# =============================================================================

.klimo_build_audit_dt <- function(prev, obs_units_dt, unit_mode, did_convert_map, did_scale_map, did_tag_map = NULL) {
  .klimo_require("data.table")
  prev <- .klimo_dt(prev)
  
  audit <- prev[, .(
    column = as.character(column),
    concept = as.character(concept),
    declared_units = as.character(declared_units),
    metric_units = as.character(metric_units),
    imperial_units = as.character(imperial_units),
    writer_scale = suppressWarnings(as.numeric(writer_scale)),
    encoding = as.character(encoding),
    encoding_scale = suppressWarnings(as.numeric(encoding_scale)),
    encoding_base_units = as.character(encoding_base_units)
  )]
  
  # observed units
  if (!is.null(obs_units_dt) && nrow(obs_units_dt)) {
    obs_units_dt <- .klimo_dt(obs_units_dt)
    # expects obs_units_dt has: column, observed_units
    audit <- obs_units_dt[audit, on = "column"]
  } else {
    audit[, observed_units := NA_character_]
  }
  
  # tag map (optional)
  if (!is.null(did_tag_map) && nrow(did_tag_map)) {
    did_tag_map <- .klimo_dt(did_tag_map)[, .(column = as.character(column), did_tag = as.logical(did_tag))]
    audit <- did_tag_map[audit, on = "column"]
  } else {
    audit[, did_tag := FALSE]
  }
  
  # conversion flags
  if (!is.null(did_convert_map) && nrow(did_convert_map)) {
    did_convert_map <- .klimo_dt(did_convert_map)[, .(column = as.character(column), did_convert = as.logical(did_convert))]
    audit <- did_convert_map[audit, on = "column"]
  } else {
    audit[, did_convert := FALSE]
  }
  
  # scaling flags + scaled column name
  if (!is.null(did_scale_map) && nrow(did_scale_map)) {
    did_scale_map <- .klimo_dt(did_scale_map)[, .(
      column = as.character(column),
      scaled_column = as.character(scaled_column),
      writer_scale = suppressWarnings(as.numeric(writer_scale)),
      did_scale = as.logical(did_scale)
    )]
    audit <- did_scale_map[audit, on = "column"]
  } else {
    audit[, `:=`(scaled_column = NA_character_, did_scale = FALSE)]
  }
  
  audit[, unit_mode := as.character(unit_mode)]
  audit[]
}


# =============================================================================
# Metadata packing/unpacking
# =============================================================================

.klimo_metadata_kv <- function(spec, prev, audit, extra = list()) {
  .klimo_require("jsonlite")
  .klimo_require("data.table")
  
  spec_dt <- data.table::copy(spec$spec_dt)
  # Drop column_norm from stored json to reduce noise
  if ("column_norm" %in% names(spec_dt)) spec_dt[, column_norm := NULL]
  
  kv <- list(
    "klimo:spec_version" = as.character(spec$version),
    "klimo:declared_system" = as.character(spec$declared_system),
    "klimo:field_spec_json" = jsonlite::toJSON(spec_dt, dataframe = "rows", auto_unbox = TRUE, na = "null"),
    "klimo:audit_json" = jsonlite::toJSON(data.table::copy(audit), dataframe = "rows", auto_unbox = TRUE, na = "null")
  )
  
  # Merge extras (force string values)
  if (length(extra)) {
    for (nm in names(extra)) kv[[nm]] <- as.character(extra[[nm]])
  }
  
  # Arrow wants named character vector/list. Ensure all values are length-1 character.
  kv <- lapply(kv, function(v) as.character(v[[1]]))
  kv
}


# =============================================================================
# Scaling (integer columns) — applied during write if scaled=TRUE
# =============================================================================
# 
# .klimo_scale_apply <- function(
#     x,
#     preview_dt,
#     scaled_suffix = "_i",
#     drop_original = TRUE
# ) {
#   .klimo_require("data.table")
#   dt <- data.table::copy(x)
#   did_scale <- data.table::data.table(column = character(), did_scale = logical())
#   
#   plan <- preview_dt[scale_status %in% c("will_scale") & !is.na(suggested_scale)]
#   if (!nrow(plan)) return(list(dt = dt, did_scale = did_scale))
#   
#   for (i in seq_len(nrow(plan))) {
#     col <- plan$column[i]
#     sc  <- as.integer(plan$suggested_scale[i])
#     out_col <- paste0(col, scaled_suffix)
#     
#     if (!(col %in% names(dt))) next
#     
#     # Only scale numeric/integer; leave others untouched but record no-op
#     if (!is.numeric(dt[[col]]) && !is.integer(dt[[col]])) {
#       did_scale <- data.table::rbindlist(list(did_scale, data.table::data.table(column = col, did_scale = FALSE)))
#       next
#     }
#     
#     # preserve NA; scale then round; store as integer64 if needed
#     v <- dt[[col]]
#     v_scaled <- suppressWarnings(round(v * sc))
#     # keep as integer if safe; else keep numeric (arrow will store double) — you can tighten later
#     if (all(is.na(v_scaled) | (v_scaled >= .Machine$integer.min & v_scaled <= .Machine$integer.max))) {
#       v_scaled <- as.integer(v_scaled)
#     }
#     dt[[out_col]] <- v_scaled
#     
#     if (isTRUE(drop_original)) dt[[col]] <- NULL
#     
#     did_scale <- data.table::rbindlist(list(did_scale, data.table::data.table(column = col, did_scale = TRUE)))
#   }
#   
#   list(dt = dt, did_scale = did_scale)
# }


# =============================================================================
# Scaling apply: only for writer_scale columns; skip already-encoded columns
# =============================================================================

.klimo_scale_apply <- function(dt, prev, scaled_suffix = "_i", drop_original = TRUE) {
  .klimo_require("data.table")
  dt <- data.table::copy(.klimo_dt(dt))
  prev <- .klimo_dt(prev)
  
  # plan: writer scaling only (encoding == "none")
  plan <- prev[
    encoding == "none" &
      is.finite(writer_scale) & writer_scale != 0,
    .(
      column = as.character(column),
      writer_scale = as.numeric(writer_scale),
      scaled_column = paste0(as.character(column), scaled_suffix)
    )
  ]
  
  if (!nrow(plan)) {
    # return an empty did_scale map with expected columns
    plan <- data.table::data.table(
      column = character(),
      writer_scale = numeric(),
      scaled_column = character(),
      did_scale = logical()
    )
    return(list(dt = dt, did_scale = plan))
  }
  
  plan[, did_scale := FALSE]
  
  for (i in seq_len(nrow(plan))) {
    base <- plan$column[i]
    sc   <- plan$writer_scale[i]
    scol <- plan$scaled_column[i]
    
    if (!base %in% names(dt)) next
    
    v <- dt[[base]]
    # Only scale numeric/integer-like. If already integer, still allow but it’s usually redundant.
    if (!(is.numeric(v) || is.integer(v))) next
    if (!is.finite(sc) || sc == 0) next
    
    # Create scaled integer column
    dt[[scol]] <- as.integer(round(v * sc))
    plan$did_scale[i] <- TRUE
    
    if (isTRUE(drop_original)) dt[[base]] <- NULL
  }
  
  list(dt = dt, did_scale = plan)
}



# =============================================================================
# Unit handling (Option 1 + Option 2)
# =============================================================================

# .klimo_unit_validate <- function(preview_dt, observed_dt, strict = FALSE) {
#   .klimo_require("data.table")
#   dt <- preview_dt[, .(column, declared_units = units)]
#   dt <- dt[observed_dt, on = "column"]
#   dt[, observed_units := as.character(observed_units)]
#   
#   # only validate where declared_units exists
#   dt <- dt[!is.na(declared_units) & nzchar(declared_units)]
#   
#   mism <- dt[!is.na(observed_units) & nzchar(observed_units) & observed_units != declared_units]
#   miss <- dt[is.na(observed_units) | !nzchar(observed_units)]
#   
#   if (nrow(mism)) {
#     msg <- sprintf(
#       "Unit mismatches (%d). Example(s): %s",
#       nrow(mism),
#       paste(head(sprintf("%s: observed=%s declared=%s", mism$column, mism$observed_units, mism$declared_units), 6L),
#             collapse = "; ")
#     )
#     if (isTRUE(strict)) .klimo_stop("dp_write_parquet(): %s", msg) else .klimo_warn("dp_write_parquet(): %s", msg)
#   }
#   
#   if (nrow(miss)) {
#     .klimo_warn(
#       "dp_write_parquet(): %d columns have declared units but no observed attr(unit). (This is ok if you are only documenting.)",
#       nrow(miss)
#     )
#   }
#   
#   invisible(list(mismatch_dt = mism, missing_dt = miss))
# }


.klimo_unit_validate <- function(preview_dt, observed_dt, strict = FALSE) {
  .klimo_require("data.table")
  
  dt <- preview_dt[, .(column, declared_units = units)]
  dt <- dt[observed_dt, on = "column"]
  dt[, observed_units := as.character(observed_units)]
  
  # only validate where declared_units exists
  dt <- dt[!is.na(declared_units) & nzchar(declared_units)]
  if (!nrow(dt)) return(invisible(list(mismatch_dt = dt[0], missing_dt = dt[0])))
  
  mism <- dt[!is.na(observed_units) & nzchar(observed_units) & observed_units != declared_units]
  miss <- dt[is.na(observed_units) | !nzchar(observed_units)]
  
  if (nrow(mism)) {
    msg <- sprintf(
      "Unit mismatches (%d). Example(s): %s",
      nrow(mism),
      paste(head(sprintf("%s: observed=%s declared=%s", mism$column, mism$observed_units, mism$declared_units), 6L),
            collapse = "; ")
    )
    if (isTRUE(strict)) .klimo_stop("dp_write_parquet(): %s", msg) else .klimo_warn("dp_write_parquet(): %s", msg)
  }
  
  if (nrow(miss)) {
    any_tagged <- any(!is.na(dt$observed_units) & nzchar(dt$observed_units))
    msg <- sprintf(
      "dp_write_parquet(): %d columns have declared units but no observed attr(unit). (Ok for documentation-only.)",
      nrow(miss)
    )
    if (any_tagged) .klimo_warn("%s", msg) else .klimo_msg("%s", msg)
  }
  
  invisible(list(mismatch_dt = mism, missing_dt = miss))
}


.klimo_unit_convert <- function(x, preview_dt, src_units = NULL, strict = TRUE) {
  # Uses jj::unit<- (same package) if present. Converts only where declared units exist.
  .klimo_require("data.table")
  dt <- data.table::copy(x)
  
  if (!exists("unit", mode = "function", inherits = TRUE) || !exists("unit<-", mode = "function", inherits = TRUE)) {
    .klimo_stop("unit conversion requested but unit()/unit<- are not available in this session/package namespace.")
  }
  
  if (!is.null(src_units)) {
    if (!is.character(src_units) || is.null(names(src_units))) {
      .klimo_stop("src_units must be a named character vector, e.g., c(ta='degF', td='degF').")
    }
  }
  
  did_convert <- data.table::data.table(column = character(), did_convert = logical())
  
  plan <- preview_dt[!is.na(units) & nzchar(units), .(column, declared_units = units)]
  if (!nrow(plan)) return(list(dt = dt, did_convert = did_convert))
  
  for (i in seq_len(nrow(plan))) {
    col <- plan$column[i]
    dst <- plan$declared_units[i]
    
    if (!(col %in% names(dt))) next
    if (!is.numeric(dt[[col]]) && !is.integer(dt[[col]])) {
      did_convert <- data.table::rbindlist(list(did_convert, data.table::data.table(column = col, did_convert = FALSE)))
      next
    }
    
    old <- attr(dt[[col]], "unit", exact = TRUE)
    has_old <- !is.null(old) && nzchar(old)
    
    # Determine conversion assignment string
    if (has_old) {
      # safest: use current tag as source (unit<- handles idempotence)
      assign <- dst
    } else if (!is.null(src_units) && !is.na(src_units[[col]])) {
      assign <- paste0(src_units[[col]], "|", dst)
    } else {
      # no source known; convert would be ambiguous (especially temps)
      msg <- sprintf("Cannot convert '%s' to '%s' because attr(unit) is missing and src_units[%s] not provided.",
                     col, dst, col)
      if (isTRUE(strict)) .klimo_stop(msg) else {
        .klimo_warn(msg)
        did_convert <- data.table::rbindlist(list(did_convert, data.table::data.table(column = col, did_convert = FALSE)))
        next
      }
    }
    
    # Apply conversion/tag
    # (unit<- returns vector; use dt[, col := {...}] style to avoid reference surprises)
    v <- dt[[col]]
    unit(v) <- assign
    dt[[col]] <- v
    
    did_convert <- data.table::rbindlist(list(did_convert, data.table::data.table(column = col, did_convert = TRUE)))
  }
  
  list(dt = dt, did_convert = did_convert)
}

# =============================================================================
# Writer: dp_write_parquet (hybrid Option 1 + 2 + audit Option 5)
# =============================================================================

#' Write a Parquet file with Klimo spec metadata (units + scaling + audit)
#'
#' `dp_write_parquet()` writes a dataset to a Parquet file (via `arrow`) and
#' stores a robust specification + audit trail in Parquet key_value_metadata.
#'
#' It implements a hybrid unit strategy:
#'
#' **Unit modes**
#' - `unit_mode = "none"`: write without validating or converting; metadata still includes
#'   declared units (if spec present) and observed units (if attributes exist).
#' - `unit_mode = "validate"` (recommended default): does not convert values; it checks
#'   `attr(x[[col]], "unit")` against declared units and warns (or errors if strict).
#' - `unit_mode = "convert"`: converts numeric columns to declared units using `unit<-`
#'   (from this package) and then writes. If the column has no `attr(unit)`, you must
#'   provide `src_units` (named vector), otherwise conversion is refused.
#'
#' **Scaling**
#' - If `scaled = TRUE` and a column has `suggested_scale`, the writer will create a scaled
#'   integer column named `paste0(column, scaled_suffix)` (default: `"_i"`). Optionally
#'   drops the original.
#'
#' **Metadata keys written**
#' - `klimo:spec_version`, `klimo:spec_system`
#' - `klimo:field_spec_json` (JSON; spec table)
#' - `klimo:audit_json` (JSON; declared vs observed units + status + scaling status)
#'
#'#' Key simplification:
#' - There is no separate `system` argument anymore.
#' - The declared system comes from the spec (klimo_spec) or from `declared_system`
#'   when `field_spec` is provided as a plain data.table/data.frame.
#'
#' @param x A data.frame/data.table. Will be converted to data.table internally.
#' @param path Output parquet path.
#' @param field_spec NULL, `"metric"`, `"imperial"`, a `klimo_spec`, or a spec `data.table`.
#'   If NULL: writes without a spec and emits a message showing how to attach defaults.
#' @param system If `field_spec` is custom, which units column to treat as declared units
#'   (`"metric"` or `"imperial"`). Ignored if `field_spec` is `"metric"`/`"imperial"` or a
#'   `klimo_spec` with `system` already set.
#' @param unit_mode `"validate"` (default), `"none"`, or `"convert"`.
#' @param unit_strict If TRUE, validation errors stop instead of warning. For convert mode,
#'   this also controls whether missing sources stop.
#' @param src_units Named character vector of source units for untagged columns (convert mode only),
#'   e.g., `c(ta="degF", td="degF")`.
#' @param scaled Logical. If TRUE, apply scaling as described above.
#' @param scaled_suffix Suffix for scaled columns, default `"_i"`.
#' @param drop_original If TRUE, drop original numeric columns that were scaled.
#' @param compression Parquet compression codec; passed to `arrow::write_parquet`.
#' @param ... Additional arguments passed to `arrow::write_parquet`.
#'
#' @return Invisibly returns a list with:
#'   - `path`
#'   - `spec` (klimo_spec or NULL)
#'   - `preview` (data.table)
#'   - `audit` (data.table)
#' @examples
#' # Minimal example (document-only, no conversion, no scaling):
#' library(data.table)
#' dat <- data.table(ta = c(20, 21), rh = c(50, 55), lat = c(35.9, 36.0), lon = c(-78.9, -78.8))
#' dp_write_parquet(dat, tempfile(fileext = ".parquet"), field_spec = "metric", unit_mode = "none", scaled = FALSE)
#'
#' # Convert-on-write (requires unit() tagging or src_units):
#' # unit(dat$ta) <- "degF"  # if already known

#' @export
dp_write_parquet <- function(
    x,
    path,
    field_spec = NULL,
    declared_system = c("metric", "imperial"),
    unit_mode = c("validate", "none", "convert"),
    unit_strict = FALSE,
    src_units = NULL,
    scaled = FALSE,
    scaled_suffix = "_i",
    drop_original = TRUE,
    compression = "zstd",
    tag_missing_units = FALSE,
    tag_missing_units_warn = TRUE,
    tag_missing_units_max_show = 12L,
    ...
) {
  .klimo_require("data.table")
  .klimo_require("arrow")
  
  declared_system <- match.arg(declared_system)
  unit_mode <- match.arg(unit_mode)
  
  x <- .klimo_dt(x)
  
  # Normalize spec (now always yields a klimo_spec with declared_system)
  spec <- .klimo_normalize_field_spec(x, field_spec, declared_system = declared_system)
  
  if (is.null(spec)) {
    .klimo_warn(
      "dp_write_parquet(): writing without a field_spec.\n  Tip: pass field_spec='metric' or field_spec='imperial', or provide a spec table and declared_system='metric'/'imperial'."
    )
    # Create a minimal "blank" spec so we can still emit consistent metadata
    blank <- data.table::data.table(column = names(x))
    blank[, `:=`(
      concept = NA_character_,
      metric_units = NA_character_,
      imperial_units = NA_character_,
      declared_units = NA_character_,
      writer_scale = as.numeric(NA),
      encoding = "none",
      encoding_scale = as.numeric(NA),
      encoding_base_units = NA_character_
    )]
    spec <- .klimo_spec_new(blank, declared_system = declared_system, version = "1")
  } else {
    .klimo_msg("dp_write_parquet(): using field_spec (declared_system=%s)", spec$declared_system)
  }
  
  # Preview with resolved declared units and scaling/encoding plan
  prev <- dp_spec_preview(
    x,
    field_spec = spec,
    declared_system = spec$declared_system,
    scaled = isTRUE(scaled),
    scaled_suffix = scaled_suffix
  )
  
  matched_n <- prev[!is.na(concept) & nzchar(concept), .N]
  .klimo_msg("  Columns in data: %d", ncol(x))
  .klimo_msg("  Columns matched to known concepts: %d", matched_n)
  
  # Work copy (only if we mutate)
  x2 <- x
  
  # observed units BEFORE any conversion/tagging
  obs0 <- .klimo_collect_unit_observed(x2)
  
  # ---- OPTIONAL: tag missing unit attrs using declared_units from spec ----
  did_tag <- data.table::data.table(column = character(), did_tag = logical())
  
  if (isTRUE(tag_missing_units)) {
    decl <- prev[!is.na(declared_units) & nzchar(declared_units), .(column, declared_units = as.character(declared_units))]
    if (nrow(decl)) {
      chk <- decl[obs0, on = "column"]
      miss <- chk[is.na(observed_units) | !nzchar(observed_units), .(column, declared_units)]
      
      if (nrow(miss)) {
        # avoid mutating caller
        x2 <- data.table::copy(x2)
        for (i in seq_len(nrow(miss))) {
          col <- miss$column[i]
          if (!col %in% names(x2)) next
          attr(x2[[col]], "unit") <- miss$declared_units[i]
        }
        did_tag <- miss[, .(column, did_tag = TRUE)]
        
        if (isTRUE(tag_missing_units_warn)) {
          show <- head(miss$column, tag_missing_units_max_show)
          .klimo_warn(
            "dp_write_parquet(): tag_missing_units=TRUE. Tagged %d column(s) missing attr(unit) using declared units (%s). Example(s): %s",
            nrow(miss),
            spec$declared_system,
            paste(show, collapse = ", ")
          )
        }
        # refresh observed units after tagging
        obs0 <- .klimo_collect_unit_observed(x2)
      }
    }
  }
  
  # Unit behavior
  did_convert <- data.table::data.table(column = character(), did_convert = logical())
  
  if (unit_mode == "validate") {
    .klimo_unit_validate(prev, obs0, strict = isTRUE(unit_strict))
  } else if (unit_mode == "convert") {
    conv <- .klimo_unit_convert(x2, prev, src_units = src_units, strict = isTRUE(unit_strict))
    x2 <- conv$dt
    did_convert <- conv$did_convert
  } else {
    # unit_mode == "none": do nothing
  }
  
  # Writer scaling (creates *_i columns). Skips encoding == scaled_int.
  did_scale <- data.table::data.table(column = character(), writer_scale = numeric(), scaled_column = character(), did_scale = logical())
  if (isTRUE(scaled)) {
    sc <- .klimo_scale_apply(x2, prev, scaled_suffix = scaled_suffix, drop_original = isTRUE(drop_original))
    x2 <- sc$dt
    did_scale <- sc$did_scale
  }
  
  # observed units after conversion/tagging
  obs1 <- .klimo_collect_unit_observed(x2)
  
  audit <- .klimo_build_audit_dt(
    prev, obs1, unit_mode = unit_mode,
    did_convert_map = did_convert,
    did_scale_map = did_scale,
    did_tag_map = did_tag
  )
  
  kv <- .klimo_metadata_kv(spec, prev, audit, extra = list(
    "klimo:writer" = "dp_write_parquet",
    "klimo:tag_missing_units" = as.character(isTRUE(tag_missing_units))
  ))
  
  # Write with schema metadata
  tab <- arrow::Table$create(x2)
  tab <- tab$ReplaceSchemaMetadata(kv)
  
  arrow::write_parquet(
    tab,
    sink = path,
    compression = compression,
    ...
  )
  
  invisible(list(path = path, spec = spec, preview = prev, audit = audit))
}



# # 
# # 
# # 
# # library(jj)
# # 
# # 
# # =============================================================================
# # Example workflow (commented; not run in package examples)
# # =============================================================================
# # # ---- create a fake dataset -------------------------------------------------
# library(data.table)
# set.seed(1)
# dat <- data.table(
#   ta = 20 + rnorm(10),
#   td = 10 + rnorm(10),
#   rh = pmin(pmax(60 + rnorm(10)*5, 0), 100),
#   wind10m = abs(rnorm(10, 3, 1)),
#   DD = runif(10, 0, 360),
#   lat = 35.9 + runif(10, -0.05, 0.05),
#   lon = -78.9 + runif(10, -0.05, 0.05),
#   tcdc = runif(10, 0, 100),
#   solar = pmax(0, rnorm(10, 400, 100))
# )

# 
# 3) Practical usage examples (units workflows)
# A) Documentation-only (no unit attrs required)
# dp_write_parquet(dat, "out.parquet", field_spec="metric", unit_mode="none")
# 
# B) Validate (warn if mismatched; allow missing unit tags)
# dp_write_parquet(dat, "out.parquet", field_spec="imperial", unit_mode="validate")
# 
# C) Validate strictly (stop on mismatches)
# dp_write_parquet(dat, "out.parquet", field_spec="imperial", unit_mode="validate", unit_strict=TRUE)
# 
# D) Convert (requires observed unit tags or src_units)
# # Example: ta/td are degF but you want metric spec (degC)
# dp_write_parquet(
#   dat, "out.parquet",
#   field_spec="metric",
#   unit_mode="convert",
#   src_units=c(ta="degF", td="degF")
# )
# 
# E) “I know my values already match declared units; please tag missing”
# 
# This is your new requested behavior:
#   
#   dp_write_parquet(
#     dat, "out.parquet",
#     field_spec="metric",
#     unit_mode="validate",
#     tag_missing_units=TRUE
#   )
# 
# 
# And in convert mode (careful, but sometimes exactly what you want):
#   
#   # Interprets “missing unit attr” as “already in declared units”
#   dp_write_parquet(
#     dat, "out.parquet",
#     field_spec="metric",
#     unit_mode="convert",
#     tag_missing_units=TRUE
#   )
# 

# 
# # ---- build default spec and preview ----------------------------------------
# spec_m <- dp_spec_default(dat, system = "metric")
# prev <- dp_spec_preview(dat, field_spec = spec_m, scaled = FALSE)
# print(prev)
# 
# # ---- modify one column’s units after-the-fact ------------------------------
# # e.g., your solar column is actually in W/m^2 already (default), but if you needed:
# 
# spec_m2 <- dp_spec_set_units(spec_m, "wind", metric_units = "kt", imperial_units = "kt")
# # class(spec_m2)
# # ---- write with documentation-only (no conversion, no scaling) -------------
# dp_write_parquet(dat, "out_unscaled_doc.parquet", field_spec = spec_m2,
#                    unit_mode = "validate", scaled = FALSE)
# 
# # ---- write with scaling (still no unit conversion) -------------------------
# dp_write_parquet(dat, "out_scaled_doc.parquet", field_spec = spec_m2,
#                    unit_mode = "validate", scaled = TRUE, drop_original = TRUE)
# 
# # ---- convert on write (requires tags or src_units) -------------------------
# # If your in-memory data is actually degF, but you want stored/declared degC:
# # unit(dat$ta) <- "degF"
# # unit(dat$td) <- "degF"
# # dp_write_parquet(dat, "out_converted.parquet", field_spec = "metric",
# #                    unit_mode = "convert", scaled = FALSE)
# 
# # If columns are untagged but you know source units:
# # dp_write_parquet(dat, "out_converted2.parquet", field_spec = "metric",
# #                    unit_mode = "convert",
# #                    src_units = c(ta = "degF", td = "degF"),
# #                    scaled = FALSE)
# 
# 
# 
# library(data.table)
# 
# set.seed(42)
# dat <- data.table(
#   ta      = 20 + rnorm(10),
#   td      = 10 + rnorm(10),
#   rh      = pmin(pmax(60 + rnorm(10)*5, 0), 100),
#   wind10m = abs(rnorm(10, 3, 1)),
#   DD      = runif(10, 0, 360),
#   lat     = 35.9 + runif(10, -0.05, 0.05),
#   lon     = -78.9 + runif(10, -0.05, 0.05),
#   tcdc    = runif(10, 0, 100),
#   solar   = pmax(0, rnorm(10, 400, 100))
# )
# 
# spec_m <- dp_spec_default(dat, system = "metric")
# spec_i <- dp_spec_default(dat, system = "imperial")
# 
# 
# prev_unscaled <- dp_spec_preview(dat, field_spec = spec_m, scaled = FALSE)
# prev_unscaled
# 
# 
# spec_m2$spec_dt
# 
# 
# 
# 
# spec_m2 <- dp_spec_set_units(spec_m, "solar", metric_units = "m/s", imperial_units = "kW/m^2")
# dp_spec_preview(dat, field_spec = spec_m2, scaled = TRUE)
# 
# 
# 
# spec_m3 <- dp_spec_set_scale(spec_m2, c("lat", "lon"), suggested_scale = 5)
# dp_spec_preview(dat, field_spec = spec_m3, scaled = TRUE)
# 
# 
# 
# unit(dat$ta) <- "degC"
# unit(dat$td) <- "degC"
# unit(dat$wind10m) <- "m/s"
# unit(dat$solar) <- "W/m^2"
# 
# .klimo_collect_unit_observed(dat)
# 
# 
# dp_write_parquet(dat, 'test.parquet', unit_mode="validate", field_spec="metric")
# 
# ?dp_write_parquet
# 
# 
# 
# #
# #
# #
# # rm(dp_spec_default)
# # rm(dp_spec_set_units)
# # rm(dp_spec_set_scale)
# # rm(dp_spec_preview)
# # rm(dp_write_parquet)
# # # =============================================================================
# #
# #
# # dat
# 
# library(jj)
# ?dp_write_parquet
# 
# 
# 
# 
# 






