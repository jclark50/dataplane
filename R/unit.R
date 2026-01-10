#' Lightweight unit getter/setter (conversion + optional rounding)
#'
#' Provides a simple way to read and set a `"unit"` attribute on numeric
#' vectors. The replacement form (`unit(x) <- value`) can also convert values
#' between supported units (handled by the package-internal
#' \code{.convert_units()}) and optionally round the result.
#'
#' \strong{Accepted assignment forms for \code{value}:}
#' \itemize{
#'   \item \code{"degF"} — set/convert to \code{degF} using the current attribute
#'         as the source (if present).
#'   \item \code{"degC -> degF"} — explicitly convert from \code{degC} to \code{degF}.
#'   \item \code{"degC|degF"} — shorthand for the arrow form above.
#'   \item Optional rounding may be requested (highest precedence first):
#'         \code{"...; round=1"}, \code{"...; digits=1"}, trailing \code{"|1"},
#'         or trailing \code{"(1)"}; e.g., \code{"degC|degF|1"} or \code{"degF (2)"}.
#' }
#'
#' If an explicit source unit is provided on the left side (e.g.,
#' \code{"degC -> degF"}), it is treated as authoritative. To prevent accidental
#' re-interpretation of already-tagged data, a source-mismatch policy is applied:
#'
#' \itemize{
#'   \item \code{options(jj.unit.on_src_mismatch = "warn_noop")} (default) —
#'         warn and do nothing when \emph{declared source} differs from
#'         \code{attr(x,"unit")}.
#'   \item \code{"error"} — stop with an error.
#'   \item \code{"convert"} — trust the declared source and convert anyway
#'         (original permissive behavior).
#' }
#'
#' Rounding is applied \emph{after} conversion. Global default rounding can be set via
#' \code{options(jj.unit.round_digits = K)}. This default is only applied when a
#' conversion actually occurred; inline digits (\code{round=} / \code{digits=} /
#' \code{|K} / \code{(K)}) always apply. When rounding occurs, the setter also records
#' \code{attr(x, "unit_digits") = K}.
#'
#' To avoid silently mislabeling temperatures, you may forbid “tag-only” temperature
#' assignments (no known source) by enabling:
#' \code{options(jj.unit.disallow_temp_tag_only = TRUE)}. With this option set,
#' attempting \code{unit(x) <- "degF"} on an untagged vector will error; use
#' \code{"src|dst"} instead.
#'
#' @section Conversions:
#' The actual numeric conversions are performed by the internal
#' \code{.convert_units(x, from, to)}. Typical pairs supported in this package
#' include temperature (\code{degC}, \code{degF}, \code{K}/\code{degK}), pressure
#' (\code{hPa}, \code{Pa}), wind speed (\code{m/s}, \code{mph}, \code{kt}),
#' precipitation depth (\code{kg/m^2}, \code{mm}, \code{in}), radiation
#' (\code{W/m^2}), and relative humidity forms (where supported by your map).
#'
#' @param x A numeric vector (or column) to query or tag with a \code{"unit"} attribute.
#' @param value Character scalar describing the assignment. See the accepted forms above.
#'   Examples: \code{"degF"}, \code{"degC -> degF"}, \code{"degC|degF|1"},
#'   \code{"degF (2)"}, \code{"degF; round=1"}.
#'
#' @return
#' \itemize{
#'   \item \code{unit(x)} returns the current unit attribute (character scalar) or \code{NULL}.
#'   \item \code{unit(x) <- value} returns the modified vector \code{x}, with values converted
#'         if needed, optionally rounded, and \code{attr(x, "unit")} set to the target unit.
#'         When rounding is applied, \code{attr(x, "unit_digits")} is also set.
#' }
#'
#' @examples
#' x <- c(25, 26, 27)
#' unit(x) <- "degC"        # tag as degC
#' unit(x)                  # "degC"
#'
#' # Convert using current attribute as the source
#' unit(x) <- "degF"        # C -> F (if attr is "degC")
#'
#' # Explicit source -> target
#' unit(x) <- "degF -> degC"
#'
#' # Shorthand with rounding
#' unit(x) <- "degC|degF|1"   # C -> F, then round(., 1)
#' unit(x) <- "degF (2)"      # tag/convert to F, then round(., 2)
#'
#' # Safer everyday pattern (idempotent):
#' y <- c(0, 5, 10); unit(y) <- "degC"
#' unit(y) <- "degF"          # converts once; calling again is a no-op
#'
#' # data.table in-place usage
#' if (requireNamespace("data.table", quietly = TRUE)) {
#'   library(data.table)
#'   DT <- data.table(ta = c(25, 26, 27))
#'   unit(DT$ta) <- "degC"
#'   DT[, ta := { unit(ta) <- "degC -> degF; round=1"; ta }]
#'   unit(DT$ta)                    # "degF"
#'   attr(DT$ta, "unit_digits")     # 1
#' }
#'
#' # Policies (optional):
#' # options(jj.unit.on_src_mismatch = "warn_noop")  # default
#' # options(jj.unit.on_src_mismatch = "error")
#' # options(jj.unit.on_src_mismatch = "convert")
#' #
#' # options(jj.unit.round_digits = 1L)              # global rounding (after conversions)
#' # options(jj.unit.disallow_temp_tag_only = TRUE)  # forbid tag-only for temperatures
#'
#' @name unit
#' @rdname unit
#' @export
unit <- function(x) {
  attr(x, "unit")
}

# ---- helpers (internal) ------------------------------------------------------

.trim <- function(s) sub("^\\s+|\\s+$", "", s)

# parse key=val parameters after semicolon (e.g., "; round=1 digits=2")
.parse_kv <- function(s) {
  if (!nzchar(s)) return(list())
  toks <- unlist(strsplit(s, "\\s*[;,]\\s*|\\s+", perl = TRUE), use.names = FALSE)
  kv <- list()
  for (tok in toks) {
    if (!nzchar(tok) || !grepl("=", tok, fixed = TRUE)) next
    parts <- strsplit(tok, "=", fixed = TRUE)[[1]]
    key <- tolower(.trim(parts[1])); val <- .trim(paste(parts[-1], collapse = "="))
    kv[[key]] <- val
  }
  kv
}

# Is this token a temperature unit we want to treat specially when tag-only?
.is_temp_unit <- function(u) {
  u %in% c("degC", "degF", "K", "degK")
}

# parser accepting:
#   - "src -> dst" or "src|dst"
#   - trailing "|K" where K is integer digits
#   - trailing "(K)" where K is integer digits
#   - optional "; round=K" or "; digits=K"
.parse_unit_assignment <- function(value) {
  stopifnot(is.character(value), length(value) == 1L, !is.na(value))
  val <- .trim(value)

  # split "LHS ; params"
  pieces <- strsplit(val, "\\s*;\\s*", perl = TRUE)[[1]]
  lhs <- pieces[1]
  params <- if (length(pieces) > 1L) .parse_kv(paste(pieces[-1], collapse = " ")) else list()

  # inline digits via params
  digits <- NULL
  if (length(params)) {
    if (!is.null(params$round))  digits <- suppressWarnings(as.integer(params$round))
    if (!is.null(params$digits)) digits <- suppressWarnings(as.integer(params$digits))
  }

  # inline digits via trailing "|K"
  if (is.null(digits)) {
    triple <- strsplit(lhs, "\\|", perl = TRUE)[[1]]
    triple <- .trim(triple)
    if (length(triple) == 3L && grepl("^[0-9]+$", triple[2L + 1L])) {
      lhs <- paste(triple[1:2], collapse = "|")
      digits <- as.integer(triple[3])
    }
  }

  # inline digits via trailing "(K)"
  if (is.null(digits)) {
    m <- regexec("\\((\\d+)\\)\\s*$", lhs, perl = TRUE)
    r <- regmatches(lhs, m)[[1]]
    if (length(r)) {
      digits <- as.integer(sub(".*\\((\\d+)\\)\\s*$", "\\1", lhs, perl = TRUE))
      lhs <- sub("\\((\\d+)\\)\\s*$", "", lhs, perl = TRUE)
      lhs <- .trim(lhs)
    }
  }

  # src/dst split
  if (grepl("(->)|\\|", lhs)) {
    lhs2 <- gsub("\\|", "->", lhs)
    ab <- strsplit(lhs2, "\\s*->\\s*", perl = TRUE)[[1]]
    if (length(ab) != 2L) stop("unit<-: expected 'src -> dst' (or 'src|dst').")
    src <- .trim(ab[1]); dst <- .trim(ab[2])
  } else {
    src <- NA_character_; dst <- lhs
  }

  # sanitize digits
  if (!is.null(digits) && (!is.finite(digits) || digits < 0)) digits <- NULL

  list(src = src, dst = dst, digits = digits)
}

# ---- setter ------------------------------------------------------------------

#' @rdname unit
#' @usage unit(x) <- value
#' @export
`unit<-` <- function(x, value) {
  parsed <- .parse_unit_assignment(value)
  old    <- attr(x, "unit")

  # Did the user supply digits inline? (If not, global default only applies after a conversion.)
  inline_digits <- !is.null(parsed$digits)

  # Global default rounding (applies only when a conversion occurred, unless inline specified)
  digits <- if (inline_digits) parsed$digits else getOption("jj.unit.round_digits", NULL)
  if (!is.null(digits) && (!is.finite(digits) || digits < 0)) digits <- NULL

  did_convert <- FALSE

  # ---- explicit source provided: "src -> dst" / "src|dst" ----
  if (!is.na(parsed$src)) {
    # Source-mismatch policy if we already have a tag
    if (!is.null(old) && !identical(old, parsed$src)) {
      policy <- getOption("jj.unit.on_src_mismatch", "warn_noop")
      if (identical(policy, "error")) {
        stop(sprintf("unit<-: declared source '%s' does not match current attr '%s'.",
                     parsed$src, old))
      } else if (identical(policy, "warn_noop")) {
        warning(sprintf("unit<-: requested '%s -> %s' but current attr is '%s'; no-op.",
                        parsed$src, parsed$dst, old))
        return(x)  # do nothing
      }
      # "convert": fall through and trust the declared source
    }

    # If src == dst, only (optional) rounding + retag
    if (identical(parsed$src, parsed$dst)) {
      if (!is.null(digits)) x <- base::round(x, digits)
      attr(x, "unit") <- parsed$dst
      if (!is.null(digits)) attr(x, "unit_digits") <- digits
      return(x)
    }

    # Normal explicit conversion
    x <- .convert_units(x, parsed$src, parsed$dst)
    did_convert <- TRUE

    if (!is.null(digits) && (did_convert || inline_digits)) x <- base::round(x, digits)
    attr(x, "unit") <- parsed$dst
    if (!is.null(digits) && (did_convert || inline_digits)) attr(x, "unit_digits") <- digits
    return(x)
  }

  # ---- only a target provided: "dst" (use current attr as source if present) ----
  if (!is.null(old) && !identical(old, parsed$dst)) {
    x <- .convert_units(x, old, parsed$dst)
    did_convert <- TRUE
  } else {
    # Optional: forbid temperature tag-only (no known source)
    if (isTRUE(getOption("jj.unit.disallow_temp_tag_only", FALSE))) {
      if (is.null(old) && .is_temp_unit(parsed$dst)) {
        stop(sprintf("unit<-: refusing to tag temperature as '%s' without a known source. Use 'src|%s'.",
                     parsed$dst, parsed$dst))
      }
    }
  }

  if (!is.null(digits) && (did_convert || inline_digits)) x <- base::round(x, digits)
  attr(x, "unit") <- parsed$dst
  if (!is.null(digits) && (did_convert || inline_digits)) attr(x, "unit_digits") <- digits
  x
}
