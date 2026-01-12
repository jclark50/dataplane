#' @keywords internal
.onLoad <- function(libname, pkgname) {
  # nothing required; keeps a home for package-level setup if needed
}

# Avoid R CMD check NOTES for NSE (data.table and general)
utils::globalVariables(c(
  # data.table special symbols
  ".", ":=", ".SD", ".N",
  
  # your common column names referenced unquoted in DT code
  "column", "column_norm", "concept",
  "declared_units", "metric_units", "imperial_units",
  "writer_scale", "encoding", "encoding_scale", "encoding_base_units",
  "observed_units", "did_tag", "did_convert", "did_scale",
  "scaled_column", "suggested_scale", "scale_status",
  "synonyms", "synonym", "synonym_norm",
  "key", "LastModified",
  
  # data.table join helper name you used
  ".hit",
  
  # any other NSE vars flagged
  "..metric_units", "..imperial_units"
))


#' @useDynLib weathertools, .registration = TRUE