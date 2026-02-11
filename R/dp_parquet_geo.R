# R/dp_parquet_geo.R
# =============================================================================
# GeoParquet helpers (gp_*; independent of dataplane metadata)
# =============================================================================

.gp_require <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    stop(sprintf("Package '%s' is required.", pkg), call. = FALSE)
  }
}

# -----------------------------------------------------------------------------
#' Build a GeoParquet `geo` metadata object
#'
#' Constructs a GeoParquet-compliant metadata list for one or more geometry columns.
#' This metadata can be serialized to JSON and stored in Parquet schema metadata
#' under the key `"geo"` (the GeoParquet convention).
#'
#' @param geometry_columns Character vector of geometry column names.
#' @param primary_column Primary geometry column name (defaults to first geometry column).
#' @param encoding Geometry encoding (default `"WKB"`).
#' @param geometry_types Optional geometry type(s). May be:
#'   - a character vector applied to all geometry columns, or
#'   - a named list keyed by geometry column.
#' @param crs CRS as PROJJSON (list) or a string convertible via `sf::st_crs()`.
#' @param edges `"planar"` or `"spherical"`.
#' @param orientation Optional orientation string (currently supports `"counterclockwise"`).
#' @param bbox Optional numeric bounding box.
#' @param covering_bbox Optional covering bbox spec (see GeoParquet).
#' @param version GeoParquet metadata version (default `"1.1.0"`).
#' @param extra Optional additional fields to include in the metadata list.
#'
#' @return A nested list suitable for JSON serialization via [gp_geoparquet_geo_json()].
#' @export
gp_geoparquet_build_geo_meta <- function(
    geometry_columns,
    primary_column = geometry_columns[1],
    encoding = "WKB",
    # geometry_types = NULL,
    crs = NULL,
    edges = c("planar", "spherical"),
    orientation = c(NULL, "counterclockwise"),
    bbox = NULL,
    covering_bbox = NULL,
    version = "1.1.0",
    extra = list()
) {
  .gp_require("jsonlite")
  
  geometry_columns <- as.character(geometry_columns)
  geometry_columns <- geometry_columns[!is.na(geometry_columns) & nzchar(geometry_columns)]
  if (!length(geometry_columns)) stop("geometry_columns must be a non-empty character vector.", call. = FALSE)
  
  primary_column <- as.character(primary_column)[1]
  if (is.na(primary_column) || !nzchar(primary_column)) stop("primary_column must be a non-empty string.", call. = FALSE)
  if (!primary_column %in% geometry_columns) stop("primary_column must be one of geometry_columns.", call. = FALSE)
  
  edges <- match.arg(edges)
  
  orientation <- orientation[1]
  if (!is.null(orientation)) {
    orientation <- match.arg(orientation, choices = c("counterclockwise"))
  }
  
  .normalize_geom_types <- function(gt, col) {
    if (is.null(gt)) return(character(0))
    if (is.character(gt)) return(as.character(gt))
    if (is.list(gt)) {
      if (!is.null(names(gt)) && nzchar(names(gt)[1]) && col %in% names(gt)) {
        return(as.character(gt[[col]]))
      }
      stop("geometry_types as list must be a named list keyed by geometry column name.", call. = FALSE)
    }
    stop("geometry_types must be NULL, a character vector, or a named list.", call. = FALSE)
  }
  
  .crs_to_projjson <- function(x) {
    if (is.null(x)) return(NULL)
    if (is.list(x)) return(x)
    
    x <- as.character(x)[1]
    if (is.na(x) || !nzchar(x)) return(NULL)
    
    if (grepl("^\\s*\\{", x)) {
      out <- jsonlite::fromJSON(x, simplifyVector = FALSE)
      if (!is.list(out)) return(NULL)
      return(out)
    }
    
    if (requireNamespace("sf", quietly = TRUE)) {
      crs_obj <- tryCatch(sf::st_crs(x), error = function(e) NULL)
      if (!is.null(crs_obj)) {
        pj <- NULL
        if (!is.null(crs_obj$projjson) && nzchar(crs_obj$projjson)) {
          pj <- tryCatch(jsonlite::fromJSON(crs_obj$projjson, simplifyVector = FALSE), error = function(e) NULL)
        }
        return(pj)
      }
    }
    
    NULL
  }
  
  crs_projjson <- .crs_to_projjson(crs)
  
  .normalize_covering_bbox <- function(cb) {
    if (is.null(cb)) return(NULL)
    if (!is.list(cb)) stop("covering_bbox must be a list or NULL.", call. = FALSE)
    
    if (!is.null(cb$group)) {
      grp <- as.character(cb$group)[1]
      if (is.na(grp) || !nzchar(grp)) stop("covering_bbox$group must be a non-empty string.", call. = FALSE)
      return(list(
        bbox = list(
          xmin = c(grp, "xmin"),
          ymin = c(grp, "ymin"),
          xmax = c(grp, "xmax"),
          ymax = c(grp, "ymax")
        )
      ))
    }
    
    if (!is.null(cb$bbox)) return(cb)
    stop("covering_bbox must be either list(group='bbox_group_name') or list(bbox=...).", call. = FALSE)
  }
  
  covering_norm <- .normalize_covering_bbox(covering_bbox)
  
  cols_meta <- list()
  for (col in geometry_columns) {
    col_meta <- list(
      encoding = as.character(encoding)[1],
      geometry_types = .normalize_geom_types(geometry_types, col)
    )
    
    if (!is.null(crs_projjson)) col_meta$crs <- crs_projjson
    if (!is.null(orientation)) col_meta$orientation <- orientation
    
    col_meta$edges <- edges
    
    if (!is.null(bbox)) col_meta$bbox <- as.numeric(bbox)
    if (!is.null(covering_norm)) col_meta$covering <- covering_norm
    
    cols_meta[[col]] <- col_meta
  }
  
  meta <- c(list(
    version = as.character(version)[1],
    primary_column = primary_column,
    columns = cols_meta
  ), extra)
  
  meta
}

# -----------------------------------------------------------------------------
#' Serialize GeoParquet metadata to JSON
#'
#' @param geo_meta A metadata list created by [gp_geoparquet_build_geo_meta()].
#'
#' @return A JSON string.
#' @export
gp_geoparquet_geo_json <- function(geo_meta) {
  .gp_require("jsonlite")
  jsonlite::toJSON(
    geo_meta,
    auto_unbox = TRUE,
    null = "null",
    digits = 16
  )
}

# -----------------------------------------------------------------------------
#' Attach GeoParquet metadata to an Arrow Table
#'
#' Adds a `geo` JSON metadata entry to an Arrow Table's schema metadata.
#'
#' @param tbl An Arrow Table.
#' @param geo_meta GeoParquet metadata list.
#' @param geo_key Metadata key to store geo JSON (default `"geo"`).
#'
#' @return An Arrow Table with updated schema metadata.
#' @export
gp_geoparquet_attach_geo_to_table <- function(tbl, geo_meta, geo_key = "geo") {
  .gp_require("arrow")
  geo_key <- as.character(geo_key)[1]
  if (is.na(geo_key) || !nzchar(geo_key)) stop("geo_key must be a non-empty string.", call. = FALSE)
  
  geo_json <- gp_geoparquet_geo_json(geo_meta)
  
  md <- tryCatch(tbl$schema$metadata, error = function(e) NULL)
  
  md2 <- list()
  if (!is.null(md) && length(md)) {
    nms <- names(md)
    for (i in seq_along(md)) {
      k <- nms[i] %||% ""
      if (!nzchar(k)) next
      v <- md[[i]]
      if (is.raw(v)) v <- rawToChar(v)
      md2[[k]] <- as.character(v)[1]
    }
  }
  
  md2[[geo_key]] <- geo_json
  
  if (!is.null(tbl$ReplaceSchemaMetadata) && is.function(tbl$ReplaceSchemaMetadata)) {
    return(tbl$ReplaceSchemaMetadata(md2))
  }
  
  sch <- tbl$schema
  if (!is.null(sch$WithMetadata) && is.function(sch$WithMetadata)) {
    sch2 <- sch$WithMetadata(md2)
    if (!is.null(tbl$cast) && is.function(tbl$cast)) {
      return(tbl$cast(sch2))
    }
  }
  
  stop(
    "Unable to attach schema metadata with this Arrow version. ",
    "Need Table$ReplaceSchemaMetadata() or Schema$WithMetadata() + Table$cast().",
    call. = FALSE
  )
}

# -----------------------------------------------------------------------------
#' Write GeoParquet with GeoParquet metadata
#'
#' Writes a Parquet file with GeoParquet `geo` metadata embedded in schema metadata.
#'
#' @param x Data frame-like object to write (often an `sf` object after dropping geometry list-columns).
#' @param path Output Parquet file path.
#' @param geo_meta GeoParquet metadata list.
#' @param compression Parquet compression (default `"zstd"`).
#' @param ... Passed to [arrow::write_parquet()].
#'
#' @return The output path (invisibly).
#' @export
gp_geoparquet_write_parquet <- function(
    x,
    path,
    geo_meta,
    compression = "zstd",
    ...
) {
  .gp_require("arrow")
  
  tbl <- arrow::Table$create(x)
  tbl2 <- gp_geoparquet_attach_geo_to_table(tbl, geo_meta)
  
  arrow::write_parquet(
    tbl2,
    sink = path,
    compression = compression,
    ...
  )
  
  invisible(path)
}

# -----------------------------------------------------------------------------
#' Read GeoParquet `geo` metadata from a Parquet file
#'
#' @param path Parquet file path.
#' @param geo_key Metadata key holding geo JSON (default `"geo"`).
#'
#' @return Parsed GeoParquet metadata list, or `NULL` if not present.
#' @export
gp_geoparquet_read_geo <- function(path, geo_key = "geo") {
  .gp_require("arrow")
  .gp_require("jsonlite")
  
  geo_key <- as.character(geo_key)[1]
  if (is.na(geo_key) || !nzchar(geo_key)) stop("geo_key must be a non-empty string.", call. = FALSE)
  
  rdr <- arrow::ParquetFileReader$create(path)
  md  <- rdr$GetFileMetaData()
  
  kv <- md$key_value_metadata
  if (is.null(kv) || !nrow(kv)) return(NULL)
  
  idx <- which(kv$key == geo_key)
  if (!length(idx)) return(NULL)
  
  geo_json <- kv$value[idx[1]]
  if (is.na(geo_json) || !nzchar(geo_json)) return(NULL)
  
  jsonlite::fromJSON(geo_json, simplifyVector = FALSE)
}

# -----------------------------------------------------------------------------
#' Test whether a Parquet file is GeoParquet
#'
#' @param path Parquet file path.
#' @param geo_key Metadata key holding geo JSON (default `"geo"`).
#'
#' @return Logical.
#' @export
gp_geoparquet_is_geoparquet <- function(path, geo_key = "geo") {
  !is.null(gp_geoparquet_read_geo(path, geo_key = geo_key))
}

# -----------------------------------------------------------------------------
#' Infer GeoParquet metadata from an sf object
#'
#' Convenience helper that infers geometry types and CRS PROJJSON from an `sf`
#' object and returns a GeoParquet metadata list.
#'
#' @param sf_obj An `sf` object.
#' @param geometry_column Geometry column name (defaults to `attr(sf_obj, "sf_column")`).
#' @param encoding Geometry encoding (default `"WKB"`).
#' @param version GeoParquet metadata version (default `"1.1.0"`).
#' @param edges `"planar"` or `"spherical"` (default `"planar"`).
#' @param orientation Optional orientation string.
#' @param covering_bbox Optional covering bbox spec.
#' @param extra Optional additional fields to include.
#'
#' @return A GeoParquet metadata list suitable for [gp_geoparquet_geo_json()].
#' @export
#'
#' @examples
#' \dontrun{
#' library(sf)
#' x <- st_as_sf(data.frame(id=1:2, x=c(0,1), y=c(0,1)), coords=c("x","y"), crs=4326)
#' meta <- gp_geoparquet_infer_from_sf(x)
#' }
gp_geoparquet_infer_from_sf <- function(
    sf_obj,
    geometry_column = NULL,
    encoding = "WKB",
    version = "1.1.0",
    edges = "planar",
    orientation = NULL,
    covering_bbox = NULL,
    extra = list()
) {
  .gp_require("jsonlite")
  if (!requireNamespace("sf", quietly = TRUE)) {
    stop("Package 'sf' is required for gp_geoparquet_infer_from_sf().", call. = FALSE)
  }
  
  if (is.null(geometry_column)) {
    geometry_column <- attr(sf_obj, "sf_column") %||% "geometry"
  }
  geometry_column <- as.character(geometry_column)[1]
  
  gt <- unique(as.character(sf::st_geometry_type(sf_obj, by_geometry = TRUE)))
  .title_geom <- function(x) {
    x <- toupper(x)
    map <- c(
      "POINT" = "Point",
      "LINESTRING" = "LineString",
      "POLYGON" = "Polygon",
      "MULTIPOINT" = "MultiPoint",
      "MULTILINESTRING" = "MultiLineString",
      "MULTIPOLYGON" = "MultiPolygon",
      "GEOMETRYCOLLECTION" = "GeometryCollection"
    )
    out <- unname(map[x])
    out[is.na(out)] <- x
    out
  }
  geom_types <- .title_geom(gt)
  
  crs_obj <- sf::st_crs(sf_obj)
  crs_pj <- NULL
  if (!is.null(crs_obj$projjson) && nzchar(crs_obj$projjson)) {
    crs_pj <- tryCatch(jsonlite::fromJSON(crs_obj$projjson, simplifyVector = FALSE), error = function(e) NULL)
  }
  
  gp_geoparquet_build_geo_meta(
    geometry_columns = geometry_column,
    primary_column   = geometry_column,
    encoding         = encoding,
    geometry_types   = geom_types,
    crs              = crs_pj,
    edges            = edges,
    orientation      = orientation,
    covering_bbox    = covering_bbox,
    version          = version,
    extra            = extra
  )
}
