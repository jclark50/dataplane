#' Internal: NULL/NA coalesce operator
#' @keywords internal
#' @noRd
`%||%` <- function(a,b) if (is.null(a) || length(a)==0L || (length(a)==1L && is.na(a))) b else a


# #' Internal: conv a temperature unit token
# #' @keywords internal
# #' @noRd
# # .convert_units <- function(x, from, to) {
  # # if (is.null(from) || from == to) return(x)
  # # key <- base::paste(from, "->", to)
  # # switch(key,
         # # "C -> F"       = x * 9/5 + 32,
         # # "F -> C"       = (x - 32) * 5/9,
         # # "degK -> degC"          = x - 273.15,
         # # "K -> degC"          = x - 273.15,
         # # "degF -> degC"       = (x - 32) * 5/9,
         # # "degC -> degF"       = x * 9/5 + 32,
         # # "Pa -> hPa"          = x / 100,
         # # "hPa -> Pa"          = x * 100,
         # # "kt -> m/s"          = x * 0.514444,
         # # "mph -> m/s"         = x / 2.2369362921,
         # # "m/s -> mph"         = x * 2.2369362921,
         # # "m/s -> mi/h"         = x * 2.2369362921,
         # # "feet -> m"          = x * 0.3048,
         # # "mile -> km"         = x * 1.609344,
         # # "kg/m^2/s -> mm/h"   = x * 3600,
         # # "kg/m^2 -> mm"       = x,
         # # "kg/m^2 -> in"       = x / 25.4,   # <-- new
         # # "mm -> in"           = x / 25.4,   # optional direct
         # # # identities
         # # "m/s -> m/s"         = x,  "hPa -> hPa" = x, "% -> %" = x,
         # # "W/m^2 -> W/m^2"     = x,  "deg -> deg" = x, "K -> K" = x,
         # # "dB -> dB"           = x,  "1/m^2/s -> 1/m^2/s" = x,
         # # stop(sprintf("No converter for %s", key)))
# # }


#' @keywords internal
#' @noRd
# Normalize a unit token to canonical form used by the switch table
.norm_u <- function(u) {
  if (is.null(u) || !nzchar(u <- trimws(as.character(u)))) return(NA_character_)
  u <- gsub("\\\\","",u)
  u <- gsub("°","",u, fixed=TRUE)
  u_up <- toupper(u)
  
  # temperatures
  if (u_up %in% c("C","DEGC","CELSIUS")) return("degC")
  if (u_up %in% c("F","DEGF","FAHRENHEIT")) return("degF")
  if (u_up %in% c("K","DEGK","KELVIN")) return("K")
  
  # wind speed
  if (u_up %in% c("MPH","MI/H","MIPH")) return("mph")
  if (u_up %in% c("M/S","MPS","MS-1","MS^-1")) return("m/s")
  if (u_up %in% c("KT","KTS","KNOT","KNOTS")) return("kt")
  
  # pressure
  if (u_up %in% c("HPA")) return("hPa")
  if (u_up %in% c("PA"))  return("Pa")
  
  # length / distance
  if (u_up %in% c("M"))   return("m")
  if (u_up %in% c("FT","FEET")) return("feet")
  if (u_up %in% c("KM"))  return("km")
  if (u_up %in% c("MI","MILE","MILES")) return("mile")
  
  # precip / radiation / misc
  if (u_up %in% c("MM")) return("mm")
  if (u_up %in% c("IN","INCH","INCHES")) return("in")
  if (u_up %in% c("MM/H","MMHR","MM/HR")) return("mm/h")
  if (u_up %in% c("W/M^2","WM^-2","W M^-2","W/M2","WM2")) return("W/m^2")
  if (u_up %in% c("%","PCT","PERCENT")) return("%")
  if (u_up %in% c("DEG","DEGREE","DEGREES")) return("deg")
  if (u_up %in% c("1/M^2/S")) return("1/m^2/s")
  
  u  # as-is
}

#' @keywords internal
#' @noRd
.convert_units <- function(x, from, to) {
  if (is.null(from) || is.null(to)) return(x)
  from <- .norm_u(from); to <- .norm_u(to)
  if (is.na(from) || is.na(to) || from == to) return(x)
  
  key <- base::paste(from, "->", to)
  
  switch(key,
         # --- Temperatures (complete triangle C/F/K) ---
         "degC -> degF" = x * 9/5 + 32,
         "degF -> degC" = (x - 32) * 5/9,
         "degC -> K"    = x + 273.15,
         "K -> degC"    = x - 273.15,
         "degF -> K"    = (x - 32) * 5/9 + 273.15,   # NEW
         "K -> degF"    = (x - 273.15) * 9/5 + 32,   # NEW
         
         # --- Pressure ---
         "Pa -> hPa"    = x / 100,
         "hPa -> Pa"    = x * 100,
         
         # --- Wind speed ---
         "kt -> m/s"    = x * 0.514444,
         "m/s -> kt"    = x / 0.514444,              # NEW
         "mph -> m/s"   = x / 2.2369362921,
         "m/s -> mph"   = x * 2.2369362921,
         
         # --- Distance / height ---
         "feet -> m"    = x * 0.3048,
         "m -> feet"    = x / 0.3048,                # NEW
         "mile -> km"   = x * 1.609344,
         "km -> mile"   = x / 1.609344,              # NEW
         
         # --- Precip / fluxes (water equiv) ---
         "kg/m^2/s -> mm/h" = x * 3600,
         "mm/h -> kg/m^2/s" = x / 3600,              # NEW
         "kg/m^2 -> mm"     = x,                     # 1 kg/m^2 = 1 mm (water)
         "mm -> kg/m^2"     = x,                     # NEW (reverse)
         "mm -> in"         = x / 25.4,
         "in -> mm"         = x * 25.4,              # NEW
         "kg/m^2 -> in"     = x / 25.4,              # (via mm equivalence)
         "in -> kg/m^2"     = x * 25.4,              # NEW
         
         # --- Identities (common tokens) ---
         "m/s -> m/s"   = x, "mph -> mph" = x, "kt -> kt" = x,
         "hPa -> hPa"   = x, "Pa -> Pa"   = x,
         "% -> %"       = x, "deg -> deg" = x,
         "W/m^2 -> W/m^2" = x,
         "K -> K"       = x, "degC -> degC" = x, "degF -> degF" = x,
         "mm -> mm"     = x, "mm/h -> mm/h" = x, "km -> km" = x, "m -> m" = x,
         "feet -> feet" = x, "mile -> mile" = x, "in -> in" = x,
         "1/m^2/s -> 1/m^2/s" = x,
         
         # Fallback:
         stop(sprintf("No converter for %s", key))
  )
}
  

#' Internal: normalize a temperature unit token
#' @keywords internal
#' @noRd
.norm_temp_unit <- function(u) {
  if (is.null(u)) return(NA_character_)
  u <- trimws(as.character(u))
  if (!nzchar(u)) return(NA_character_)
  u <- gsub("\\\\", "", u)
  u <- gsub("°", "", u, fixed = TRUE)
  u <- toupper(u)
  if (u %in% c("C","DEGC","CELSIUS")) return("degC")
  if (u %in% c("F","DEGF","FAHRENHEIT")) return("degF")
  if (u %in% c("K","KELVIN")) return("K")
  u
}

#' Internal: map unit to C++ token ("C" or "F")
#' @keywords internal
#' @noRd
.cpp_temp_token <- function(u) {
  u <- .norm_temp_unit(u)
  if (u == "degC") return("C")
  if (u == "degF") return("F")
  stop(sprintf("C++ temperature token requires C or F, got: %s", u))
}

#' Internal: temperature conversion among degC/degF/K
#' @keywords internal
#' @noRd
.temp_convert <- function(x, from, to) {
  from <- .norm_temp_unit(from)
  to   <- .norm_temp_unit(to)
  if (is.na(from) || is.na(to)) stop("Unknown temperature units (from=", from, ", to=", to, ")")
  if (identical(from, to)) return(x)

  if (from == "degF") xC <- (x - 32) * 5/9 else
  if (from == "K")    xC <- x - 273.15 else
                       xC <- x

  if (to == "degF")   return(xC * 9/5 + 32)
  if (to == "K")      return(xC + 273.15)
  xC
}

#' Internal: read friendly unit attribute
#' @keywords internal
#' @noRd
.attr_unit <- function(x) .norm_temp_unit(attr(x, "unit"))
