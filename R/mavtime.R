#' Calculate Rolling Averages for Time-Series Data
#'
#' The `mavtime` function calculates rolling averages over a specified time window
#' for time-series data. This implementation leverages an optimized Rcpp function
#' for enhanced performance on large datasets.
#'
#' @param timevals A `POSIXct` vector of timestamps for the data points.
#' @param datavals A numeric vector of data values corresponding to the timestamps.
#' @param timewindow An integer specifying the size of the rolling window in the units defined by `timeunits`.
#' @param timeunits A character string specifying the time units for the rolling window. Supported values are:
#'   `"min"` (default), `"hour"`, or `"day"`.
#' @param resample A logical indicating whether to resample the data to regular intervals (default: `TRUE`).
#' @param resampleinterval A character string specifying the interval for resampling, e.g., `"1 min"`.
#'
#' @return A numeric vector of rolling averages aligned with the input `timevals`.
#'   Missing values (`NA`) are returned if insufficient data exists within the window.
#'
#' @details
#' This function resamples the data to regular intervals if `resample = TRUE`,
#' ensuring consistent time-series alignment before calculating rolling averages. The rolling
#' averages are computed using a highly optimized Rcpp implementation for performance.
#'
#' @examples
#' library(data.table)
#' 
#' # Generate example time-series data
#' timevals <- seq(from = as.POSIXct("2024-12-01 00:00:00"), by = "1 min", length.out = 100)
#' datavals <- runif(100, min = 0, max = 100)
#' 
#' # Calculate rolling averages with a 15-minute window
#' result <- mavtime(timevals, datavals, timewindow = 15, timeunits = "min")
#'
#' @seealso `rollapply` from the zoo package for a pure R implementation.
#' 
#' @export

mavtime <- function(timevals, datavals, timewindow, timeunits = "min", resample = NULL,
                   resampleinterval = "1 min") 
{
    # Input Validation
    if (!inherits(timevals, "POSIXct")) 
        stop("timevals must be POSIXct.")
    if (!is.numeric(datavals)) 
        stop("datavals must be numeric.")
    
    # Create a data.table with original data

    dt_original <- data.table::data.table(datetime = timevals, value = datavals)
    # Resampling Step
    # Extract unit from resampleinterval (e.g., "1 min" -> "min")
    unit <- sub("^\\d+\\s+", "", resampleinterval)
    
    # Create a regular time sequence based on resampleinterval
    time_seq <- seq(
        from = floor_date(min(dt_original$datetime, na.rm = TRUE), unit = unit),
        to = ceiling_date(max(dt_original$datetime, na.rm = TRUE), unit = unit),
        by = resampleinterval
    )
    
    # Create a data.table for resampled data
    dt_resampled <- data.table(datetime = time_seq)
    
    # Merge original data with resampled time sequence
    dt_merged <- merge(dt_resampled, dt_original, by = "datetime", all.x = TRUE)
    # Compute moving average on the resampled data using Rcpp
    timewindow_minutes <- switch(timeunits, 
        min = timewindow, 
        hour = timewindow * 60, 
        day = timewindow * 60 * 24, 
        stop("Unsupported time unit")
    )
    
    # Handle NA values as per original implementation
    # Rcpp function already handles NA by excluding them from the sum
    value_ma_resampled <- mavtime_cpp(dt_merged$value, window = timewindow_minutes)
    
    # Assign the moving average to the resampled data.table
    # dt_merged[, value_ma := value_ma_resampled]
    dt_merged$value_ma <- value_ma_resampled
	
    # Merge the moving average back to the original data
    # This ensures alignment with the original timestamps
    dt_final <- merge(dt_original, dt_merged[, c('datetime', 'value_ma')], by = "datetime", all.x = TRUE)
    
    # Return the moving average aligned with the original data
    return(dt_final$value_ma)
}
