
#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
NumericVector mavtime_cpp(NumericVector x, int window) {
    int n = x.size();
    NumericVector result(n, NA_REAL);  // Initialize with NA
    double sum = 0;                   // Sum of values in the window
    int count = 0;                    // Count of valid (non-NA) values in the window

    for (int i = 0; i < n; ++i) {
        // Add the current value if it's not NA
        if (!NumericVector::is_na(x[i])) {
            sum += x[i];
            count++;
        }

        // Remove the value that's leaving the window
        if (i >= window) {
            if (!NumericVector::is_na(x[i - window])) {
                sum -= x[i - window];
                count--;
            }
        }

        // Calculate the moving average if there is at least one valid value
        if (i >= window - 1 && count > 0) {
            result[i] = sum / count;
        }
    }

    return result;
}

