#' @title Start a Parallel Cluster with a Cross-Platform Watchdog (Temp Stop File)
#'
#' @description
#' `startEnvironment()` sets up a parallel cluster and starts a watchdog script
#' that monitors memory usage on Windows or Linux. It writes out default watchdog scripts if needed.
#'
#' If `stop_file` is not provided, a unique temporary filename (with random characters) is generated.
#'
#' @param num_cores Integer. Number of cores to use for the parallel cluster. Defaults to 2.
#' @param threshold Numeric. The memory usage threshold (0 to 1) at which the watchdog triggers. Default 0.99.
#' @param consecutive_seconds Integer. How many consecutive seconds memory must be above threshold before killing. Default 15.
#' @param stop_file Character. The full path and filename of the stop file to be used by the watchdog.
#'   If not specified, a random unique temp file name is created.
#' @param windows_path Character. The directory for `watchdog2.ps1` on Windows. Defaults to `"C:\\Users\\Jordan\\Desktop"`.
#' @param linux_path Character. The directory for `watchdog2.sh` on Linux. Defaults to `"~/scripts"`.
#'
#' @details
#' - On Windows, uses a PowerShell script (`watchdog2.ps1`).
#' - On Linux, uses a Bash script (`watchdog2.sh`).
#' - If the watchdog script doesn't exist, it is written out.
#' - If `stop_file` is `NULL`, a unique temp file is created automatically.
#'
#' The watchdog monitors memory usage. If it stays above `threshold` for `consecutive_seconds`,
#' it kills the provided worker PIDs. To stop the watchdog gracefully, create the stop file
#' specified by `stop_file`.
#'
#' @return A list: `list(cluster = cl, stop_file = stop_file, worker_pids = pids)`.
#'
#' @examples
#' \dontrun{
#' # Start environment with a random unique stop file name:
#' env_info <- startEnvironment()
#'
#' # ... do some computations ...
#'
#' # Gracefully stop watchdog:
#' # ?jj::closeEnvironment
#' closeEnvironment(env_info)
#' file.create(env_info$stop_file)
#' jstopCluster(env_info$worker_pids)
#  or 
#' # file.create(env_info$stop_file)
#
#' # Kill processes if desired:
#' jstopCluster(env_info$worker_pids)
#' # Then stop cluster (probably DO NOT need to also do this after calling jstopCluster():
#' parallel::stopCluster(env_info$cluster)
#' }
#'
#' @export
startEnvironment <- function(num_cores = 2, threshold = 0.99, consecutive_seconds = 15,
                             stop_file = tempfile("stop_watchdog_"),
                             windows_path = "C:\\Users\\Jordan\\Desktop",
                             linux_path = "~/scripts") {
  os <- Sys.info()[["sysname"]]

  # Determine stop_file if not provided (create a random unique temp file)
  if (is.null(stop_file)) {
    stop_file <- tempfile("stop_watchdog_")
  }

  cl <- parallel::makeCluster(num_cores)
  doParallel::registerDoParallel(cl)

  pids <- parallel::parSapply(cl, 1:num_cores, function(x) Sys.getpid())
  pid_arg <- paste(pids, collapse = ",")

  if (os == "Windows") {
    watchdogfile <- file.path(windows_path, "watchdog2.ps1")
    # If file doesn't exist, write it out
    if (!file.exists(watchdogfile)) {
      dir.create(windows_path, showWarnings = FALSE, recursive = TRUE)
      cat(
        'param(
    [Parameter(Mandatory=$true)]
    [string]$Pids,

    [Parameter(Mandatory=$true)]
    [double]$Threshold,

    [Parameter(Mandatory=$true)]
    [int]$ConsecutiveSeconds,

    [Parameter(Mandatory=$true)]
    [string]$StopFile
)

$PidList = $Pids.Split(",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -match "^\\d+$" }

if (-not $PidList) {
    Write-Host "No valid PIDs provided. Exiting..."
    exit
}

if ($Threshold -le 0 -or $Threshold -gt 1) {
    Write-Host "Invalid threshold. Must be between 0 and 1. Exiting..."
    exit
}

if ($ConsecutiveSeconds -le 0) {
    Write-Host "Invalid ConsecutiveSeconds. Must be a positive integer. Exiting..."
    exit
}

$Counter = 0

while (-not (Test-Path $StopFile)) {
    $mem = Get-Counter \'\\Memory\\Available MBytes\'
    $AvailableMB = $mem.CounterSamples[0].CookedValue
    $TotalMB = (Get-CimInstance Win32_OperatingSystem).TotalVisibleMemorySize / 1024
    $UsedPercent = (1 - ($AvailableMB / $TotalMB))

    if ($UsedPercent -ge $Threshold) {
        $Counter++
    } else {
        $Counter = 0
    }

    if ($Counter -ge $ConsecutiveSeconds) {
        Write-Host "Memory usage exceeded $($Threshold * 100)% for $ConsecutiveSeconds seconds. Killing processes..."
        foreach ($procId in $PidList) {
            $proc = Get-Process -Id $procId -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Host "Killing process with PID $procId"
                Stop-Process -Id $procId -Force
            } else {
                Write-Host "No process found with PID $procId"
            }
        }
        exit
    }

    Start-Sleep -Seconds 1
}

Write-Host "Stop signal detected, exiting..."
',
        file = watchdogfile, sep = ""
      )
    }

    # system2(
      # "powershell",
      # c(
        # "-ExecutionPolicy", "Bypass",
        # "-File", watchdogfile,
        # pid_arg,
        # as.character(threshold),
        # as.character(consecutive_seconds),
        # stop_file
      # ),
      # wait = FALSE
    # )
	
	system2(
	  "powershell",
	  args = c(
		"-ExecutionPolicy", "Bypass",
		"-File", watchdogfile,
		"-Pids", pid_arg,
		"-Threshold", as.character(threshold),
		"-ConsecutiveSeconds", as.character(consecutive_seconds),
		"-StopFile", stop_file
	  ),
	  wait = FALSE
	)


  } else {
    # Linux
    watchdogfile <- file.path(path.expand(linux_path), "watchdog2.sh")
    # If file doesn't exist, write it out
    if (!file.exists(watchdogfile)) {
      dir.create(path.expand(linux_path), showWarnings = FALSE, recursive = TRUE)
      cat(
        '#!/usr/bin/env bash
# Usage: ./watchdog2.sh "pid1,pid2,..." THRESHOLD CONSECUTIVE_SECONDS STOP_FILE

PIDS=$1
THRESHOLD=$2
CONSECUTIVE_SECONDS=$3
STOP_FILE=$4

IFS=',' read -r -a PID_ARRAY <<< "$PIDS"

if [ -z "${PID_ARRAY[*]}" ]; then
  echo "No valid PIDs provided. Exiting..."
  exit 1
fi

if (( $(echo "$THRESHOLD <= 0 || $THRESHOLD > 1" | bc -l) )); then
  echo "Invalid threshold. Must be between 0 and 1. Exiting..."
  exit 1
fi

if [ "$CONSECUTIVE_SECONDS" -le 0 ]; then
  echo "Invalid ConsecutiveSeconds. Must be a positive integer. Exiting..."
  exit 1
fi

COUNTER=0

get_used_percent() {
    MemTotal=$(grep MemTotal /proc/meminfo | awk \'{print $2}\')
    MemAvailable=$(grep MemAvailable /proc/meminfo | awk \'{print $2}\')
    UsedRatio=$(echo "1 - ($MemAvailable/$MemTotal)" | bc -l)
    echo "$UsedRatio"
}

while [ ! -f "$STOP_FILE" ]; do
    UsedPercent=$(get_used_percent)

    exceed=$(echo "$UsedPercent >= $THRESHOLD" | bc -l)
    if [ "$exceed" -eq 1 ]; then
        COUNTER=$((COUNTER+1))
    else
        COUNTER=0
    fi


    if [ $COUNTER -ge $CONSECUTIVE_SECONDS ]; then
        echo "Memory usage exceeded $(echo "$THRESHOLD*100" | bc)% for $CONSECUTIVE_SECONDS seconds. Killing processes..."
        for procId in "${PID_ARRAY[@]}"; do
            if ps -p $procId > /dev/null; then
                echo "Killing process with PID $procId"
                kill -9 $procId
            else
                echo "No process found with PID $procId"
            fi
        done
        exit
    fi

    sleep 1
done

echo "Stop signal detected, exiting..."
',
        file = watchdogfile, sep = ""
      )
      Sys.chmod(watchdogfile, "0755")
    }

    system2(
      "bash",
      c(
        watchdogfile,
        pid_arg,
        as.character(threshold),
        as.character(consecutive_seconds),
        stop_file
      ),
      wait = FALSE
    )
  }

  message("Cluster started, watchdog script running, and worker PIDs retrieved.")
  return(list(cluster = cl, stop_file = stop_file, worker_pids = pids))
}
