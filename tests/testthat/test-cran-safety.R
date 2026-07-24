test_that("RAM checks return logical values without terminating R", {
  result <- dataplane::check_ram(
    threshold = 0,
    unit = "B",
    wait_time = 0,
    max_attempts = 1,
    verbose = FALSE
  )
  expect_true(result)
  expect_error(
    dataplane::check_ram(-1, wait_time = 0, max_attempts = 1),
    "non-negative"
  )
})

test_that("process termination requires explicit safe PIDs", {
  expect_error(dataplane::stop_cluster(NULL), "explicitly identify")
  expect_error(dataplane::stop_cluster(Sys.getpid()), "current R process")
})

test_that("parallel environment is limited and closes cleanly", {
  expect_error(dataplane::env_start(3, quiet = TRUE), "1 or 2")
  info <- dataplane::env_start(1, quiet = TRUE)
  expect_false(info$watchdog_started)
  expect_length(info$worker_pids, 1)
  expect_null(dataplane::env_close(info))
})
