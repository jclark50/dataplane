getwd()
file.exists("DESCRIPTION")
list.files()



r_files <- list.files("R", pattern = "\\.R$", recursive = TRUE, full.names = TRUE)

hits <- lapply(r_files, function(f) {
  lines <- readLines(f, warn = FALSE)
  idx <- grep("\\blibrary\\s*\\(", lines)
  if (length(idx)) {
    data.frame(
      file = f,
      line = idx,
      code = trimws(lines[idx]),
      stringsAsFactors = FALSE
    )
  }
})

do.call(rbind, hits)









r_files <- list.files("R", pattern = "\\.R$", recursive = TRUE, full.names = TRUE)

hits <- lapply(r_files, function(f) {
  lines <- readLines(f, warn = FALSE)
  idx <- grep("\\blibrary\\s*\\(", lines)
  if (length(idx)) {
    data.frame(
      file = f,
      line = idx,
      code = trimws(lines[idx]),
      stringsAsFactors = FALSE
    )
  }
})

do.call(rbind, hits)










hits_req <- lapply(r_files, function(f) {
  lines <- readLines(f, warn = FALSE)
  idx <- grep("\\brequire\\s*\\(", lines)
  if (length(idx)) {
    data.frame(
      file = f,
      line = idx,
      code = trimws(lines[idx]),
      stringsAsFactors = FALSE
    )
  }
})

do.call(rbind, hits_req)



available::available('klimate', browse = FALSE)
available::available('wxther', browse = FALSE)
available::available("weathertools", browse = FALSE)
available::available('wbgtR', browse = FALSE)
available::available('wbgtr', browse = FALSE)


available::available('wbgt', browse = FALSE)





install.packages("gh")
library(gh)

repos <- gh("jclark50", visibility = "all", affiliation = "owner")
repos$name



browseURL("https://github.com/jclark50/dataplane")


# available::available('climater', browse = FALSE)
# available::available("available", browse = FALSE)


