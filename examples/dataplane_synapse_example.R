# =============================================================================
# Synapse read/write example (dataplane / synapse_rest_backend)
# =============================================================================
# This script assumes the following functions are available (exported):
# - syn_resolve_token()
# - syn_ensure_folder_path()
# - syn_resolve_entity_by_path()
# - syn_upload_file()
# - syn_upload_file_path()
# - syn_download_file()
# - syn_download_file_path()
# - syn_get_entity()
# - syn_get_annotations()
# - syn_set_annotations()
#
# =============================================================================
# 
# devtools::install_github("jclark50/dataplane")
# remotes::install_github("jclark50/dataplane")

suppressPackageStartupMessages({
  library(dataplane)
  library(tools)
})

# =============================================================================
# 0) Configuration (EDIT THESE)
# =============================================================================

# Base container where uploads go: a Synapse Project or Folder synId.
# Example: "syn12345678"
BASE_ID <- "syn66524038" # Heat Hub Core Datasets

# Remote folder path under BASE_ID (created if missing).
# This is a *path string* like "dataplane/examples/synapse".
REMOTE_FOLDER <- "dataplane/examples/synapse"

# Remote "file path" under BASE_ID used by syn_resolve_entity_by_path() and syn_download_file_path().
# This typically includes folder(s) + filename.
REMOTE_FILE_PATH <- file.path(REMOTE_FOLDER, "example_payload.csv")

# Verbose logging + dry-run mode
VERBOSE <- TRUE
DRY_RUN <- FALSE

# =============================================================================
# 1) Authentication (token resolution)
# =============================================================================
# Tokens are resolved in this order:
# 1) token= argument (if provided)
# 2) env var SYNAPSE_PAT        (preferred)
# 3) env var SYNAPSE_AUTH_TOKEN (fallback)

# Recommended:
# Sys.setenv(SYNAPSE_PAT = "YOUR_PAT")

# token <- c("YOURTOKENHERE") 
           
token <- syn_resolve_token(token = NULL)  # will read env vars
if (!nzchar(token)) stop("No Synapse token resolved. Set SYNAPSE_PAT or SYNAPSE_AUTH_TOKEN.")

# =============================================================================
# 2) Create a small local file to upload
# =============================================================================

local_dir  <- file.path(tempdir(), "dataplane_synapse_example")
dir.create(local_dir, recursive = TRUE, showWarnings = FALSE)

local_file <- file.path(local_dir, "example_payload.csv")

payload <- data.frame(
  site_id = c("A", "A", "B"),
  ts_utc  = c("2026-01-10 12:00:00", "2026-01-10 13:00:00", "2026-01-10 12:00:00"),
  ta_c    = c(1.3, 2.1, -0.4),
  rh_pct  = c(80, 75, 85)
)

write.csv(payload, local_file, row.names = FALSE)
stopifnot(file.exists(local_file))

# Local integrity hash (Synapse upload uses MD5 by default when md5=TRUE)
local_md5 <- as.character(tools::md5sum(local_file))
if (VERBOSE) message("Local file: ", local_file, " (md5=", local_md5, ")")

# =============================================================================
# 3) Ensure remote folder path exists
# =============================================================================

remote_folder_id <- syn_ensure_folder_path(
  parent_id = BASE_ID,
  path      = REMOTE_FOLDER,
  token     = token,
  verbose   = VERBOSE,
  dry_run   = DRY_RUN
)

if (VERBOSE) message("Remote folder id: ", remote_folder_id)

# =============================================================================
# 4) Upload (write) using syn_upload_file_path()
# =============================================================================
# syn_upload_file_path():
# - ensures REMOTE_FOLDER exists under BASE_ID
# - uploads local file via multipart flow
# - creates FileEntity if missing
# - if file exists and overwrite=TRUE, updates the existing FileEntity’s file handle
# - optionally sets annotations

annotations <- list(
  owner        = "dataplane",
  purpose      = "example",
  created_utc  = format(Sys.time(), tz = "UTC"),
  file_kind    = "csv"
)

up <- syn_upload_file_path(
  local_path          = local_file,
  base_id             = BASE_ID,
  remote_folder_path  = REMOTE_FOLDER,
  name                = basename(local_file),
  contentType         = "text/csv",
  overwrite           = FALSE,          # set TRUE if you want to update existing
  description         = "dataplane Synapse upload example (CSV).",
  annotations         = annotations,
  token               = token,
  verbose             = VERBOSE,
  dry_run             = DRY_RUN
)

# Returned structure includes file_entity_id, file_handle_id, upload_id, etc.
if (VERBOSE) {
  message("Upload result:")
  print(up)
}

file_entity_id <- up$file_entity_id
stopifnot(is.character(file_entity_id), nzchar(file_entity_id))

# =============================================================================
# 5) Confirm entity + annotations
# =============================================================================

ent <- syn_get_entity(file_entity_id, token = token, verbose = VERBOSE, dry_run = DRY_RUN)
if (VERBOSE) {
  message("Entity summary:")
  print(list(
    id = ent$id,
    name = ent$name,
    parentId = ent$parentId,
    concreteType = ent$concreteType,
    dataFileHandleId = ent$dataFileHandleId
  ))
}

ann <- syn_get_annotations(file_entity_id, token = token, verbose = VERBOSE, dry_run = DRY_RUN)
if (VERBOSE) {
  message("Annotations:")
  print(ann)
}

# =============================================================================
# 6) Resolve entity ID by remote path (read by path)
# =============================================================================
# This demonstrates that your folder/file resolution logic works.
resolved_id <- syn_resolve_entity_by_path(
  base_id     = BASE_ID,
  remote_path = REMOTE_FILE_PATH,
  token       = token,
  verbose     = VERBOSE,
  dry_run     = DRY_RUN
)

if (VERBOSE) message("Resolved id by path: ", resolved_id)

# =============================================================================
# 7) Download (read) by entity id
# =============================================================================

download_dir <- file.path(local_dir, "downloads")
dir.create(download_dir, recursive = TRUE, showWarnings = FALSE)

download_path_by_id <- file.path(download_dir, "download_by_id.csv")

syn_download_file(
  entity_id  = file_entity_id,
  dest_path  = download_path_by_id,
  overwrite  = TRUE,
  token      = token,
  verbose    = VERBOSE,
  dry_run    = DRY_RUN
)

if (!DRY_RUN) {
  stopifnot(file.exists(download_path_by_id))
  dl_md5 <- as.character(tools::md5sum(download_path_by_id))
  if (VERBOSE) message("Downloaded (by id) md5=", dl_md5)
  
  # Integrity check (MD5 should match)
  if (!identical(local_md5, dl_md5)) {
    stop("MD5 mismatch after download-by-id. local=", local_md5, " downloaded=", dl_md5)
  }
}

# =============================================================================
# 8) Download (read) by remote path
# =============================================================================

download_path_by_path <- file.path(download_dir, "download_by_path.csv")

syn_download_file_path(
  base_id          = BASE_ID,
  remote_file_path = REMOTE_FILE_PATH,
  dest_path        = download_path_by_path,
  overwrite        = TRUE,
  token            = token,
  verbose          = VERBOSE,
  dry_run          = DRY_RUN
)

if (!DRY_RUN) {
  stopifnot(file.exists(download_path_by_path))
  dl2_md5 <- as.character(tools::md5sum(download_path_by_path))
  if (VERBOSE) message("Downloaded (by path) md5=", dl2_md5)
  
  if (!identical(local_md5, dl2_md5)) {
    stop("MD5 mismatch after download-by-path. local=", local_md5, " downloaded=", dl2_md5)
  }
}

# =============================================================================
# 9) Update an existing file (overwrite workflow)
# =============================================================================
# Demonstrates the "write again" path: same filename under the same parent folder,
# but overwrite=TRUE causes an update rather than a stop().

# Modify the local payload slightly
payload$ta_c <- payload$ta_c + 0.1
write.csv(payload, local_file, row.names = FALSE)

local_md5_v2 <- as.character(tools::md5sum(local_file))
if (VERBOSE) message("Local updated md5=", local_md5_v2)

up2 <- syn_upload_file_path(
  local_path          = local_file,
  base_id             = BASE_ID,
  remote_folder_path  = REMOTE_FOLDER,
  name                = basename(local_file),
  contentType         = "text/csv",
  overwrite           = TRUE,           # <-- key difference
  description         = "dataplane Synapse upload example (CSV) - updated.",
  annotations         = list(version = "2", updated_utc = format(Sys.time(), tz = "UTC")),
  token               = token,
  verbose             = VERBOSE,
  dry_run             = DRY_RUN
)

if (VERBOSE) {
  message("Upload update result:")
  print(up2)
}

# Re-download and re-check
download_path_v2 <- file.path(download_dir, "download_v2.csv")

syn_download_file(
  entity_id  = file_entity_id,
  dest_path  = download_path_v2,
  overwrite  = TRUE,
  token      = token,
  verbose    = VERBOSE,
  dry_run    = DRY_RUN
)

if (!DRY_RUN) {
  dl_v2_md5 <- as.character(tools::md5sum(download_path_v2))
  if (VERBOSE) message("Downloaded v2 md5=", dl_v2_md5)
  
  if (!identical(local_md5_v2, dl_v2_md5)) {
    stop("MD5 mismatch after overwrite update. local=", local_md5_v2, " downloaded=", dl_v2_md5)
  }
}

message("Done.")
