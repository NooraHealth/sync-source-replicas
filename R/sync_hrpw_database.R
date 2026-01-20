# ==============================================================================
# Main HRPW Sync Script
# File: R/sync_hrpw_database.R
# ==============================================================================

library(DBI)
library(RPostgres)
library(rsurveycto)
library(googlesheets4)

source(here::here("R", "utilities.R"))
source(here::here("R", "sync_hrpw_modules.R"))

# ---- Error Tracking ----
options(error = function() {
  cat("\n!!! ERROR OCCURRED !!!\n")
  traceback(2)
})

# ---- Main Execution ----
sync_hrpw_database <- function(params) {
  log_step("Starting HRPW Data Sync Process")

  # Initialize connections
  connections <- initialize_connections(params)

  # Parse SQL queries
  queries_data <- parse_sql_file(here::here(params$queries_file))

  # Process each dataset
  process_all_datasets(
    con = connections$db,
    auth = connections$scto_auth,
    queries_data = queries_data,
    params = params
  )

  # Cleanup
  cleanup_connections(connections$db)

  log_step("ALL SYNCS COMPLETED", level = "SUCCESS")
}

# ---- Setup and Run ----

# Check if running on GitHub Actions
is_github <- Sys.getenv("GITHUB_ACTIONS") == "true"

# Load parameters first
params <- get_params(here::here("params", "hrpw_sync.yaml"))

if (is_github) {
  cat("Running on GitHub Actions\n")

  # Authenticate with Google Sheets using GOOGLE_TOKEN from secrets
  set_google_auth(type = "gs4")

  # Override database credentials from GitHub secrets
  params$database <- list(
    dbname = Sys.getenv("DB_NAME"),
    host = Sys.getenv("DB_HOST"),
    port = as.integer(Sys.getenv("DB_PORT")),
    user = Sys.getenv("DB_USER"),
    password = Sys.getenv("DB_PASSWORD")
  )

} else {
  cat("Running locally\n")

  # Use local authentication - set_google_auth will automatically find the file
  set_google_auth(auth_file = "google_token.json", type = "gs4")
}

# Run sync
sync_hrpw_database(params)
