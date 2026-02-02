# ---- Load Libraries ----
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
params <- get_params(here::here("params", "hrpw_sync.yaml"))

# Authenticate with Google Sheets
set_google_auth(auth_file = "google_token.json", type = "gs4")

# Run sync
sync_hrpw_database(params)
