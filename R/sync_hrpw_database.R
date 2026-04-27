# ---- Load Libraries ----
library('DBI')
library('here')
library('RPostgres')

source(here('R', 'utilities.R'))
source(here('R', 'hrpw_utilities.R'))
set.seed(2001)

# ---- Main Execution ----
sync_hrpw_database = function(params) {
  cli_alert_success('Sync started.')

  # Parse SQL queries
  params = parse_queries_file(params)

  # Initialize connections
  set_google_auth(auth_file = 'google_token.json', type = 'gs4')
  con = do.call(dbConnect, c(list(drv = Postgres()), params$database))
  auth = get_scto_auth()

  # Process datasets
  sync_datasets(params = params, con = con, auth = auth)

  # Cleanup
  dbDisconnect(con)

  cli_alert_success('Sync completed.')
}

# ---- Setup and Run ----
params = get_params(here('params', 'hrpw.yaml'))

# Run sync
sync_hrpw_database(params)
