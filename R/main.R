source(here::here('R', 'utilities.R'))

# Main process ------------------------------------------------------------
sync_replica = \() {
  # Authorization
  auth = get_scto_auth()
  set_google_auth()

  # Parameters
  params = get_params('params.yaml')
  synced_at = format(Sys.time(), '%Y-%m-%dT%H:%M:%SZ', tz = 'GMT')

  catalog_source = scto_catalog(auth)
  catalog_source[, `:=`(
    last_version_created_at = format(last_version_created_at, '%FT%XZ'),
    last_incoming_data_at = format(last_incoming_data_at, '%FT%XZ'))]

  folder_url = params$output_folder_url
  folder_meta = setDT(drive_ls(folder_url))
  history_file = get_history_file(folder_meta)

  forms = get_forms_to_sync(history_file, catalog_source)
  if (nrow(forms) == 0) {
    cli_alert_success('No forms need syncing.')
    return(invisible())
  }

  # >>>>> TESTING
  if (params$environment == 'dev') {
    set.seed(1984)
    forms = forms[sample.int(.N, min(.N, 3L))]
  }
  # <<<<< TESTING

  # Sync forms
  cli_alert_success('Syncing definition{?s} for {nrow(forms)} form{?s}.')
  syncs = sync_form_definitions(auth, forms, folder_url, folder_meta, synced_at)

  # Update history file
  cli_alert_success('Updating history file.')
  update_history_file(history_file, folder_url, catalog_source, syncs)
  invisible()
}

# Run main process --------------------------------------------------------
sync_replica()
