source(here::here('R', 'utilities.R'))

# Main process ------------------------------------------------------------
run = \(){
    # Authorization
    auth = get_scto_auth()
    set_google_auth()
    # Global Variables
    params = get_params('params.yaml')
    synced_at = format(Sys.time(), '%Y-%m-%dT%H:%M:%SZ', tz = 'GMT')
    catalog_source = scto_catalog(auth)
    catalog_source[, `:=`(
      last_version_created_at = format(last_version_created_at, '%FT%XZ'),
      last_incoming_data_at = format(last_incoming_data_at, '%FT%XZ'))]
    def_dir = tempdir()

    output_folder_ls = setDT(drive_ls(params$output_folder_url))
    history_file = get_history_file(output_folder_ls)
    forms_to_sync = get_forms_to_sync(history_file,catalog_source,output_folder_ls)
    if(nrow(forms_to_sync) == 0){
      return()
    }

    # >>>>> TESTING
    if (params$environment == 'dev') {
      set.seed(1984)
      forms_to_sync = forms_to_sync[sample.int(.N, 1L)]
    }
    # <<<<< TESTING

    # Meta info of all versions
    meta_info = get_form_scto_metadata(forms_to_sync)
    # Sync forms
    syncs = sync_form_definitions(
      auth,params,def_dir,forms_to_sync,
      catalog_source,output_folder_ls,meta_info,synced_at)

    # Update history file
    update_history_file(params,forms_to_sync,syncs,output_folder_ls,
                        catalog_source)

  }

# Run main process --------------------------------------------------------
run()


