source(here::here('R', 'utilities.R'))


# _history ----------------------------------------------------------------
# rename gdrive_file_meta to folder_meta, same thing for output_folder_ls
get_history_file = \(gdrive_file_meta){
    history_file_id_if_exists=gdrive_file_meta[name == '_history']
    history_file = if (nrow(history_file_id_if_exists) > 0L) history_file_id_if_exists$id[1L] else NA
    history_file
  }


# Main process ------------------------------------------------------------
run = \(){
    # Authorization
    auth = get_scto_auth()
    set_google_auth()
    # Global Variables
    params = get_params('params.yaml')
    synced_at = format(Sys.time(), '%Y-%m-%dT%H:%M:%SZ', tz = 'GMT')
    catalog_source = scto_catalog(auth)
    # Meta info of all versions
    # meta_info = scto_get_form_metadata(auth,
    #                                  form_ids = NULL,
    #                                  deployed_only = FALSE,
    #                                  get_defs = FALSE)
    # deal with timestamps changing by one second in round trip from gsheet
    # meta_info[,`:=`(
    #   date_str = format(date_str, '%FT%XZ'))]
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
    set.seed(1984)
    forms_to_sync = forms_to_sync[sample.int(.N, 10L)]
    # <<<<< TESTING
    # Sync forms
    syncs = sync_form_definitions(auth,params,def_dir,forms_to_sync,
                                  catalog_source,meta_info,
                                  output_folder_ls,synced_at)
    # Update history file
    update_history_file(params,forms_to_sync,syncs,output_folder_ls,
                        catalog_source)
    # # Remove deleted forms
    # rename_removed_forms(output_folder_ls,catalog_source)
  }

# Change detection -----------------------------------------------
get_forms_to_sync = \(history_file,catalog_source,output_folder_ls){
    if (is.na(history_file)) {
      catalog_merged = copy(catalog_source)[, last_version_created_at_dest := NA]
    } else {
      catalog_dest = setDT(read_sheet(history_file, sheet = '_catalog'))
      catalog_merged = merge(
        catalog_source, catalog_dest[, .(id, last_version_created_at)],
        by = 'id', all.x = TRUE, suffixes = c('', '_dest'))
      # Detect ghost forms
      # ghost_forms=detect_ghost_forms(catalog_source,output_folder_ls)
      # if (nrow(ghost_forms)>0L){
      #   catalog_merged=catalog_merged[!detect_ghost_forms(catalog_source,output_folder_ls), by = c('id'='id')]
      # }
    }
    forms = catalog_merged[
      is_deployed == TRUE &
        (is.na(last_version_created_at_dest) | (last_version_created_at != last_version_created_at_dest))]
    return(forms)
  }


sync_form_definitions = \(auth,params,def_dir,forms,catalog,meta,
                          gdrive_file_meta,synced_at){
    syncs_empty = data.table(form_version = NA, synced_at = NA)
    version_empty=data.table(id = NA, form_version = NA, last_version_created_at = NA)
    forms_iter = iterators::iter(forms, by = 'row')

    syncs = foreach(form = forms_iter, .combine = rbind) %do% { # %dopar%
      f = tryCatch({
        # Check if existing forms are zombies
        is_zombie=detect_zombie_forms(form_id = form$id,meta = meta,
                                      gdrive_file_meta = gdrive_file_meta )
        metadata = scto_get_form_metadata(
          auth, form$id, deployed_only = TRUE, def_dir = def_dir)

        ext = if (grepl('\\?file=.+\\.xlsx', tolower(metadata$download_link)))
          'xlsx' else 'xls'
        media = here(def_dir, glue('{form$id}__{form$form_version}.{ext}'))
        form_file = drive_put(
          media = media, name = form$id, path = params$output_folder_url,
          type = 'spreadsheet')
        # Check if _syncs sheet exists, else create
        if (!('_syncs' %in% sheet_names(form_file))) {
          sheet_write(syncs_empty, form_file, '_syncs')
        } # Append to existing sheet
        sheet_append(
          form_file, form[, .(form_version, synced_at = synced_at)], '_syncs')
        range_autofit(form_file, '_syncs')
        # # Check if _versions sheet exists, else create
        # if (!('_versions' %in% sheet_names(form_file))) {
        #   sheet_write(version_empty[, !'id'], form_file, '_versions')
        # } # Append to existing sheet
        sheet_write(
          form_file,
          data=meta[form_id==form$id,.(form_version,date_str,actor,is_deployed)],
          sheet = '_versions')
        range_autofit(form_file, '_versions')
        # This is a lambda function for error handling
      }, error = \(e) e)
      error = if (inherits(f, 'error')) as.character(f) else NA_character_
      form[, .(id, form_version, synced_at = ..synced_at, error = ..error)]
    }
    return(syncs)
  }


update_history_file = \(params,forms,syncs,output_folder,catalog_source){
  syncs_empty = data.table(id = NA, form_version = NA, synced_at = NA)
  # Remove this check because it's getting called again.
    if (nrow(forms) > 0L) {
      history_file=get_history_file(output_folder)
      if (is.na(history_file)) {
        history_file = gs4_create(
          '_history', sheets = c('_catalog', '_syncs', '_errors'))
        drive_mv(history_file, path = as_id(params$output_folder_url))
        sheet_write(syncs_empty, history_file, '_syncs')
      }
      sheet_write(catalog_source, history_file, '_catalog')
      range_autofit(history_file, '_catalog')
      sheet_append(ss= history_file, data = syncs[is.na(error), !'error'], sheet= '_syncs')
      range_autofit(history_file, '_syncs')
      sheet_write(syncs[!is.na(error)], history_file, '_errors')
      range_autofit(history_file, '_errors')
    }
}


get_form_action = \(auth,forms,gdrive_folder_meta){
  # Our possible output
  action_info = data.table(
    id=character(0),
    action=character(0),
    drive_id=character(0))
  # Vector of all forms to be updated
  list_of_forms = forms$id
  # SCTO Meta info
  meta_info = rsurveycto::scto_get_form_metadata(
    auth,form_ids = list_of_forms, deployed_only = FALSE, get_defs = FALSE)
  # Go through the versions already stored in the google sheet
  for (form in list_of_forms) {
    # Get gdrive details
    drive_file = gdrive_folder_meta[name == form]
    drive_id = drive_file$id[1]
    # Read the _version in that form definition
    sheet_version = as.data.table(
          read_sheet(drive_id, sheet = "_versions")
        )[, .(form_version, date_str, actor)]
    # Get meta info for that form
    scto_version = meta_info[form_id == form, .(form_version, date_str, actor)]
    # Set operation to see if there has been any changes
    if (nrow(fsetdiff(sheet_version, scto_version)) != 0){
      action = "rename"
    }
    action = 'update'
    row = data.table(id = form, action = action,
                     drive_id = as.character(drive_id))
    action_info = rbind(action_info,row)
  }
  forms[action_info, on = 'id']
}

rename_zombie_forms = \(auth,form_id,drive_id){
  drive_id=as_id(drive_id)
  # Get the last sync details from the form
  sheets_in_file = sheet_names(drive_id)
  if (!("_versions" %in% sheets_in_file)) {
    message("_versions sheet not found for form_id: ", form_id)
    return(FALSE)
  }
  sheet_sync = as.data.table(read_sheet(
    ss = as_sheets_id(drive_id),
    sheet = '_syncs'
  ))
  new_form_name = paste0('(removed)[',sheet_sync$synced_at,'] ',form_id)
  # Rename the forms
  drive_rename(
    file = drive_id,
    name = new_form_name,
    overwrite = TRUE
  )
}



# detect_zombie_forms = \(form_id, meta,gdrive_file_meta) {
#   drive_file = gdrive_file_meta[name == form_id]
#   # Check if file exists
#   if (nrow(drive_file) == 0) {
#     return(FALSE)
#   }
#   drive_id = drive_file$id[1]
#   # Check if "_versions" sheet exists
#   sheets_in_file = sheet_names(as_sheets_id(drive_id))
#   if (!("_versions" %in% sheets_in_file)) {
#     message("_versions sheet not found for form_id: ", form_id)
#     return(FALSE)
#   }
#   # Read sheet
#   sheet_version = as.data.table(
#     read_sheet(as_sheets_id(drive_id), sheet = "_versions")
#   )
#   # Latest deployed version in sheet
#   last_version_details = sheet_version[is_deployed == TRUE, date_str][1]
#   # Version info from scto metadata
#   scto_version = meta[form_id == form_id, .(form_version, date_str, actor, is_deployed)]
#   common_cols = intersect(names(sheet_version), names(scto_version))
#
#   if (nrow(fsetdiff(sheet_version[, ..common_cols], scto_version[, ..common_cols])) != 0) {
#     new_name = paste0("(removed << ",last_version_details," >>) ", form_id)
#     drive_rename(as_id(drive_id), new_name, overwrite = TRUE)
#     message("Zombie form detected and renamed: ", form_id)
#     return(TRUE)
#   }
#
#   return(FALSE)
# }



# Run main process --------------------------------------------------------
run()

