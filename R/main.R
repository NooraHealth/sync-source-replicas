source(here::here('R', 'utilities.R'))


# _history ----------------------------------------------------------------
get_history_file <-
  # Looks for a file called _history in the specified drive location.
  # If it exists, returns the file id, else NA
  function(output_folder){
    history_file_id_if_exists=output_folder[name == '_history']
    history_file = if (nrow(history_file_id_if_exists) > 0L) history_file_id_if_exists$id[1L] else NA
    return(history_file)
  }


# Main process ------------------------------------------------------------
run <-
  function(){
    # Authorization
    auth = get_scto_auth()
    set_google_auth()
    # Global Variables
    params = get_params('params.yaml')
    synced_at = format(Sys.time(), '%Y-%m-%dT%H:%M:%SZ', tz = 'GMT')
    catalog_source = scto_catalog(auth)
    # deal with timestamps changing by one second in round trip from gsheet
    catalog_source[, `:=`(
      last_version_created_at = format(last_version_created_at, '%FT%XZ'),
      last_incoming_data_at = format(last_incoming_data_at, '%FT%XZ'))]
    def_dir = tempdir()
    output_folder_ls = setDT(drive_ls(params$output_folder_url))
    history_file= get_history_file(output_folder= output_folder_ls)
    forms=get_form_def_changed(history_file,catalog_source,output_folder_ls)
    if(nrow(forms)==0){
      return()
    }
    # >>>>> TESTING
    set.seed(1984)
    forms = forms[sample.int(.N, 10L)]
    # <<<<< TESTING

    # Sync forms
    syncs=sync_form_definition(forms)
    # Update history file
    update_history_file(forms,syncs,output_folder_ls)
    # Remove deleted forms
    remove_deleted_forms(output_folder_ls,forms)
  }

# Change detection -----------------------------------------------
get_form_def_changed <-
  function(history_file,catalog_source,output_folder_ls){
    if (is.na(history_file)) {
      catalog_merged = copy(catalog_source)[, last_version_created_at_dest := NA]
    } else {
      catalog_dest = setDT(read_sheet(history_file, sheet = '_catalog'))
      catalog_merged = merge(
        catalog_source, catalog_dest[, .(id, last_version_created_at)],
        by = 'id', all.x = TRUE, suffixes = c('', '_dest'))
      # Detect ghost forms
      ghost_forms=detect_ghost_forms(catalog_source,output_folder_ls)
      if (nrow(ghost_forms)>0L){
        catalog_merged=catalog_merged[!detect_ghost_forms(catalog_source,output_folder_ls), by = c('id'='id')]
      }
    }
    forms = catalog_merged[
      is_deployed == TRUE &
        (is.na(last_version_created_at_dest) | last_version_created_at != last_version_created_at_dest)]
    return(forms)
  }


sync_form_definition <-
  function(forms){
    syncs_empty = data.table(id = NA, form_version = NA, synced_at = NA)
    forms_iter = iterators::iter(forms, by = 'row')

    syncs = foreach(form = forms_iter, .combine = rbind) %do% { # %dopar%
      f = tryCatch({
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
          sheet_write(syncs_empty[, !'id'], form_file, '_syncs')
        } # Append to existing sheet
        sheet_append(
          form_file, form[, .(form_version, synced_at = ..synced_at)], '_syncs')
        range_autofit(form_file, '_syncs')
        # This is a lambda function for error handling
      }, error = \(e) e)
      error = if (inherits(f, 'error')) as.character(f) else NA_character_
      form[, .(id, form_version, synced_at = ..synced_at, error = ..error)]
    }
    return(syncs)
  }


update_history_file <-
  function(forms,syncs,output_folder){
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
      sheet_append(history_file, syncs[is.na(error), !'error'], '_syncs')
      range_autofit(history_file, '_syncs')
      sheet_write(syncs[!is.na(error)], history_file, '_errors')
      range_autofit(history_file, '_errors')
    }
  }


remove_deleted_forms <-
  function(output_folder,forms){
    forms_removed = output_folder[
      name != '_history' & !startsWith(name, '(removed) ')][
        !forms, on = c('name' = 'id')]

    for (i in seq_len(nrow(forms_removed))) {
      drive_rename(
        forms_removed$id[i], paste('(removed)', forms_removed$name[i]),
        overwrite = TRUE)
    }
  }


detect_ghost_forms <-
    function(catalog_source,output_folder_ls){
      # Find the removed files
      list_of_removed_files = copy(output_folder_ls[grepl("^\\(removed\\)", name)])
      setnames(list_of_removed_files,"id","drive_id")
      list_of_removed_files[, name := gsub("^\\(removed\\)\\s*", "", name)]
      # If they occur in the catalog, filter
      catalog=copy(catalog_source)[is_deployed==TRUE]
      ghost_form_info= catalog[list_of_removed_files[,.(name,drive_id)], on = c('id' = 'name'), nomatch = 0]
      # Add this information to _history in _skip sheet
      if(nrow(ghost_form_info)>0){
        sheet_write(
          data = ghost_form_info[,.(title,id,form_version,last_version_created_at)],
          ss = get_history_file(output_folder_ls),
          sheet = '_skip'
        )
        return(ghost_form_info)
      }
      return(ghost_form_info)
    }

# Run main process --------------------------------------------------------
run()

