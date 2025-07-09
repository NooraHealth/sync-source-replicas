library('data.table')
# library('doParallel')
library('foreach')
library('glue')
library('googledrive')
library('googlesheets4')
library('here')
library('rsurveycto')


# Global variables ----------------------------------------------------------
get_params = \(path) {
  params_raw = yaml::read_yaml(path)
  envir = if (Sys.getenv('GITHUB_REF_NAME') == 'main') 'prod' else 'dev'
  envirs = sapply(params_raw$environments, \(x) x$name)
  params = c(
    params_raw[names(params_raw) != 'environments'],
    params_raw$environments[[which(envirs == envir)]])
  names(params)[names(params) == 'name'] = 'environment'
  params
}


# Authentication ----------------------------------------------------------

get_scto_auth = \(auth_file = 'scto_auth.txt') {
  if (Sys.getenv('SCTO_AUTH') == '') {
    auth_path = here('secrets', auth_file)
  } else {
    auth_path = withr::local_tempfile()
    writeLines(Sys.getenv('SCTO_AUTH'), auth_path)
  }
  scto_auth(auth_path)
}

set_google_auth = \(auth_file = 'google-token.json', type = c('drive', 'gs4')) {
  type = match.arg(type, several.ok = TRUE)
  token_env = Sys.getenv('GOOGLE_TOKEN')
  token_path = here('secrets', auth_file)

  path = if (token_env != '') {
    token_env
  } else if (file.exists(token_path)) {
    token_path
  } else {
    NULL
  }

  if ('drive' %in% type) drive_auth(path = path)
  if ('gs4' %in% type) gs4_auth(path = path)
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
  }
  forms = catalog_merged[
    is_deployed == TRUE &
      (is.na(last_version_created_at_dest) | (last_version_created_at != last_version_created_at_dest))]
  forms
}


# Lookup methods ----------------------------------------------------------
get_history_file = \(gdrive_file_meta){
  history_file_id_if_exists=gdrive_file_meta[name == '_history']
  history_file = if (nrow(history_file_id_if_exists) > 0L) history_file_id_if_exists$id[1L] else NA
  history_file
}

get_form_scto_metadata = \(forms){
  # Vector of all forms to be updated
  list_of_forms = forms$id
  # SCTO Meta info
  meta_info = rsurveycto::scto_get_form_metadata(
    auth,form_ids = list_of_forms, deployed_only = FALSE, get_defs = FALSE)
  return(meta_info)
}


# Determine what kind of update -------------------------------------------
get_form_action = \(auth,forms,gdrive_folder_meta,scto_meta_info){
  # Our possible output
  action_info = data.table(
    id=character(0),
    action=character(0),
    drive_id=character(0))
  # Vector of all forms to be updated
  list_of_forms = forms$id

  # Go through the versions already stored in the google sheet
  for (form in list_of_forms) {
    # Get gdrive details
    drive_file = gdrive_folder_meta[name == form]

    if (nrow(drive_file)==0){
      action = 'update'
      drive_id = ''
    } else {
      drive_id = drive_file$id[1]
      # Read the _version in that form definition
      sheet_version = as.data.table(
        read_sheet(drive_id, sheet = "_versions")
      )[, .(form_version, date_str, actor)]
      # Get meta info for that form
      scto_version = scto_meta_info[form_id == form, .(form_version, date_str, actor)]

      # Set operation to see if there has been any changes
        if (nrow(fsetdiff(sheet_version, scto_version)) != 0){
          action = "rename"
        } else {
          action = 'update'
        }
    }
    # Update action table

    row = data.table(id = form, action = action,
                     drive_id = as.character(drive_id))
    action_info = rbind(action_info,row)
  }
  forms[action_info, on = 'id']
}


# Main function to sync forms ---------------------------------------------
sync_form_definitions = \(auth,params,def_dir,forms,catalog,
                          gdrive_file_meta,scto_meta_info,
                          synced_at){
  # Generate actions for the forms
  forms_action=get_form_action(
    auth = auth,forms = forms,gdrive_folder_meta = gdrive_file_meta,
    scto_meta_info = scto_meta_info
  )

  syncs_empty = data.table(form_version = NA, synced_at = NA)
  version_empty=data.table(id = NA, form_version = NA, last_version_created_at = NA)
  forms_iter = iterators::iter(forms_action, by = 'row')
  syncs = foreach(form = forms_iter, .combine = rbind) %do% { # %dopar%
    f = tryCatch({
      # Download the file definition
      metadata = scto_get_form_metadata(
        auth, form$id, deployed_only = TRUE, def_dir = def_dir)
      ext = if (grepl('\\?file=.+\\.xlsx', tolower(metadata$download_link)))
        'xlsx' else 'xls'
      media = here(def_dir, glue('{form$id}__{form$form_version}.{ext}'))
      # Choice of rename and update or just update
      if(form$action=="rename"){
        rename_zombie_forms(auth,form$id,form$drive_id)
      }
      # After renaming, it's business as usual
      form_file = drive_put(
        media = media, name = form$id, path = params$output_folder_url,
        type = 'spreadsheet')
      # Run form definition details update
      update_form_definition_details(
        form=form, form_file = form_file,
        scto_meta_info = scto_meta_info, syncs_empty = syncs_empty)
    }, error = \(e) e)
    error = if (inherits(f, 'error')) as.character(f) else NA_character_
    form[, .(id, form_version, synced_at = ..synced_at, error = ..error)]
  }
  return(syncs)
}


# Update methods ----------------------------------------------------------

update_form_definition_details = \(form,form_file,scto_meta_info,syncs_empty){
  # Check if _syncs sheet exists, else create
  if (!('_syncs' %in% sheet_names(form_file))) {
    sheet_write(syncs_empty, form_file, '_syncs')
  } # Append to existing sheet
  sheet_append(
    form_file, form[, .(form_version, synced_at = synced_at)], '_syncs')
  range_autofit(form_file, '_syncs')
  sheet_write(
    form_file,
    data=scto_meta_info[form_id==form$id,.(form_version,date_str,actor,is_deployed)],
    sheet = '_versions')
  range_autofit(form_file, '_versions')
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


# Rename zombie forms -----------------------------------------------------

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
