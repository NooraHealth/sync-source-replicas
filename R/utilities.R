library('cli')
library('data.table')
# library('doParallel')
library('foreach')
library('glue')
library('googledrive')
library('googlesheets4')
library('here')
library('progress')
library('rsurveycto')


db_get_query = \(...) setDT(DBI::dbGetQuery(...))
read_sheet_dt = \(...) setDT(read_sheet(...))


# Parameters ----------------------------------------------------------
get_params = \(path, envir = NULL) {
  params_raw = yaml::read_yaml(path)
  if (is.null(envir)) {
    envir = if (Sys.getenv('GITHUB_REF_NAME') == 'main') 'prod' else 'dev'
  }
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


set_google_auth = \(
  auth_file = 'google-token.json', type = c('bigquery', 'drive', 'gs4')) {
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

  if ('bigquery' %in% type) bigrquery::bq_auth(path = path)
  if ('drive' %in% type) drive_auth(path = path)
  if ('gs4' %in% type) gs4_auth(path = path)
}


# Change detection -----------------------------------------------
get_files_to_remove = \(folder_meta, catalog_source) {
  files = folder_meta[
    !(name == '_history' | startsWith(name, '(removed '))][
      !catalog_source[type == 'form'], on = c('name' = 'id')]
  files
}


get_forms_to_sync = \(history_file, catalog_source) {
  if (is.na(history_file)) {
    catalog_merged = copy(catalog_source)[, last_version_created_at_dest := NA]
    errors_dest = NULL
  } else {
    catalog_dest = read_sheet_dt(history_file, sheet = '_catalog')
    errors_dest = read_sheet_dt(history_file, sheet = '_errors')
    catalog_merged = merge(
      catalog_source, catalog_dest[, .(id, last_version_created_at)],
      by = 'id', all.x = TRUE, suffixes = c('', '_dest'))
  }
  forms = catalog_merged[
    is_deployed == TRUE &
      (is.na(last_version_created_at_dest) |
         (last_version_created_at != last_version_created_at_dest) |
         id %in% errors_dest$id)]
  forms
}


# Lookup methods ----------------------------------------------------------
get_history_file = \(folder_meta) {
  file_if_exists = folder_meta[name == '_history']
  history_file = if (nrow(file_if_exists) > 0L) file_if_exists$id[1L] else NA
  history_file
}


# Deal with zombie forms -----------------------------------------------------
check_is_form_zombie = \(form_id, form_meta, folder_meta) {
  form_file = folder_meta[name == form_id]
  if (nrow(form_file) == 0L) return(NA)

  file_id = form_file$id[1L]
  sht_names = sheet_names(file_id)
  if (!('_versions' %in% sht_names)) return(NA)

  ver_cols = c('form_version', 'date_str', 'actor')
  versions_dest = read_sheet_dt(file_id, '_versions')[, ..ver_cols]
  versions_source = form_meta[, ..ver_cols]

  versions_discrepant = fsetdiff(versions_dest, versions_source)
  if (nrow(versions_discrepant) == 0L) return(NA)
  file_id
}


rename_zombie_file = \(form_id, file_id, synced_at) {
  new_name = paste0('(removed ', synced_at, ') ', form_id)
  drive_rename(file = file_id, name = new_name, overwrite = TRUE)
}


# Main function to sync forms ---------------------------------------------
sync_form_definitions = \(auth, forms, folder_url, folder_meta, synced_at) {
  forms_iter = iterators::iter(forms, by = 'row')
  syncs = foreach(form = forms_iter, .combine = rbind) %do% { # %dopar%
    f = tryCatch({
      # Metadata of all versions
      form_meta = scto_get_form_metadata(
        auth, form_ids = form$id, deployed_only = FALSE, get_defs = FALSE)

      # Check if form is a zombie
      zombie_file = check_is_form_zombie(form$id, form_meta, folder_meta)
      if (!is.na(zombie_file)) {
        cli_alert_warning(
          'Form {.val {form$id}} is a zombie; renaming the previous file.')
        rename_zombie_file(form$id, zombie_file, synced_at)
      }

      # After renaming, it's business as usual
      update_form_definition(auth, form$id, folder_url, form_meta, synced_at)
    }, error = \(e) e)

    error = if (inherits(f, 'error')) as.character(f) else NA_character_
    if (inherits(f, 'error')) {
      cli_alert_danger('Error while syncing form {.val {form$id}}.')
    }
    form[, .(id, form_version, synced_at = ..synced_at, error = ..error)]
  }
  syncs
}


# Update methods ----------------------------------------------------------
fetch_form_definition = \(auth, form_id, def_dir = tempdir()) {
  meta = scto_get_form_metadata(
    auth, form_id, deployed_only = TRUE, def_dir = def_dir)
  ext = tools::file_ext(meta$filename)
  media = here(def_dir, glue('{meta$form_id}__{meta$form_version}.{ext}'))
  media
}


sheet_append_safe = \(ss, data, sheet) {
  ss_meta = gs4_get(ss)
  sheets = as.data.table(ss_meta$sheets)
  num_rows = sheets[name == sheet]$grid_rows + nrow(data)
  sheet_resize(ss, sheet, nrow = num_rows)
  sheet_append(ss = ss, data = data, sheet = sheet)
}


update_form_definition = \(auth, form_id, folder_url, form_meta, synced_at) {
  media = fetch_form_definition(auth, form_id)
  form_file = drive_put(
    media = media, name = form_id, path = folder_url, type = 'spreadsheet')

  # Check if _syncs and _versions sheets exist, else create
  sht_names = sheet_names(form_file)
  if (!('_syncs' %in% sht_names)) {
    syncs_empty = data.table(form_version = NA, synced_at = NA)
    sheet_write(syncs_empty, form_file, '_syncs')
  }

  if (!('_versions' %in% sht_names)) sheet_add(form_file, '_versions')

  # Append to existing sheet
  form_dt = form_meta[
    is_deployed == TRUE, .(form_version, synced_at = ..synced_at)]
  sheet_append_safe(ss = form_file, data = form_dt, sheet = '_syncs')
  range_autofit(form_file, '_syncs')

  form_versions = form_meta[, .(form_version, date_str, actor, is_deployed)]
  sheet_write(form_versions, form_file, '_versions')
  range_autofit(form_file, '_versions')
  invisible(form_file)
}


update_history_file = \(history_file, folder_url, catalog_source, syncs) {
  if (is.na(history_file)) {
    history_file = gs4_create(
      '_history', sheets = c('_catalog', '_syncs', '_errors'))
    drive_mv(history_file, path = as_id(folder_url))
    syncs_empty = data.table(id = NA, form_version = NA, synced_at = NA)
    sheet_write(syncs_empty, history_file, '_syncs')
  }

  sheet_write(catalog_source, history_file, '_catalog')
  range_autofit(history_file, '_catalog')

  syncs_ok = syncs[is.na(error), !'error']
  if (nrow(syncs_ok) > 0L) {
    sheet_append_safe(ss = history_file, data = syncs_ok, sheet = '_syncs')
    range_autofit(history_file, '_syncs')
  }

  sheet_write(syncs[!is.na(error)], history_file, '_errors')
  range_autofit(history_file, '_errors')
  invisible(history_file)
}
