source(here::here('R', 'utilities.R'))

# registerDoParallel() # prevents cli messages, could maybe use futures
params = get_params('params.yaml')
auth = get_scto_auth()
set_google_auth()

def_dir = tempdir()
synced_at = format(Sys.time(), '%FT%XZ', tz = 'GMT')
catalog_source = scto_catalog(auth)

# deal with timestamps changing by one second in round trip from gsheet
catalog_source[, `:=`(
  last_version_created_at = format(last_version_created_at, '%FT%XZ'),
  last_incoming_data_at = format(last_incoming_data_at, '%FT%XZ'))]

output_folder_ls = setDT(drive_ls(params$output_folder_url))
history_file = output_folder_ls[name == '_history']
history_file = if (nrow(history_file) > 0L) history_file$id[1L] else NA

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
    (is.na(last_version_created_at_dest) |
       (last_version_created_at > last_version_created_at_dest))]

# >>>>> TESTING
if (params$environment == 'dev') {
  set.seed(1984)
  forms = forms[sample.int(.N, 3L)]
}
# <<<<< TESTING

cli_alert_success('Attempting to sync definitions for {nrow(forms)} form{?s}.')
syncs_empty = data.table(id = NA, form_version = NA, synced_at = NA)

pb = progress_bar$new(total = nrow(forms), show_after = 0)
foreach_form = foreach(
  form = iterators::iter(forms, by = 'row'), .combine = rbind)

syncs = foreach_form %do% { # %dopar%
  f = tryCatch({
    pb$tick()
    cat('\n')
    metadata = scto_get_form_metadata(
      auth, form$id, deployed_only = TRUE, def_dir = def_dir)

    ext = if (grepl('\\?file=.+\\.xlsx', tolower(metadata$download_link)))
      'xlsx' else 'xls'
    media = here(def_dir, glue('{form$id}__{form$form_version}.{ext}'))
    form_file = drive_put(
      media = media, name = form$id, path = params$output_folder_url,
      type = 'spreadsheet')

    if (!('_syncs' %in% sheet_names(form_file))) {
      sheet_write(syncs_empty[, !'id'], form_file, '_syncs')
    }
    sheet_append(
      form_file, form[, .(form_version, synced_at = ..synced_at)], '_syncs')
    range_autofit(form_file, '_syncs')

  }, error = \(e) e)

  error = if (inherits(f, 'error')) as.character(f) else NA_character_
  form[, .(id, form_version, synced_at = ..synced_at, error = ..error)]
}

if (nrow(forms) > 0L) {
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

forms_removed = output_folder_ls[
  name != '_history' & !startsWith(name, '(removed) ')][
    !catalog_source, on = c('name' = 'id')]

for (i in seq_len(nrow(forms_removed))) {
  drive_rename(
    forms_removed$id[i], paste('(removed)', forms_removed$name[i]),
    overwrite = TRUE)
}
