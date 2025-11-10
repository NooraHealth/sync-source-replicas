source(here::here('R', 'utilities.R'))

sync_hep_database_replica = \(con, query_path, params) {

  if (!is.null(params$scto_id_suffix)) auth = get_scto_auth()

  query_str = readChar(query_path, file.info(query_path)$size)

  rej = '-- sheet_name: '
  queries = strsplit(query_str, paste0(rej, '[a-z_]+.'))[[1L]][-1L]
  query_lines = strsplit(query_str, '\\n')[[1L]]
  sheet_names = gsub(rej, '', query_lines[grepl(rej, query_lines)])

  for (i in seq_len(length(queries))) {
    d = db_get_query(con, queries[i])
    dataset_id = paste0('hep_', sheet_names[i])

    if (!is.null(params$output_file_url)) {
      sheet_write(d, ss = params$output_file_url, sheet = sheet_names[i])
    }

    if (!is.null(params$output_folder_url)) {
      media = here(tempdir(), paste0(dataset_id, '.csv'))
      fwrite(d, media)
      drive_put(media, path = params$output_folder_url, name = dataset_id)
    }

    if (!is.null(params$scto_id_suffix)) {
      scto_write(auth, d, paste0(dataset_id, params$scto_id_suffix))
    }
  }

  status = data.table(synced_at = format(Sys.time(), '%FT%XZ', tz = 'GMT'))
  sheet_write(status, ss = params$output_file_url, sheet = 'status')
}


set_google_auth()
params = get_params(here('params', 'hep_database.yaml'))
con = DBI::dbConnect(bigrquery::bigquery(), project = params$project)

query_path = here('R', 'queries.sql')

sync_hep_database_replica(con, query_path, params)
