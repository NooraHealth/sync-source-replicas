# ==============================================================================
# Data Sync Modules (REFRACTORY REFACTORING)
# File: R/hrpw_utilities.R
# ==============================================================================

# ---- Normalization ----
format_datetime = \(x) format(x, '%F %X', tz = 'GMT')

set_column_classes = \(d) {
  for (col in colnames(d)) {
    vals = if (inherits(d[[col]], c('POSIXct', 'POSIXlt'))) {
      format_datetime(d[[col]])
    } else if (inherits(d[[col]], c('Date', 'factor'))) {
      as.character(d[[col]])
    } else if (is.logical(d[[col]])) {
      as.integer(d[[col]])
    } else {
      NULL
    }
    if (!is.null(vals)) set(d, j = col, value = vals)
  }
  invisible(d)
}

# ---- SQL Parsing ----
parse_queries_file = \(params) {
  queries_str = readChar(
    params$queries_file, file.info(params$queries_file)$size)

  rej = '-- dataset_id: '
  queries = strsplit(queries_str, paste0(rej, '[a-zA-Z0-9_]+.'))[[1L]][-1L]
  queries_lines = strsplit(queries_str, '\\n')[[1L]]
  dataset_ids = gsub(rej, '', queries_lines[grepl(rej, queries_lines)])

  param_ids = sapply(params$datasets, \(x) x$id)
  params_new = params
  for (i in seq_len(length(queries))) {
    idx = which(dataset_ids[i] == param_ids)
    params_new$datasets[[idx]]$query = queries[i]
  }
  params_new
}

# ---- Sync Status ----
write_sync_status = \(file_url, dataset_id, num_rows) {
  status = data.table(
    dataset_id = dataset_id,
    rows_updated = num_rows,
    synced_at = format_datetime(Sys.time())
  )
  sheet_append_safe(ss = file_url, data = status, sheet = 'status')
}

sync_dataset = \(dataset_params, con, auth, file_url, catalog) {
  dataset_id = dataset_params$id
  id_type = catalog[id == dataset_id]$type

  df_scto = if (id_type == 'dataset') {
    scto_read(auth, dataset_id)
  } else if (id_type == 'form') {
    cli_alert_warning(
      '{.val {dataset_id}} corresponds to a form, not a dataset.')
    return(0L)
  } else {
    cli_alert_warning('{.val {dataset_id}} does not exist on SurveyCTO.')
    data.table(id = character())
  }

  file_meta = gs4_get(file_url)
  df_sheet = if (dataset_id %in% file_meta$sheets$name) {
    read_sheet_dt(file_url, sheet = dataset_id)
  } else {
    NULL
  }

  df_target = df_sheet %||% df_scto # TODO: in production, use only df_scto

  df_source = db_get_query(con, dataset_params$query)
  if (nrow(df_source) == 0L) {
    cli_alert_warning('Database query for {.val {dataset_id}} returned 0 rows.')
    return(0L)
  }

  df_new = if (nrow(df_target) == 0L) {
    df_source
  } else if ('id' %notin% colnames(df_target)) {
    cli_alert_warning(
      '{.val {dataset_id}} on SurveyCTO or Google Sheet lacks `id` column.')
    return(0L)
  } else {
    df_source[as.character(id) %notin% as.character(df_target$id)]
  }

  if (nrow(df_new) == 0L) {
    cli_alert_warning('{.val {dataset_id}} has no new records to sync.')
    return(0L)
  }

  set_column_classes(df_new)

  # TODO: would be easier to update enumerator info if it came from the gsheet
  col = 'enumerator_email'
  assignments = rep_len(sample(p[[col]]), nrow(df_new))
  set(df_new, j = col, value = assignments)

  scto_write(auth, df_new, dataset_id, append = TRUE, fill = TRUE)

  if (is.null(df_sheet)) {
    sheet_write(data = df_new, ss = file_url, sheet = dataset_id)
  } else {
    df_sheet_new = rbind(df_sheet[0L], df_new, fill = TRUE)
    sheet_append_safe(ss = file_url, data = df_sheet_new, sheet = dataset_id)
  }

  nrow(df_new)
}

sync_datasets = \(params, con, auth) {
  file_url = params$output_file_url
  catalog = scto_catalog(auth)
  for (p in params$datasets) {
    r = tryCatch(sync_dataset(p, con, auth, file_url, catalog), error = \(e) e)

    n = if (inherits(r, 'error')) {
      cli_bullets(
        c('x' = 'Sync failed for {.val {p$id}}:', ' ' = as.character(r)))
      -1L
    } else {
      cli_alert_success('Sync completed for {.val {p$id}}.')
      r
    }
    write_sync_status(file_url, p$id, n)
  }
}
