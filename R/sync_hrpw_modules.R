# ==============================================================================
# Data Sync Modules (REFRACTORED)
# File: R/sync_hrpw_modules.R
# ==============================================================================
# ---- Logging ----

log_step <- function(message, level = "INFO") {

  prefix <- switch(level,
  "INFO" = "[INFO]",
  "SUCCESS" = "[SUCCESS]",
  "ERROR" = "[ERROR]",
  "WARNING" = "[WARN]",
  "SECTION" = "======================================",
  "---"
)
  if (level == "SECTION") {
    cat(sprintf("\n%s\n=== %s ===\n%s\n", prefix, message, prefix))
  } else {
    cat(sprintf("\n%s %s\n", prefix, message))
  }
}

log_detail <- function(message) {
  cat(sprintf("  %s\n", message))
}

# ---- Safe Executor (Central Error Handler) ----

safe_run <- function(expr, default = NULL, msg = NULL) {

  tryCatch(expr, error = function(e) {

    if (!is.null(msg)) {
      log_step(sprintf("%s: %s", msg, e$message), "ERROR")
    }
    default
  })
}

# ---- Connection Management ----
initialize_connections <- function(params) {

  log_step("Initializing Connections", "SECTION")

  con <- dbConnect(
    RPostgres::Postgres(),
    dbname   = params$database$dbname,
    host     = params$database$host,
    port     = params$database$port,
    user     = params$database$user,
    password = params$database$password
  )

  auth <- get_scto_auth()
  log_step("Database Connected", "SUCCESS")
  log_step("SurveyCTO Authenticated", "SUCCESS")
  log_step("Google Sheets Authenticated", "SUCCESS")

  list(db = con, scto_auth = auth)
}

cleanup_connections <- function(con) {
  log_step("Closing Database Connection", "SECTION")
  dbDisconnect(con)
  log_step("Disconnected", "SUCCESS")
}

# ---- Enumerator Assignment ----
assign_next_enumerators <- function(n, enumerators, sheet_url, dataset_id) {
  last_enum <- safe_run({
    df <- read_sheet(sheet_url, sheet = dataset_id)
    tail(df$enumerator_email, 1)
  }, default = NA)
  start <- match(last_enum, enumerators)
  start <- ifelse(is.na(start), 1, start + 1)
  idx <- ((seq_len(n) + start - 2) %% length(enumerators)) + 1
  enumerators[idx]
}

# ---- Normalization ----
normalize_for_sheets <- function(df, ref = NULL) {
  df[] <- lapply(df, function(x) {
    if (inherits(x, c("POSIXct","POSIXlt")))
      format(x, "%Y-%m-%d %H:%M:%S")
    else if (inherits(x, "Date") || is.factor(x))
      as.character(x)
    else x
  })

  if (!is.null(ref)) {
    missing <- setdiff(names(ref), names(df))
    df[missing] <- NA
    df <- df[names(ref)]
  }
  names(df) <- make.names(names(df), unique = TRUE)
  df
}

# ---- SQL Parsing ----
parse_sql_file <- function(filepath) {

  log_step("Parsing SQL File", "SECTION")

  lines <- readLines(filepath, warn = FALSE)
  markers <- grep("-- @dataset_id:", lines)

  if (length(markers) == 0) stop("No dataset markers found")

  queries <- list()
  datasets <- character()
  enumerators <- list()

  for (i in seq_along(markers)) {
    start <- markers[i]
    end <- if (i < length(markers)) markers[i+1]-1 else length(lines)

    block <- lines[start:end]

    dataset_id <- trimws(sub("-- @dataset_id:", "", block[1]))

    enum_line <- block[grepl("-- @enumerators:", block)]
    enum_list <- trimws(unlist(strsplit(sub("-- @enumerators:", "", enum_line), ",")))
    query <- paste(block[!grepl("^--", block)], collapse = "\n")

    datasets <- c(datasets, dataset_id)
    queries[[dataset_id]] <- query
    enumerators[[dataset_id]] <- enum_list
  }

  log_step("SQL Parsed Successfully", "SUCCESS")
  list(
    queries = queries,
    dataset_names = datasets,
    enumerators = enumerators
  )
}

# ---- Read Existing Data (Sheet -> SCTO fallback) ----
read_existing_data <- function(auth, dataset_id, sheet_url) {
  sheet_data <- safe_run(
    read_sheet(sheet_url, sheet = dataset_id),
    default = NULL
  )
  if (!is.null(sheet_data)) return(sheet_data)
  safe_run(
    scto_read(auth, dataset_id),
    default = data.frame(id = character(0))
  )
}

# ---- Identify New Records ----
identify_new_records <- function(df_new, existing) {
  if (nrow(existing) == 0 || !"id" %in% names(existing)) {
    return(df_new)
  }
  df_new[!(as.character(df_new$id) %in% as.character(existing$id)), ]
}

# ---- Upload Handlers ----
upload_to_surveycto <- function(auth, df, dataset_id) {

  safe_run({
    scto_write(auth, df, dataset_id, append = TRUE, fill = TRUE)
    log_step(sprintf("SurveyCTO upload successful: %s", dataset_id), "SUCCESS")
    TRUE
  }, default = FALSE, msg = "SurveyCTO Upload Failed")
}

upload_to_google_sheets <- function(df, sheet_url, dataset_id, sheet_exists) {
  if (nrow(df) == 0) return(TRUE)

  df[] <- lapply(df, function(x) {
    if (inherits(x, c("Date","POSIXct"))) as.character(x)
    else if (is.logical(x)) as.integer(x)
    else as.character(x)
  })

  safe_run({
    if (sheet_exists) {
      sheet_append(df, ss = sheet_url, sheet = dataset_id)
      log_step(sprintf("Appended %d rows → %s", nrow(df), dataset_id), "SUCCESS")
    } else {
      sheet_write(df, ss = sheet_url, sheet = dataset_id)
      log_step(sprintf("Created sheet → %s", dataset_id), "SUCCESS")
    }
    TRUE
  }, default = FALSE, msg = "Sheets Upload Failed")
}

# ---- Sync Status ----
write_sync_status <- function(sheet_url, dataset_id, rows) {

  status <- data.frame(
    dataset_id = dataset_id,
    rows_updated = rows,
    synced_at = Sys.time(),
    stringsAsFactors = FALSE
  )

  safe_run({
    sheet_append(status, ss = sheet_url, sheet = "status")
  }, msg = "Status Write Failed")
}

process_single_dataset <- function(con, auth, dataset_id, query, enumerators, params) {

  log_step(sprintf("PROCESSING: %s", dataset_id), "SECTION")
  df_new <- safe_run(
    dbGetQuery(con, query),
    default = NULL,
    msg = paste("Query Failed:", dataset_id)
  )
  if (is.null(df_new) || nrow(df_new) == 0) {
    log_step("No new data", "WARNING")
    return(0)
  }

  existing <- read_existing_data(auth, dataset_id, params$output_file_url)
  df_new <- identify_new_records(df_new, existing)

  if (nrow(df_new) == 0) {
    log_step("No delta records", "WARNING")
    return(0)
  }

  sheet_data <- safe_run(
    read_sheet(params$output_file_url, sheet = dataset_id),
    default = NULL
  )
  sheet_exists <- !is.null(sheet_data)

  df_new$enumerator_email <- assign_next_enumerators(
    nrow(df_new),
    enumerators,
    params$output_file_url,
    dataset_id
  )

  df_new <- normalize_for_sheets(df_new, sheet_data)
  if (!upload_to_surveycto(auth, df_new, dataset_id)) return(0)
  upload_to_google_sheets(df_new, params$output_file_url, dataset_id, sheet_exists)
  nrow(df_new)
}

process_all_datasets <- function(con, auth, queries_data, params) {
  for (dataset_id in queries_data$dataset_names) {
    rows <- process_single_dataset(
      con,
      auth,
      dataset_id,
      queries_data$queries[[dataset_id]],
      queries_data$enumerators[[dataset_id]],
      params
    )
    log_step(sprintf("Processed %d rows for dataset: %s", rows, dataset_id), "SUCCESS")
    write_sync_status(params$output_file_url, dataset_id, rows)
  }
}
