# ==============================================================================
# Data Sync Modules
# File: R/sync_hrpw_modules.R
# ==============================================================================

# ---- Logging Utilities ----

log_step <- function(message, level = "INFO") {
  prefix <- switch(level,
    "INFO" = "===",
    "SUCCESS" = "✓",
    "ERROR" = "✗",
    "WARNING" = "⚠",
    "SECTION" = "======================================",
    "---"
  )

  if (level == "SECTION") {
    cat(sprintf("\n%s\n", prefix))
    cat(sprintf("=== %s ===\n", message))
    cat(sprintf("%s\n", prefix))
  } else {
    cat(sprintf("\n%s %s\n", prefix, message))
  }
}

log_detail <- function(message) {
  cat(sprintf("  %s\n", message))
}

# ---- Connection Management ----

initialize_connections <- function(params) {
  log_step("STEP 1-3: Initializing Connections")

  # Database connection
  log_detail("Connecting to database...")
  con <- dbConnect(
    RPostgres::Postgres(),
    dbname = params$database$dbname,
    host = params$database$host,
    port = params$database$port,
    user = params$database$user,
    password = params$database$password
  )
  log_step("Database connected", "SUCCESS")

  # SurveyCTO authentication using get_scto_auth from utilities.R
  # This function automatically handles GitHub vs local authentication
  log_detail("Authenticating SurveyCTO...")
  auth <- get_scto_auth()
  log_step("SurveyCTO authenticated", "SUCCESS")

  # Google Sheets authentication (already done in main script)
  log_step("Google Sheets authenticated", "SUCCESS")

  list(
    db = con,
    scto_auth = auth
  )
}

cleanup_connections <- function(con) {
  log_step("STEP: Cleanup")
  dbDisconnect(con)
  log_step("Database disconnected", "SUCCESS")
}


# ---- Assign Enumerators IDs -------

# assign_enumerators <- function(n, enum_list, last_enum = NULL) {

#   if (length(enum_list) == 0) {
#     stop("Enumerator list is empty")
#   }

#   if (is.null(last_enum) || !(last_enum %in% enum_list)) {
#     start_index <- 1
#   } else {
#     last_index <- match(last_enum, enum_list)
#     start_index <- last_index %% length(enum_list) + 1
#   }
#   ordered <- c(
#     enum_list[start_index:length(enum_list)],
#     enum_list[1:(start_index - 1)]
#   )
#   rep(ordered, length.out = n)
# }

assign_enumerators <- function(n, enumerators, start_index = 1) {

  total <- length(enumerators)

  if (total == 0) {
    stop("Enumerator list is empty")
  }

  indexes <- ((seq_len(n) + start_index - 2) %% total) + 1

  enumerators[indexes]
}

get_last_assigned_enumerator <- function(output_file_url, dataset_id) {

  tryCatch({

    df <- read_sheet(output_file_url, sheet = dataset_id)

    if (nrow(df) == 0 || !"enumerator_email" %in% names(df)) {
      return(1)
    }

    last_enum <- tail(df$enumerator_email, 1)

    return(last_enum)

  }, error = function(e) {
    return(1)
  })
}

get_start_index <- function(last_enum, enumerators) {

  if (is.numeric(last_enum)) {
    return(1)
  }

  idx <- match(last_enum, enumerators)

  if (is.na(idx)) {
    return(1)
  }

  idx + 1
}

# ---- Sync Status ----

write_sync_status <- function(output_file_url, dataset_id, rows_updated) {

  log_step("Writing sync status")
  sync_time <- as.POSIXct(Sys.time(), tz = "Asia/Kolkata", format = "%Y-%m-%d %H:%M:%S")

  status <- data.frame(
    dataset_id = dataset_id,
    rows_updated = as.integer(rows_updated),
    synced_at = sync_time,
    stringsAsFactors = FALSE
  )

  tryCatch({

    log_detail("-> Trying to append to status sheet")
    sheet_append(status, ss = output_file_url, sheet = "status")

    log_step(sprintf(
      "Sync status written: %s (%d rows)",
      dataset_id,
      rows_updated
    ), "SUCCESS")

  }, error = function(e) {

    log_step("Status sheet not found — creating new sheet", "WARNING")

    sheet_write(status, ss = output_file_url, sheet = "status")

    log_step("Status sheet created and first entry written", "SUCCESS")

  })
}


# ---- SQL File Parsing ----

# parse_sql_file <- function(filepath) {
#   log_step("STEP 5-7: Parsing SQL File")

#   # Read file
#   lines <- readLines(filepath, warn = FALSE)
#   log_detail(sprintf("Read %d lines from SQL file", length(lines)))

#   # Find markers
#   pattern <- '-- @dataset_id: '
#   marker_lines <- grep(pattern, lines, fixed = TRUE)
#   log_detail(sprintf("Found %d dataset markers", length(marker_lines)))

#   if (length(marker_lines) == 0) {
#     stop("No dataset markers found in SQL file")
#   }

#   # Extract queries
#   queries <- list()
#   dataset_names <- character(length(marker_lines))

#   for (i in seq_along(marker_lines)) {
#     dataset_names[i] <- trimws(sub(pattern, '', lines[marker_lines[i]], fixed = TRUE))
#     start_line <- marker_lines[i] + 1
#     end_line <- if (i < length(marker_lines)) marker_lines[i + 1] - 1 else length(lines)
#     queries[[i]] <- paste(lines[start_line:end_line], collapse = "\n")
#     log_detail(sprintf("Query %d: %s (%d characters)", i, dataset_names[i], nchar(queries[[i]])))
#   }

#   log_step("All queries extracted", "SUCCESS")

#   list(
#     queries = queries,
#     dataset_names = dataset_names
#   )
# }

parse_sql_file <- function(filepath) {

  log_step("STEP 5-7: Parsing SQL File")

  lines <- readLines(filepath, warn = FALSE)
  log_detail(sprintf("Read %d lines from SQL file", length(lines)))

  dataset_pattern <- "-- @dataset_id: "
  enum_pattern <- "-- @enumerators: "

  dataset_lines <- grep(dataset_pattern, lines, fixed = TRUE)

  log_detail(sprintf("Found %d dataset markers", length(dataset_lines)))

  if (length(dataset_lines) == 0) {
    stop("No dataset markers found in SQL file")
  }

  queries <- list()
  dataset_names <- character()
  enumerator_lists <- list()

  for (i in seq_along(dataset_lines)) {

    start <- dataset_lines[i]
    end <- if (i < length(dataset_lines)) dataset_lines[i + 1] - 1 else length(lines)

    block <- lines[start:end]

    # Dataset ID
    dataset_id <- trimws(sub(dataset_pattern, "", block[1], fixed = TRUE))

    # Enumerator list
    enum_line <- block[grepl(enum_pattern, block)]

    if (length(enum_line) == 0) {
      stop(sprintf("Missing enumerators for dataset: %s", dataset_id))
    }

    enum_string <- trimws(sub(enum_pattern, "", enum_line[1], fixed = TRUE))

    enumerators <- trimws(unlist(strsplit(enum_string, ",")))

    # SQL query
    query_lines <- block[!grepl("^--", block)]
    query <- paste(query_lines, collapse = "\n")

    dataset_names <- c(dataset_names, dataset_id)
    queries[[dataset_id]] <- query
    enumerator_lists[[dataset_id]] <- enumerators

    log_detail(sprintf(
      "Loaded dataset: %s | Enumerators: %d",
      dataset_id,
      length(enumerators)
    ))
  }

  log_step("All queries + enumerators extracted", "SUCCESS")

  list(
    queries = queries,
    dataset_names = dataset_names,
    enumerators = enumerator_lists
  )
}


# ---- Query Execution ----

count_query_results <- function(con, query) {
  count_query <- sprintf("SELECT COUNT(*) as total FROM (%s) subquery", query)

  tryCatch({
    count_result <- dbGetQuery(con, count_query)
    total <- as.integer(count_result$total)
    log_detail(sprintf("Query will return %d total records", total))
    total
  }, error = function(e) {
    log_step(sprintf("ERROR counting records: %s", e$message), "ERROR")
    NA
  })
}

execute_query <- function(con, query, expected_count) {
  log_detail(sprintf("Attempting to fetch %d records...", expected_count))

  tryCatch({
    log_detail("-> Calling dbGetQuery()...")
    result <- dbGetQuery(con, query)
    log_step(sprintf("Successfully fetched %d records", nrow(result)), "SUCCESS")
    log_detail(sprintf("Columns: %s", paste(names(result), collapse = ", ")))
    log_detail(sprintf("Memory size: %.2f MB", object.size(result) / 1024^2))
    result
  }, error = function(e) {
    log_step(sprintf("ERROR executing query: %s", e$message), "ERROR")
    NULL
  })
}

# ---- Data Validation ----

validate_dataframe <- function(df, dataset_id) {
  if (is.null(df) || nrow(df) == 0) {
    log_detail("Query returned 0 records or failed")
    return(FALSE)
  }

  if (!"id" %in% names(df)) {
    log_step("WARNING: No 'id' column found", "WARNING")
    return(FALSE)
  }

  log_detail(sprintf("ID column validated (%d ids)", length(df$id)))
  TRUE
}

# ---- Data Retrieval ----

read_existing_data <- function(auth, dataset_id, output_file_url) {
  log_detail("-> Trying Google Sheets...")

  existing <- tryCatch({
    result <- read_sheet(output_file_url, sheet = dataset_id)
    log_step(sprintf("Found %d existing records in Google Sheets", nrow(result)), "SUCCESS")
    result
  }, error = function(e) {
    log_detail(sprintf("Google Sheets failed: %s", e$message))
    log_detail("-> Trying SurveyCTO fallback...")

    tryCatch({
      result <- scto_read(auth, dataset_id)
      log_step(sprintf("Found %d existing records in SurveyCTO", nrow(result)), "SUCCESS")
      result
    }, error = function(e2) {
      log_detail("Both sources failed, treating as initial load")
      data.frame(id = integer(0))
    })
  })

  if (nrow(existing) > 0) {
    log_detail(sprintf("Existing data columns: %s", paste(names(existing), collapse = ", ")))
  }

  existing
}

# ---- New Records Identification ----

identify_new_records <- function(df_new, existing) {
  if (nrow(existing) == 0) {
    log_step(sprintf("Initial load: all %d records are new", nrow(df_new)), "SUCCESS")
    return(df_new)
  }

  if (!"id" %in% names(existing)) {
    log_step("ERROR: No 'id' column in existing data", "ERROR")
    log_detail(sprintf("Available columns: %s", paste(names(existing), collapse = ", ")))
    log_detail("Treating all records as new")
    return(df_new)
  }

  existing_ids <- as.character(existing$id)
  new_ids <- as.character(df_new$id)

  df_to_upload <- df_new[!(new_ids %in% existing_ids), ]

  log_step(sprintf("Found %d new records (out of %d queried)",
                   nrow(df_to_upload), nrow(df_new)), "SUCCESS")
  log_detail(sprintf("Already existing: %d records", nrow(df_new) - nrow(df_to_upload)))

  df_to_upload
}

# ---- Data Upload ----

upload_to_surveycto <- function(auth, df, dataset_id) {
  log_detail(sprintf("Preparing to upload %d records to %s...", nrow(df), dataset_id))

  tryCatch({
    log_detail("-> Calling scto_write()...")
    scto_write(auth, df, dataset_id, append = TRUE, fill = TRUE)
    log_step(sprintf("Successfully uploaded %d records to SurveyCTO", nrow(df)), "SUCCESS")
    TRUE
  }, error = function(e) {
    log_step(sprintf("ERROR uploading to SurveyCTO: %s", e$message), "ERROR")
    FALSE
  })
}

upload_to_google_sheets <- function(df, existing, output_file_url, dataset_id) {
  log_detail(sprintf("Preparing to append %d records...", nrow(df)))

  tryCatch({
    if (nrow(existing) > 0) {
      # Verify columns match
      existing_cols <- names(existing)
      new_cols <- names(df)
      if (!setequal(existing_cols, new_cols)) {
        log_step("WARNING: Column mismatch detected", "WARNING")
        log_detail(sprintf("Existing: %s", paste(existing_cols, collapse = ", ")))
        log_detail(sprintf("New: %s", paste(new_cols, collapse = ", ")))
      }
      log_detail("-> Calling sheet_append()...")
      sheet_append(data = df, ss = output_file_url, sheet = dataset_id)
    } else {
      log_detail("-> First load, calling sheet_write()...")
      sheet_write(data = df, ss = output_file_url, sheet = dataset_id)
    }
    log_step(sprintf("Successfully wrote %d records to Google Sheets", nrow(df)), "SUCCESS")
    TRUE
  }, error = function(e) {
    log_step(sprintf("ERROR writing to Google Sheets: %s", e$message), "ERROR")
    log_step("WARNING: Data uploaded to SurveyCTO but not synced to Google Sheets!", "WARNING")
    FALSE
  })
}

# ---- Dataset Processing ----

process_single_dataset <- function(con, auth, dataset_id, query, enumerators, params) {
  log_step(sprintf("PROCESSING DATASET: %s", dataset_id), "SECTION")

  # Count results
  cat("\n--- Step A: Counting query results ---\n")
  result_count <- count_query_results(con, query)
  if (is.na(result_count)) {
    log_detail("Skipping this dataset due to count error")
    return(FALSE)
  }

  # Execute query
  cat("\n--- Step B: Executing main query ---\n")
  df_new <- execute_query(con, query, result_count)

  # Validate
  cat("\n--- Step C: Validating ID column ---\n")
  if (!validate_dataframe(df_new, dataset_id)) {
    return(FALSE)
  }

  # Read existing data
  cat("\n--- Step D: Reading existing data ---\n")
  existing <- read_existing_data(auth, dataset_id, params$output_file_url)

  # last_enum <- NULL # Track last assigned enumerator
  # if (nrow(existing) > 0 && "enumerator_email" %in% names(existing)) {
  # last_enum <- tail(existing$enumerator_email, 1)
  # log_detail(sprintf("Last assigned enumerator: %s", last_enum))
  # }

  last_enum <- get_last_assigned_enumerator(
    params$output_file_url,
    dataset_id
  )

  # Identify new records
  cat("\n--- Step E: Identifying new records ---\n")
  df_to_upload <- identify_new_records(df_new, existing)

  if (nrow(df_to_upload) > 0) {
    df_to_upload$enumerator_email <- assign_enumerators(
      n = nrow(df_to_upload),
      enumerators = enumerators,
      start_index = get_start_index(last_enum, enumerators)
    )
  }

  # Upload if there are new records
  if (nrow(df_to_upload) > 0) {
    cat("\n--- Step F: Uploading to SurveyCTO ---\n")
    scto_success <- upload_to_surveycto(auth, df_to_upload, dataset_id)

    if (scto_success) {
      cat("\n--- Step G: Appending to Google Sheets ---\n")
      upload_to_google_sheets(df_to_upload, existing, params$output_file_url, dataset_id)
      # sheets_success <- upload_to_google_sheets(
      #   df_to_upload,
      #   existing,
      #   params$output_file_url,
      #   dataset_id
      # )

      # if (sheets_success) {
      #   cat("\n--- Step H: Writing Sync Status ---\n")
      #   write_sync_status(params$output_file_url, dataset_id)
      # }
    } else {
      cat("\n--- Step G: Skipped (SurveyCTO upload failed) ---\n")
      log_step("Google Sheets NOT updated to prevent data inconsistency", "WARNING")
    }
  } else {
    cat("\n--- No new records to upload ---\n")
  }

  return(nrow(df_to_upload))
}

process_all_datasets <- function(con, auth, queries_data, params) {
  queries <- queries_data$queries
  dataset_names <- queries_data$dataset_names

  for (i in seq_along(queries)) {
    rows_updated <- process_single_dataset(
      con = con,
      auth = auth,
      dataset_id = dataset_names[i],
      query = queries[[dataset_names[i]]],
      enumerators = queries_data$enumerators[[dataset_names[i]]],
      params = params
    )

    cat("dataset processed: dataset_id =", dataset_names[i])
    cat("\n")

    write_sync_status(
      output_file_url = params$output_file_url,
      dataset_id = dataset_names[i],
      rows_updated = rows_updated
    )

    cat("\n")

  }
}
