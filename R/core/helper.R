
# Dependencies ------------------------------------------------------------
library(dplyr)
source("./R/core/surveycto.R")


# Folder Interactivity ----------------------------------------------------
noora_helper_form_definition_local_path <-
  function(directory = FORM_DEFINITION_DIRECTORY) {
    files <- list.files(directory, full.names = FALSE)
    names <- sapply(basename(files), function(x) {
      parts <- strsplit(x, "__")[[1]]
      paste(parts[-length(parts)], collapse = "__")
    })
    setNames(files, names)
  }


# Function for code to figure out what to do ------------------------------

noora_helper_list_of_form_def_scto <-
  function(){
    info=noora_scto_catalog() |>
      filter(type=="form" & is_deployed==TRUE) |>
      group_by(id) |>
      mutate(upgraded=n()>1 &
               last_version_created_at==max(last_version_created_at)) |>
      filter(last_version_created_at==max(last_version_created_at)) |>
      ungroup() |>
      select(id,form_version,last_version_created_at,upgraded) |>
      distinct() |>
      rename(c('form_id'='id','scto_form_version'='form_version'))
    return(info)
  }

noora_helper_where_is_control_panel <-
  function(){
    folder_meta_info=noora_gdrive_subfolder_meta()[['SurveyCTO Control Panel']]
    return(folder_meta_info)
  }

noora_helper_list_of_form_def_control_panel <-
  function(){
    control_panel_raw <-
      googlesheets4::read_sheet(ss = noora_helper_where_is_control_panel(),
                                sheet = 'info') |>
      rename('gdrive_form_version'='form_version') |>
      # Temporary PLEASE DONT FORGET TO DELETE
      mutate(has_been_upgraded=FALSE)
    return(control_panel_raw)
  }

noora_helper_decision <-
  function(){
    scto_form_def=noora_helper_list_of_form_def_scto()
    control_panel_form_def=noora_helper_list_of_form_def_control_panel()
    decision_high_jinks=scto_form_def |>
      select(form_id,scto_form_version,upgraded) |>
      left_join((control_panel_form_def |>
                   select(form_id,gdrive_form_version,
                          has_been_upgraded,google_file_id)),
                by=c('form_id')) |>
      mutate(to_be_inserted=
               if_else(is.na(gdrive_form_version)==TRUE,TRUE,FALSE)) |>
      mutate(to_be_updated=if_else(gdrive_form_version==scto_form_version,FALSE,TRUE)) |>
      mutate(to_be_upgraded=if_else(
        has_been_upgraded==FALSE &
          upgraded==TRUE &
          gdrive_form_version!=scto_form_version,TRUE,FALSE)) |>
      select(form_id,scto_form_version,google_file_id,
             to_be_inserted,to_be_updated,to_be_upgraded)
      return(decision_high_jinks)
  }


# Form Definition log generator -------------------------------------------
noora_helper_form_def_log <-
  function(google_file_id, form_version = NULL,
           form_deployed_date = NULL,
           deployed_by = NULL, status = NULL) {

  sheet_info <- googlesheets4::sheet_properties(google_file_id)
  list_of_sheets <- sheet_info$name

  # Check if the sheet exists
  log_exists <- "log" %in% list_of_sheets
  if (!log_exists) {
    # Create the "log" sheet if it doesn't exist
    message("'log' sheet not found. Creating new 'log' sheet...")
    sheet_add(google_file_id, sheet = "log")  # Use google_file_id, not sheet_info
    log_is_empty <- TRUE
  } else {
    # Check if the "log" sheet is empty
    message("'log' sheet found. Checking if it's empty...")  # Changed from log_info()
    # Try to read the sheet to see if it has data
    tryCatch({
      log_data <- range_read(google_file_id, sheet = "log", range = "A1:E1")  # Use google_file_id
      log_is_empty <- nrow(log_data) == 0 || all(is.na(log_data))
    }, error = function(e) {
      # If there's an error reading (e.g., completely empty), assume it's empty
      log_is_empty <- TRUE
    })
  }

  # If the log sheet is empty, add the headers
  if (log_is_empty) {
    message("'log' sheet is empty. Adding column headers...")
    # Define the column headers
    headers <- data.frame(
      form_version = character(0),
      form_deployed_date = character(0),
      deployed_by = character(0),
      status = character(0)
    )
    # Write headers to the sheet
    range_write(
      data = headers,
      ss = google_file_id,
      sheet = "log",
      range = "A1",
      col_names = TRUE,
      reformat = FALSE
    )

    message("Headers successfully added to 'log' sheet!")
  } else {
    message("'log' sheet already has data.")
  }

  # If new data is provided, append it to the sheet
  if (!is.null(form_version) || !is.null(form_deployed_date) ||
      !is.null(deployed_by) || !is.null(status)) {

    message("Appending new data to 'log' sheet...")

    # Create new row of data
    new_row <- data.frame(
      form_version = ifelse(is.null(form_version), "", form_version),
      form_deployed_date = ifelse(is.null(form_deployed_date), "", form_deployed_date),
      deployed_by = ifelse(is.null(deployed_by), "", deployed_by),
      status = ifelse(is.null(status), "", status)
    )

    # Append the new row to the sheet
    sheet_append(
      data = new_row,
      ss = google_file_id,  # Use google_file_id, not sheet_info
      sheet = "log"
    )

    message("New data successfully appended to 'log' sheet!")
    return("Data appended")
  }

  return("Sheet checked/created - no new data to append")
}


