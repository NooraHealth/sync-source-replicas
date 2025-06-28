# Dependencies ------------------------------------------------------------
source('R/core/surveycto.R')
source('R/core/googledrive.R')


# Initialization ----------------------------------------------------------

noora_init <-
  function(){
    # Initialize Google Drive
    noora_gdrive_folder_init()
    # Store the meta information of the folder structure created
    gdrive_meta=noora_gdrive_subfolder_meta()
    # Download all the form definitions locally
    download_info=noora_scto_update()
    # Start Form Upload to gdrive
    upload_info=noora_gdrive_raw_upload()
    # Generate the control panel info
    control_panel_info <-
      download_info |>
      left_join(upload_info,by = 'form_id') |>
      filter(!is.na(google_file_id)) |>
      mutate(link_to_form_definition=drive_link(google_file_id))
    # Write to control panel sheet
    googlesheets4::write_sheet(
      data = control_panel_output,
      ss = gdrive_meta[["SurveyCTO Control Panel"]],
      sheet = "info"
    )
  log_info('Project initialization Complete!')
  }

