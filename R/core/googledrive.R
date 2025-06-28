
# Dependencies ------------------------------------------------------------
library(googledrive)
source('surveycto.R')
source('helper.R')

SERVICE_ACCOUNT_PATH='secrets/service_account_creds.json'
GDRIVE_FOLDER_ID='1khmWpbSCnFawvIq8Yg7Xus3QbDDJdL5D'
# Authorization -----------------------------------------------------------
noora_google_auth <-
  function(authorize=TRUE){
    if(!file.exists(SERVICE_ACCOUNT_PATH)){
      log_error('Service account credentials not found. Exiting...')
      return(NULL)
    } else {
        NULL
    }
    if(authorize==TRUE){
      googledrive::drive_auth(path = SERVICE_ACCOUNT_PATH)
      log_info('Googledrive API successfully authorized!')
    } else {
      googledrive::drive_deauth()
      log_info('Googledrive API successfully deauthorized')
    }
  }

noora_gdrive_folder_validate <-
  function(id=GDRIVE_FOLDER_ID){
    on.exit(noora_google_auth(FALSE))

    noora_google_auth(TRUE)

    tryCatch({
      folder_info <- drive_get(as_id(id))
      return(nrow(folder_info) > 0)
    }, error = function(e) {
      log_fatal('Non-valid folder details provided. Please verify. Error: ', e$message)
      return(FALSE)
    })
  }

noora_gdrive_folder_init <-
  function(id=GDRIVE_FOLDER_ID){
    on.exit(noora_google_auth(authorize = FALSE))
    folder_id=as_id(id)
    folders_to_be_created=c('form definition')
    is_valid_folder=noora_gdrive_folder_validate(id)
    if(is_valid_folder==TRUE){
      NULL
    } else {
      log_error('Cannot initialize folder due to non-valid folder id')
      return(NULL)
    }
    # Check if folder is empty or not
    noora_google_auth(authorize = TRUE)
    if(nrow(drive_ls(as_id(GDRIVE_FOLDER_ID)))==0){
      log_info('Folder tagged is empty. Primed for init')
      NULL
    } else {
      log_error('Folder not empty! Unable to initialize!')
      return(NULL)
    }
    # Get folder structure ready
    for (folder in folders_to_be_created) {
      drive_mkdir(names = folder,path = folder_id)
    }
    log_info('Folders created successfully!')
    # Create Control Panel Spreadsheet
    drive_create(name = "SurveyCTO Control Panel",
                path = folder_id,
                type = "spreadsheet")
    log_info('Control Panel Successfully Created!')
  }

noora_gdrive_subfolder_meta <-
  function(id=GDRIVE_FOLDER_ID){
    on.exit(noora_google_auth(FALSE))
    noora_google_auth(TRUE)
    folder_info=drive_ls(path = as_id(id))
    meta=as.list(folder_info$id)
    names(meta)=folder_info$name
    return(meta)
  }

noora_gdrive_raw_upload <-
  function(){
    on.exit(noora_google_auth(FALSE))
    gdrive_path=noora_gdrive_subfolder_meta()[['form definition']]
    # Downloaded files, all mapped and ready to go
    form_definitions_downloaded=noora_helper_form_definition_local_path()
    noora_google_auth()
    # Store the upload information
    upload_log=data.frame(form_id=character(),
                          google_file_id=googledrive::as_id())
    for (form in names(form_definitions_downloaded)) {
      upload_info=drive_put(media = paste0(FORM_DEFINITION_DIRECTORY,
                                              form_definitions_downloaded[form]),
                   name = form,
                   path = gdrive_path,
                   type = "spreadsheet")
      upload_log=rbind(upload_log,data.frame(form_id=form,
                                             google_file_id=as_id(upload_info$id)))
    }
    return(upload_log)
  }

