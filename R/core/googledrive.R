
# Dependencies ------------------------------------------------------------
library(googledrive)
source('./R/core/surveycto.R')
source('./R/core/helper.R')

SERVICE_ACCOUNT_PATH='secrets/service_account_creds.json'
GDRIVE_FOLDER_ID='1khmWpbSCnFawvIq8Yg7Xus3QbDDJdL5D'
CONTROL_PANEL_FILE_NAME='SurveyCTO Control Panel'
GDRIVE_FORM_DEFINITION_FOLDER='form definition'
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

noora_gdrive_validate_folder_id <-
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

noora_gdrive_check_if_folder_is_empty <-
  function(id=GDRIVE_FOLDER_ID){
    noora_google_auth()
    on.exit(noora_google_auth(FALSE))
    folder_contents= drive_ls(as_id(id))
    if (nrow(folder_contents)==0){
      warning('Folder is empty! Is this your first runtime?')
      return(TRUE)
    } else {
      message('Folder has some contents. Checking...')
      return(FALSE)
    }
  }


noora_gdrive_check_folder_structure <-
  function(id=GDRIVE_FOLDER_ID){
    if(noora_gdrive_check_if_folder_is_empty()==TRUE){
      message('This folder lacks character. Lets change that')
      return(FALSE)
    } else {
      if(GDRIVE_FORM_DEFINITION_FOLDER %in% names(noora_gdrive_subfolder_meta())){
        message(paste0(GDRIVE_FORM_DEFINITION_FOLDER,' exists!'))
      } else {
        warning(paste0(GDRIVE_FORM_DEFINITION_FOLDER, ' does not exist'))
        return(FALSE)
      }
      if(CONTROL_PANEL_FILE_NAME %in% names(noora_gdrive_subfolder_meta())){
        message(paste0(CONTROL_PANEL_FILE_NAME, ' exists!'))
        control_panel_id=noora_control_panel_find_control_panel_id()
        control_panel_raw=read_sheet(control_panel_id,sheet = 'info')
        if(nrow(control_panel_raw)==0){
          message(paste0(CONTROL_PANEL_FILE_NAME, ' does not exist!'))
          return(FALSE)
        } else {
          return(TRUE)
        }
      } else {
        warning(paste0(CONTROL_PANEL_FILE_NAME, ' does not exist!'))
        return(FALSE)
      }
    }
  }

noora_gdrive_folder_structure_init <-
  function(id=GDRIVE_FOLDER_ID){
    on.exit(noora_google_auth(authorize = FALSE))
    # Get folder structure ready
    drive_mkdir(names = GDRIVE_FORM_DEFINITION_FOLDER,
                path = as_id(GDRIVE_FOLDER_ID))
    log_info('Folders created successfully!')
    # Create Control Panel Spreadsheet
    drive_create(name = "SurveyCTO Control Panel",
                 path = as_id(GDRIVE_FOLDER_ID),
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

