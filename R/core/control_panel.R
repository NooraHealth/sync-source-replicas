# Dependencies ------------------------------------------------------------
library(dplyr)
library(googlesheets4)
source('googledrive.R')
source('surveycto.R')


# Methods -----------------------------------------------------------------
noora_control_panel_find_control_panel_id <-
    function() {
      gdrive_file_info=noora_gdrive_subfolder_meta()
      return(gdrive_file_info$`SurveyCTO Control Panel`)
    }

noora_gsheet_auth <-
  function(authorize=TRUE){
    if(!file.exists(SERVICE_ACCOUNT_PATH)){
      log_error('Service account credentials not found. Exiting...')
      return(NULL)
    } else {
      NULL
    }
    if(authorize==TRUE){
      googlesheets4::gs4_auth(path = SERVICE_ACCOUNT_PATH)
      log_info('Googlesheet API successfully authorized!')
    } else {
      googlesheets4::gs4_deauth()
      log_info('Googlesheet API successfully deauthorized')
    }
  }

