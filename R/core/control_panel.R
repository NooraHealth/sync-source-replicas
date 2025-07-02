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

noora_control_panel_update <-
  function(init=FALSE,data_to_be_inserted=NULL){
    on.exit(noora_google_auth(FALSE))
    safe_drive_link=purrr::possibly(drive_link,otherwise = NA_character_)
    noora_google_auth()
    if(init==FALSE){
      if(!is.null(data_to_be_inserted)){
        control_panel_raw=googlesheets4::read_sheet(ss = noora_helper_where_is_control_panel(),
                                  sheet = 'info') |>
          select(-c(status))
        unaffected_forms=control_panel_raw |>
          anti_join(data_to_be_inserted,by = 'form_id') |>
          mutate(status='NO CHANGE')
        noora_google_auth()
        affected_forms=data_to_be_inserted |>
          mutate(link_to_form_definition=purrr::map_chr(google_file_id,safe_drive_link)) |>
          mutate(has_been_upgraded=if_else(status=="UPGRADE","YES","NO"))
        print(affected_forms)
        final_output=affected_forms |>
          add_row(unaffected_forms)
        googlesheets4::write_sheet(data = final_output,
                                   ss = noora_helper_where_is_control_panel(),
                                   sheet = 'info')
        return(TRUE)
      } else {
          final_output=read_sheet(ss = noora_helper_where_is_control_panel(),
                                  sheet = 'info') |>
            mutate(status="NO CHANGE")
          googlesheets4::write_sheet(data=final_output,
                                     ss = noora_helper_where_is_control_panel(),
                                     sheet = 'info')
          return(TRUE)
      }
    } else {
      googlesheets4::write_sheet(data = data_to_be_inserted,
                                 ss=noora_helper_where_is_control_panel(),
                                 sheet = 'info')
      return(TRUE)
    }

  }

