# Dependencies ------------------------------------------------------------
source('./R/core/helper.R')
source('./R/core/googledrive.R')
source('./R/core/control_panel.R')

# Business as usual -------------------------------------------------------

runtime_process <-
  function(){
    # Check if the folder is valid or not
    if(noora_gdrive_validate_folder_id()==FALSE){
      stop('Exiting process due to invalid folder id')
    }
    # Check if the folder structure is valid
    valid_folder_structure=noora_gdrive_check_folder_structure()
    if(valid_folder_structure==TRUE){
      # Find out what we need to do
      decision_to_be_made=noora_helper_decision() |>
        filter(to_be_updated==TRUE | to_be_upgraded==TRUE |
                 to_be_inserted==TRUE) |>
        # There's a weird form called Untitled_20230621071753 that has no version
        filter(scto_form_version!="") |>
        mutate(status=case_when(to_be_inserted==TRUE ~ 'INSERT',
                                to_be_updated==TRUE ~ 'UPDATE',
                                to_be_upgraded==TRUE ~ "UPGRADE"))
      # List of forms to be downloaded
      list_of_forms_tbd=decision_to_be_made |> pull(,var = 'form_id')
      # If there is no forms to edit
      if(nrow(decision_to_be_made)==0){
        message('No forms to be updated.')
        noora_control_panel_update(init = FALSE,data_to_be_inserted = NULL)
        message('Control Panel reset!')
        return(NULL)
      }
      # Download these forms
      download_info=noora_scto_update(list_of_forms_tbd)
      # Upload these forms
      upload_info=noora_gdrive_raw_upload()
      # Snapshot of information on runtime
      process_completion_details=download_info |>
        select(-c(downloaded_file_name)) |>
        left_join(upload_info,by = 'form_id') |>
        left_join((decision_to_be_made |> select(form_id,status)),by = 'form_id')
      # Use the process completion details to upload the log
      ## Temporary solution need to change
      ### Note: The log details get reset as the form definition sheet gets reset entirely. Need to fix.
      noora_helper_form_def_log_mod <- purrr::possibly(noora_helper_form_def_log)
      process_completion_details |>
        select('google_file_id','form_version',
               'form_deployed_date','deployed_by',
               'status') |>
        purrr::pwalk(noora_helper_form_def_log_mod)
      # Control Panel
      noora_control_panel_update(data_to_be_inserted = process_completion_details)
      message('Update successful!')
    } else {
      return(NULL)
    }
  }