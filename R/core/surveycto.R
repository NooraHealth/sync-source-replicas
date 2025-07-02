
# Dependencies ------------------------------------------------------------
library(rsurveycto)
library(logger)
library(dplyr)

# Logging config ----------------------------------------------------------
logger::log_threshold(level = DEBUG)

# Secrets -----------------------------------------------------------------
SURVEYCTO_SECRETS_FILE_PATH='secrets/scto_auth.txt'
FORM_DEFINITION_DIRECTORY='form_definition_repo/'

# Relevant Methods --------------------------------------------------------
noora_scto_auth_key <-
  # Function to access the authorization key for rsurveyCTO.
  function(){
    if (file.exists(SURVEYCTO_SECRETS_FILE_PATH)) {
      tryCatch(
        expr = {auth <- rsurveycto::scto_auth(auth_file = SURVEYCTO_SECRETS_FILE_PATH)},
        error = function(e){log_fatal(conditionMessage(e))}
      )
      log_info('SurvyCTO authorization successful.')
      return(auth)
    } else {
      log_fatal('SurveyCTO authorization failed.')
      return(NULL)
    }
  }


noora_scto_catalog <-
  # Function to access global catalog stored in surveycto server
  function(){
    authorization <-  noora_scto_auth_key()
    if(is.null(authorization)) {
      log_error('Unable to extract SCTO catalog')
      break
    } else {
      catalog = rsurveycto::scto_catalog(authorization)
      log_info('SurveyCTO catalog successfully saved.')
      return(catalog)
    }
  }

noora_scto_form_definition_download <-
  # Downloads all form definitions in their raw format and stores it in
  # a designated folder. Also returns a data.table with some basic information of the form  definition downloaded.
  function(form_id=NULL){
    auth=noora_scto_auth_key()
    if(!dir.exists(FORM_DEFINITION_DIRECTORY)){
      log_info('Folder does not exist. Creating...')
      dir.create(FORM_DEFINITION_DIRECTORY)
    } else {
      log_info('Folder exists. Deleting all existing contents...')
      unlink(file.path(FORM_DEFINITION_DIRECTORY, "*"), recursive = TRUE)
      log_info('Contents cleared!')
    }
    if(is.null(auth)) {
      log_error('Unable to access metadata information, due to invalid auth')
      return(NULL)
    } else {
      raw_metadata=rsurveycto::scto_get_form_metadata(auth = auth,
                                                  form_ids = form_id,
                                                  deployed_only = TRUE,
                                                  def_dir = FORM_DEFINITION_DIRECTORY,
                                                  get_defs = TRUE)
      cleaned_metadata=raw_metadata |>
        dplyr::select(form_id,form_version,date_str,
                      actor) |>
        dplyr::rename(c('form_deployed_date'='date_str',
                        'deployed_by'='actor')) |>
        dplyr::mutate(downloaded_file_name=
                        stringr::str_c(form_id,'__',form_version,'.xlsx')
                      )
      return(cleaned_metadata)
    }
  }

noora_scto_update <-
  function(form_id=NULL){
    catalog=noora_scto_catalog() |>
      filter(is_deployed==TRUE & type=="form") |>
      select(group_title,title,id,form_version,num_submissions_complete) |>
      distinct() |> rename(c('form_id'='id'))
    main=noora_scto_form_definition_download(form_id) |>
      inner_join(catalog,by = c('form_id','form_version')) |>
      select(group_title,title,form_id,
             form_version,num_submissions_complete,
             form_deployed_date,deployed_by,
             downloaded_file_name)
    return(main)
  }

