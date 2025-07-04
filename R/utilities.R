library('cli')
library('data.table')
# library('doParallel')
library('foreach')
library('glue')
library('googledrive')
library('googlesheets4')
library('here')
library('progress')
library('rsurveycto')


get_params = \(path) {
  params_raw = yaml::read_yaml(path)
  envir = if (Sys.getenv('GITHUB_REF_NAME') == 'main') 'prod' else 'dev'
  envirs = sapply(params_raw$environments, \(x) x$name)
  params = c(
    params_raw[names(params_raw) != 'environments'],
    params_raw$environments[[which(envirs == envir)]])
  names(params)[names(params) == 'name'] = 'environment'
  params
}


get_scto_auth = \(auth_file = 'scto_auth.txt') {
  if (Sys.getenv('SCTO_AUTH') == '') {
    auth_path = here('secrets', auth_file)
  } else {
    auth_path = withr::local_tempfile()
    writeLines(Sys.getenv('SCTO_AUTH'), auth_path)
  }
  scto_auth(auth_path)
}


set_google_auth = \(auth_file = 'google-token.json', type = c('drive', 'gs4')) {
  type = match.arg(type, several.ok = TRUE)
  token_env = Sys.getenv('GOOGLE_TOKEN')
  token_path = here('secrets', auth_file)

  path = if (token_env != '') {
    token_env
  } else if (file.exists(token_path)) {
    token_path
  } else {
    NULL
  }

  if ('drive' %in% type) drive_auth(path = path)
  if ('gs4' %in% type) gs4_auth(path = path)
}
