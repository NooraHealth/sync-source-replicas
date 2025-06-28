
# Dependencies ------------------------------------------------------------
library(dplyr)


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


