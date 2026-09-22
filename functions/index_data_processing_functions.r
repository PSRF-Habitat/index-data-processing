########## PROCESSING INDEX SITE LOGGER DATA ##########

# List all .csv files from a Google Drive folder by ID ----
#'
#' @param root_folder_id The ID of the folder containing sensor subfolders (e.g., "Temperature", "pH")
#' @return A data frame of all .csv files found, with an added `sensor_type` column inhereted from subfolder name
#'
get_all_logger_csvs_by_id <- function(root_folder_id) {
  # Create a list to store drive's metadata for all .csv files found
  all_files <- list()
  
  # This function will look into each folder (Temperature, PH, etc.) and collect csvs
  crawl_folder <- function(folder_id) {
    # List all items in the current folder
    items <- drive_ls(as_id(folder_id))
    
    # Only keep subfolders in csvfiles folder (eg, Temperature, Conductivity, PH...)
    # mimeType == "application/vnd.google-apps.folder" is how Google marks folders in its API
    subfolders <- items |>
      filter(drive_resource[[1]]$mimeType == "application/vnd.google-apps.folder") |>
      # Do not grab Fancy Logger files
      filter(name != "Fancy Logger")
    
    # Reach (recurse) into each subfolder and collect csvs
    if (nrow(subfolders) > 0) {   # Proceed if we found subfolders
      # Loop through each subfolder
      for (i in seq_len(nrow(subfolders))) {
        # Grab subfolder name and ID
        subfolder_name <- subfolders$name[i]
        subfolder_id   <- subfolders$id[i]
        
        # List items in current subfolder
        sub_items <- drive_ls(as_id(subfolder_id))
        
        # Get .csv files in subfolder
        csv_files <- sub_items |>
          filter(str_detect(tolower(name), "\\.csv$")) |>
          # Tag each file with sensor type, using folder name
          mutate(sensor_type = subfolder_name)
        
        # Save to list
        all_files <<- append(all_files, list(csv_files))
      }
    }
  }
  
  # Start crawling from the root folder
  crawl_folder(root_folder_id)
  
  # Combine all file results into a single data frame
  return(bind_rows(all_files))
} # END get_all_logger_csvs_by_id function ----



# Read and clean index logger metadata ----
#' @param metadata_file_url URL to main index metadata google sheet
#' @param sheet_name Name of the tab that the metadata is on
#'
#' @returns A clean metadata sheet to be combined with logger data
#'
read_and_clean_metadata <- function(metadata_file_url, sheet_name) {
  
  # Read in metadata sheet from google drive
  metadata_raw <- read_sheet(metadata_file_url,
                             sheet = sheet_name,
                             na = c("", "n/a", "#N/A"),
                             col_types = "c")
  
  metadata <- metadata_raw |>
    clean_names() |>
    
    # Rename nickname to logger_id to match logger file convention
    rename(logger_id = nickname) |>
    
    # Make position and logger_type consistently lowercase
    mutate(position = tolower(position),
           logger_type = tolower(logger_type)) |>
    
    # Parse date columns
    mutate(across(c(launch_date_office, in_water_date,
                    out_of_water_date, data_readout_date),
                  ~ suppressWarnings(as.Date(parse_date_time(.x, orders = c("ymd", "mdy")))))) |>
    
    select(site, position, logger_id, serial_number, deployment_type,
           launch_date_office, in_water_date, in_water_time,
           out_of_water_date, out_of_water_time, data_readout_date,
           issue_flag, comments, logger_type,
           file_name) |>
    arrange(site, position, in_water_date, logger_id)
  
  return(metadata)
} # END metadata cleaning function ----


#' Pre-Run Check: Identify logger files with NO match in metadata ----
#'
#' Run this before updating the main logger file to catch all CSVs in the Drive that
#' will not be processed successfully due to missing metadata matches
#' 
#' Use this list to check for errors: Site name, logger ID, and dates must match perfectly in
#' file name and metadata
#' 
#' This run will also check for missing metadata entries. Missing information in the metadata
#' could cause the pipeline to silently fail
#' 
#' @param root_folder_id The ID of the folder containing sensor subfolders (e.g., "Temperature", "pH")
#' @param metadata_file_url URL to main index metadata google sheet
#' @param sheet_name Name of the tab that the metadata is on
#' 
#' @returns A data frame of unmatched files
#'
prerun_check <- function(root_folder_id, metadata_file_url, sheet_name) {
  
  cli::cli_h1("Pre-Run Check")
  
  # Get all files from Drive
  cli::cli_h2("Scanning Google Drive")
  all_files <- get_all_logger_csvs_by_id(root_folder_id)
  cli::cli_alert_success("Found {nrow(all_files)} total file{?s} in Drive")
  
  # Load metadata
  cli::cli_h2("Loading metadata")
  metadata <- read_and_clean_metadata(metadata_file_url, sheet_name)
  
  # Check for invalid deployment dates
  cli::cli_h2("Checking for invalid metadata dates")
  invalid_dates <- metadata |>
    filter(is.na(in_water_date) | is.na(out_of_water_date))
  
  if (nrow(invalid_dates) > 0) {
    cli::cli_alert_danger("{nrow(invalid_dates)} metadata row{?s} have invalid (missing or unparseable) in_water_date or out_of_water_date. Rows with invalid dates will not be matched to potential associated data files.")
  } else {
    cli::cli_alert_success("All metadata rows have valid deployment dates.")
  }
  
  # Check for duplicate site/position/in_water_date/logger_type rows
  cli::cli_h2("Checking for duplicate metadata rows")
  duplicate_rows <- metadata |>
    filter(!is.na(in_water_date)) |>
    count(site, position, in_water_date, logger_type) |>
    filter(n > 1)
  
  if (nrow(duplicate_rows) == 0) {
    cli::cli_alert_success("No duplicate metadata rows. Good to go!")
  } else {
    cli::cli_alert_danger("{nrow(duplicate_rows)} duplicate row{?s} in metadata. Duplicate rows will cause pipeline to fail. Handle duplicates before proceeding.")
  }
  
  
  # Parse file name components for all files (same logic as read_and_clean_logger_csv)
  cli::cli_h2("Checking that all .csv files have a match in the metadata")
  file_info <- all_files |>
    mutate(
      parts = str_split(name, "_", simplify = TRUE),
      site_name = str_replace_all(parts[, 1], "(?<=[a-z])(?=[A-Z])", " "),
      file_logger_id = parts[, 2],
      deployment_date = as.Date(str_remove_all(parts[, 3], regex("\\.csv", ignore_case = TRUE)))
    ) |>
    select(-parts)
  
  # Attempt to match each file to metadata
  unmatched <- file_info |>
    rowwise() |>
    mutate(
      n_matches = nrow(
        metadata |>
          filter(site == site_name,
                 logger_id == file_logger_id,
                 as.Date(in_water_date) == deployment_date)
      ),
      match_status = case_when(
        n_matches == 0 ~ "no match",
        n_matches > 1  ~ "multiple matches",
        TRUE ~ "ok"
      )
    ) |>
    ungroup() |>
    filter(match_status != "ok")  |>
    select(match_status, name, sensor_type, site_name, file_logger_id, deployment_date)
  
  # Report results
  if (nrow(unmatched) == 0) {
    cli::cli_alert_success("All files have exactly one metadata match. Good to go!")
  } else {
    cli::cli_alert_danger("{nrow(unmatched)} file{?s} will not be processed:")
    cli::cli_bullets(c(
      "*" = "no match:          {sum(unmatched$match_status == 'no match')}",
      "*" = "multiple matches:  {sum(unmatched$match_status == 'multiple matches')}"
    ))
    
    cli::cli_alert_info("Fix these in the metadata sheet before running the pipeline.")
  }
  
  cli::cli_h1("Pre-run check complete")
  
  return(list(
    invalid_dates = invalid_dates,
    duplicate_rows = duplicate_rows,
    unmatched = unmatched
  ))
  
} # END prerun check function ----

# Read and clean a logger CSV file from Google Drive ----
#'
#' @param file_row One row from get_all_logger_csvs_by_id() output
#' @param metadata Metadata dataframe from read_and_clean_metadata()
#' @param verbose Logical, whether to print detailed processing information (default FALSE)
#' @return A cleaned logger CSV for one file
read_and_clean_logger_csv <- function(file_row, metadata, verbose = FALSE) {
  
  # Pull out file info
  file_id <- file_row$id      # File ID for Google Drive API
  file_name <- file_row$name  # File name
  sensor_type <- str_trim(tolower(file_row$sensor_type))  # Sensor type pulled from folder name, to know how to proceed processing
  
  # Grab info from file name
  parts <- str_split(file_name, "_", simplify = TRUE)
  # Part 1 is site name. Make Title Case
  site_name <- str_replace_all(parts[1], "(?<=[a-z])(?=[A-Z])", " ")
  # Part 2 is logger_id
  file_logger_id <- parts[2]
  # Part 3 is date deployed or relaunched
  deployment_date <- as.Date(str_remove_all(parts[3], 
                                            regex("\\.csv", 
                                                  ignore_case = TRUE)))
  
  if (verbose) {
    cli_alert_info(paste("Site:", site_name,
                         " | Logger ID:", file_logger_id,
                         " | Date deployed:", deployment_date))
  }
  
  
  # Join logger file to metadata row
  meta_row <- metadata |>
    filter(site == site_name,
           logger_id == file_logger_id,
           as.Date(in_water_date) == deployment_date)
  
  # Warning if no match in metadata
  if (nrow(meta_row) == 0) {
    cli_alert_warning(paste("No metadata match for file:", file_name,
                            "- check site name, logger_id, and deployment date in metadata sheet"))
    return(NULL)
  }
  
  # Warning if multiple matches
  if (nrow(meta_row) > 1) {
    cli_alert_warning(paste("Multiple metadata matches for:", file_name, "- using first row. Check file name and metadata sheet"))
    meta_row <- meta_row[1, ]
  }
  
  # Pull out values from matched metadata row
  position <- meta_row$position
  deployment_type <- meta_row$deployment_type
  in_water_date <- meta_row$in_water_date
  out_of_water_date <- meta_row$out_of_water_date
  issue_flag <- meta_row$issue_flag
  comments <- meta_row$comments
  
  if (verbose) {
    cli_alert_info(paste("Position:", position, "| Type:", deployment_type,
                         "| In:", in_water_date, "| Out:", out_of_water_date))
    cli_alert_info(paste("Issue flag:", issue_flag, "| Comments:", comments))
    cli_alert_info(paste("Sensor type:", sensor_type))
  }
  
  # Download csv to temp file
  temp_path <- tempfile(fileext = ".csv")
  if (length(file_id) != 1) {     # Error if there are duplicate files
    cli_abort("drive_download() aborted: file_id is not unique.")
  }
  drive_download(as_id(file_id), path = temp_path, overwrite = TRUE)
  
  if (verbose) {
    cli_alert_info(paste("File downloaded to:", temp_path))
  }
  
  ##### TEMP LOGGER #####
  if (sensor_type == "temperature") {
    
    # Read the first few lines and find the header row
    lines <- readLines(temp_path, n = 5)
    header_line <- which(
      sapply(lines, function(line) {
        clean_line <- str_remove(line, "^#\\s*")
        lower_line <- tolower(clean_line)
        str_detect(lower_line, "date") && str_detect(lower_line, "temp")
      })
    )[1]
    
    # Fallback in case no header line is found
    if (is.na(header_line)) {
      cli_alert_warning(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    
    if (verbose) {
      cli_alert_info(paste("Skipping", skip_n, "lines"))
    }
    
    # Read in the csv
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
    
    # Check that the header row was correctly read in
    if (verbose) {
      cli_alert_info("Columns detected:")
      cli_alert_info(paste(names(df), collapse = ", "))
    }
    
    # Only keep the rows with actual data
    df <- df |>
      select(contains("date", ignore.case = TRUE),
             contains("temp", ignore.case = TRUE))
    # Check that the columns kept were correct
    if (verbose) {
      cli_alert_info("Columns selected:")
      cli_alert_info(paste(names(df), collapse = ", "))
    }
    
    df <- df |>
      # Rename selected columns
      setNames(c("datetime", "tidbit_temp_c")) |>
      mutate(site = site_name, 
             position = position, 
             logger_id = file_logger_id,
             logger_type = "temperature",
             .before = datetime) |>
      # Make datetime column POSIXct class
      mutate(datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
      filter(!is.na(tidbit_temp_c)) |>
      distinct()
    
    # Set NA for full day when logger was deployed and relaunched
    df <- df |>
      mutate(
        date = as.Date(datetime),
        tidbit_temp_c = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ tidbit_temp_c)
      ) |>
      select(-date)
    
    if (verbose) {
      cli_alert_info(paste("Rows after filtering:", nrow(df)))
      cli_alert_success(paste("Finished processing", file_name))
    }
  }
  
  ##### PH LOGGER #####
  else if (sensor_type == "ph") {
    
    lines <- readLines(temp_path, n = 5)
    header_line <- which(
      sapply(lines, function(line) {
        clean_line <- str_remove(line, "^#\\s*")
        lower_line <- tolower(clean_line)
        str_detect(lower_line, "date") && str_detect(lower_line, "mv")
      })
    )[1]
    
    if (is.na(header_line)) {
      cli_alert_warning(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    
    if (verbose) {
      cli_alert_info(paste("Skipping", skip_n, "lines"))
    }
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
    
    if (verbose) {
      cli_alert_info("Columns detected:")
      cli_alert_info(paste(names(df), collapse = ", "))
    }
    
    ph_matches <- names(df)[str_detect(names(df), regex("ph", ignore_case = TRUE)) &
                              !str_detect(names(df), regex("calibrat", ignore_case = TRUE))]
    
    ph_col <- if (length(ph_matches) > 0) ph_matches[1] else NA_character_
    has_ph_col <- !is.na(ph_col)
    
    if (has_ph_col) {
      selected_cols <- df |>
        select(contains("date", ignore.case = TRUE),
               contains("temp", ignore.case = TRUE),
               contains("mv", ignore.case = TRUE),
               all_of(ph_col))
    } else {
      selected_cols <- df |>
        select(contains("date", ignore.case = TRUE),
               contains("temp", ignore.case = TRUE),
               contains("mv", ignore.case = TRUE))
    }
    
    if (verbose) {
      cli_alert_info("Columns selected:")
      cli_alert_info(paste(names(selected_cols), collapse = ", "))
    }
    
    df <- selected_cols |>
      setNames(c("datetime", "ph_temp_c", "millivolts", if (has_ph_col) "pH")) |>
      mutate(
        pH = if (has_ph_col) pH else NA_real_,
        site = site_name,
        logger_id = file_logger_id,
        logger_type = "ph",
        position = position,
        .before = datetime
      ) |>
      mutate(datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
      distinct()
    
    # Set NA for full day when logger was placed in water and taken out
    df <- df |>
      mutate(
        date = as.Date(datetime),
        pH = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ pH
        ),
        millivolts = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ millivolts
        ),
        ph_temp_c = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ ph_temp_c
        )
      ) |>
      select(-date)
    
    if (verbose) {
      cli_alert_info(paste("Rows after filtering:", nrow(df)))
      cli_alert_success(paste("Finished processing", file_name))
    }
  }
  
  ##### WATER LEVEL LOGGER #####
  else if (sensor_type == "water level") {
    
    lines <- readLines(temp_path, n = 5)
    header_line <- which(
      sapply(lines, function(line) {
        clean_line <- str_remove(line, "^#\\s*")
        lower_line <- tolower(clean_line)
        str_detect(lower_line, "date") && str_detect(lower_line, "pres")
      })
    )[1]
    
    if (is.na(header_line)) {
      cli_alert_warning(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    
    if (verbose) {
      cli_alert_info(paste("Skipping", skip_n, "lines"))
    }
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE)) |>
      clean_names()
    
    if (verbose) {
      cli_alert_info("Columns detected:")
      cli_alert_info(paste(names(df), collapse = ", "))
    }
    
    pres_col <- names(df)[str_detect(names(df), "pres")][1]
    datetime_col <- names(df)[str_detect(names(df), "date")][1]
    temp_col <- names(df)[str_detect(names(df), "temp")][1]
    
    if (verbose) {
      cli_alert_info(paste("Pressure column:", pres_col))
      cli_alert_info(paste("Datetime column:", datetime_col))
      cli_alert_info(paste("Temp column:", temp_col))
    }
    
    unit <- case_when(
      str_detect(pres_col, "psi") ~ "psi",
      str_detect(pres_col, "k.?pa") ~ "kpa",
      TRUE ~ NA_character_
    )
    
    if (verbose) {
      cli_alert_info(paste("Detected unit:", unit))
    }
    
    df <- df |>
      select(datetime = all_of(datetime_col),
             wl_temp_c = all_of(temp_col),
             abs_pres = all_of(pres_col)) |>
      mutate(abs_pres_kpa = case_when(
        unit == "psi" ~ as.numeric(abs_pres) * 6.89476,
        unit == "kpa" ~ as.numeric(abs_pres),
        TRUE ~ NA_real_)) |>
      select(-abs_pres)
    
    if (verbose) {
      cli_alert_info("Columns selected:")
      cli_alert_info(paste(names(df), collapse = ", "))
      cli_alert_info(paste("Rows before filtering:", nrow(df)))
    }
    
    df <- df |>
      mutate(site = site_name,
             logger_id = file_logger_id,
             logger_type = "water level",
             position = position,
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
      filter(!is.na(abs_pres_kpa)) |>
      distinct()
    
    # Set NA for full day when logger was placed in water and taken out
    df <- df |>
      mutate(
        date = as.Date(datetime),
        abs_pres_kpa = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ abs_pres_kpa
        ),
        wl_temp_c = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ wl_temp_c
        )
      ) |>
      select(-date)
    
    if (verbose) {
      cli_alert_info(paste("Rows after filtering:", nrow(df)))
      cli_alert_success(paste("Finished processing", file_name))
    }
  }
  
  
  ##### CONDUCTIVITY LOGGER #####
  else if (sensor_type == "conductivity") {
    
    lines <- readLines(temp_path, n = 5)
    header_line <- which(
      sapply(lines, function(line) {
        clean_line <- str_remove(line, "^#\\s*")
        lower_line <- tolower(clean_line)
        str_detect(lower_line, "date") && str_detect(lower_line, "range")
      })
    )[1]
    
    if (is.na(header_line)) {
      cli_alert_warning(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    
    if (verbose) {
      cli_alert_info(paste("Skipping", skip_n, "lines"))
    }
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
    
    if (verbose) {
      cli_alert_info("Columns detected:")
      cli_alert_info(paste(names(df), collapse = ", "))
    }
    
    df <- df |>
      select(contains("date", ignore.case = TRUE),
             contains("temp", ignore.case = TRUE),
             contains("range", ignore.case = TRUE))
    
    if (verbose) {
      cli_alert_info("Columns selected:")
      cli_alert_info(paste(names(df), collapse = ", "))
    }
    
    df <- df |>
      setNames(c("datetime", 
                 "con_temp_c",
                 "high_range_microsiemens_per_cm")) |>
      mutate(site = site_name,
             position = position,
             logger_id = file_logger_id,
             logger_type = "conductivity",
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
      filter(!is.na(high_range_microsiemens_per_cm)) |>
      distinct()
    
    # Set NA for full day when logger was placed in water and taken out
    df <- df |>
      mutate(
        date = as.Date(datetime),
        high_range_microsiemens_per_cm = case_when(
          !is.na(in_water_date) & date < as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date > as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ high_range_microsiemens_per_cm
        ),
        con_temp_c = case_when(
          !is.na(in_water_date) & date < as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date > as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ con_temp_c
        ),
        date_flag = case_when(
          !is.na(in_water_date) & date == as.Date(in_water_date) ~ "Deployment day",
          !is.na(out_of_water_date) & date > as.Date(out_of_water_date) ~ "Retrieval day",
          TRUE ~ NA_character_
        )
      ) |>
      select(-date)
    
    if (verbose) {
      cli_alert_info(paste("Rows after filtering:", nrow(df)))
      cli_alert_success(paste("Finished processing", file_name))
    }
  }
  
  ##### DISSOLVED OXYGEN LOGGER #####
  else if (sensor_type == "dissolved oxygen") {
    
    lines <- readLines(temp_path, n = 5)
    header_line <- which(
      sapply(lines, function(line) {
        clean_line <- str_remove(line, "^#\\s*")
        lower_line <- tolower(clean_line)
        str_detect(lower_line, "date") && str_detect(lower_line, "mg/l")
      })
    )[1]
    
    if (is.na(header_line)) {
      cli_alert_warning(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    
    if (verbose) {
      cli_alert_info(paste("Skipping", skip_n, "lines"))
    }
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
    
    if (verbose) {
      cli_alert_info("Columns detected:")
      cli_alert_info(paste(names(df), collapse = ", "))
    }
    
    df <- df |>
      select(contains("date", ignore.case = TRUE),
             contains("temp", ignore.case = TRUE),
             contains("mg/l", ignore.case = TRUE))
    
    if (verbose) {
      cli_alert_info("Columns selected:")
      cli_alert_info(paste(names(df), collapse = ", "))
    }
    
    df <- df |>
      setNames(c("datetime", "do_temp_c", "do_conc_mg_per_L")) |>
      mutate(site = site_name,
             logger_id = file_logger_id,
             logger_type = "dissolved oxygen",
             position = position,
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
      filter(!is.na(do_conc_mg_per_L)) |>
      distinct()
    
    
    # Set NA for full day when logger was placed in water and taken out
    df <- df |>
      mutate(
        date = as.Date(datetime),
        do_conc_mg_per_L = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ do_conc_mg_per_L
        ),
        do_temp_c = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ do_temp_c
        )
      ) |>
      select(-date)
    
    if (verbose) {
      cli_alert_info(paste("Rows after filtering:", nrow(df)))
      cli_alert_success(paste("Finished processing", file_name))
    }
  }
  
  ##### PAR LOGGER #####
  else if (sensor_type == "par") {
    
    # Skim the start of the file
    lines <- readLines(temp_path, n = 20)
    # Look for the start of the actual data
    data_start <- which(str_detect(lines, "^1,"))[1]
    
    # Start at line 10 if search does not work
    if (is.na(data_start)) {
      cli_alert_warning(paste("Could not detect data start in", file_name, "- defaulting to line 10"))
      data_start <- 10
    }
    
    # Read in the temp file starting at the data start
    df <- suppressWarnings(read_csv(temp_path, skip = data_start - 1, col_names = FALSE, show_col_types = FALSE))
    
    # Add in the column names, and a warning if there is something missing
    if (ncol(df) < 5) {
      cli_alert_warning(paste("Unexpected number of columns in PAR file:", file_name))
      df <- NULL
    } else {
      names(df)[1:5] <- c("scan_no", "date", "time", "raw_integrating_light", "calibrated_integrating_light")
      
      df <- df |>
        mutate(datetime = parse_date_time(paste(date, time),
                                          orders = c("dmy HMS", "dmy HM", "dmy IMp", "m/d/y HM")), 
               .before = raw_integrating_light) |>
        select(datetime, raw_integrating_light, calibrated_integrating_light)
    }
    
    if (!is.null(df)) {
      df <- df |>
        mutate(site = site_name,
               logger_id = file_logger_id,
               logger_type = "par",
               position = position,
               .before = datetime) |>
        filter(!is.na(datetime)) |>
        distinct()
      
      if (verbose) {
        cli_alert_info(paste("Rows after filtering:", nrow(df)))
      }
    }
    
    # Set NA for full day when logger was placed in water and taken out
    df <- df |>
      mutate(
        date = as.Date(datetime),
        raw_integrating_light = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ raw_integrating_light
        ),
        calibrated_integrating_light = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ calibrated_integrating_light
        )
      ) |>
      select(-date)
    
    if (verbose) {
      cli_alert_info(paste("Rows after filtering:", nrow(df)))
      cli_alert_success(paste("Finished processing", file_name))
    }
    
  }
  
  unlink(temp_path)
  return(df)
} # END read_and_clean_logger_csv function ----


#' # Read and clean a logger CSV file from Google Drive ----
#' #'
#' #' @param file_row One row from get_all_logger_csvs_by_id() output
#' #' @param metadata Metadata dataframe from read_and_clean_metadata()
#' #' @return A cleaned logger CSV for one file
#' read_and_clean_logger_csv <- function(file_row, metadata) {
#'   
#'   # Pull out file info
#'   file_id <- file_row$id      # File ID for Google Drive API
#'   file_name <- file_row$name  # File name
#'   sensor_type <- str_trim(tolower(file_row$sensor_type))  # Sensor type pulled from folder name, to know how to proceed processing
#'   
#'   # Grab info from file name
#'   parts <- str_split(file_name, "_", simplify = TRUE)
#'   # Part 1 is site name. Make Title Case
#'   site_name <- str_replace_all(parts[1], "(?<=[a-z])(?=[A-Z])", " ")
#'   # Part 2 is logger_id
#'   file_logger_id <- parts[2]
#'   # Part 3 is date deployed or relaunched
#'   deployment_date <- as.Date(str_remove_all(parts[3], 
#'                                             regex("\\.csv", 
#'                                                   ignore_case = TRUE)))
#'   
#'   print(paste("Site:", site_name,
#'               " | Logger ID:", file_logger_id,
#'               " | Date deployed:", deployment_date))
#'   
#'   
#'   # Join logger file to metadata row
#'   meta_row <- metadata |>
#'     filter(site == site_name,
#'            logger_id == file_logger_id,
#'            as.Date(in_water_date) == deployment_date)
#'   
#'   # Warning if no match in metadata
#'   if (nrow(meta_row) == 0) {
#'     warning(paste("No metadata match for file:", file_name,
#'                   "- check site name, logger_id, and deployment date in metadata sheet"))
#'     return(NULL)
#'   }
#'   
#'   # Warning if multiple matches
#'   if (nrow(meta_row) > 1) {
#'     warning(paste("Multiple metadata matches for:", file_name, "- using first row. Check file name and metadata sheet"))
#'     meta_row <- meta_row[1, ]
#'   }
#'   
#'   # Pull out values from matched metadata row
#'   position <- meta_row$position
#'   deployment_type <- meta_row$deployment_type
#'   in_water_date <- meta_row$in_water_date
#'   out_of_water_date <- meta_row$out_of_water_date
#'   issue_flag <- meta_row$issue_flag
#'   comments <- meta_row$comments
#'   
#'   print(paste("Position:", position, "| Type:", deployment_type,
#'               "| In:", in_water_date, "| Out:", out_of_water_date))
#'   print(paste("Issue flag:", issue_flag, "| Comments:", comments))
#'   print(paste("Sensor type:", sensor_type))
#'   
#'   # Download csv to temp file
#'   temp_path <- tempfile(fileext = ".csv")
#'   if (length(file_id) != 1) {     # Error if there are duplicate files
#'     stop("drive_download() aborted: file_id is not unique.")
#'   }
#'   drive_download(as_id(file_id), path = temp_path, overwrite = TRUE)
#'   print(paste("File downloaded to:", temp_path))
#'   
#'   ##### TEMP LOGGER #####
#'   if (sensor_type == "temperature") {
#'     
#'     # Read the first few lines and find the header row
#'     lines <- readLines(temp_path, n = 5)
#'     header_line <- which(
#'       sapply(lines, function(line) {
#'         clean_line <- str_remove(line, "^#\\s*")
#'         lower_line <- tolower(clean_line)
#'         str_detect(lower_line, "date") && str_detect(lower_line, "temp")
#'       })
#'     )[1]
#'     
#'     # Fallback in case no header line is found
#'     if (is.na(header_line)) {
#'       print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
#'     }
#'     skip_n <- if (!is.na(header_line)) header_line - 1 else 0
#'     print(paste("Skipping", skip_n, "lines"))
#'     
#'     # Read in the csv
#'     df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
#'     
#'     # Check that the header row was correctly read in
#'     print("Columns detected:")
#'     print(names(df))
#'     
#'     # Only keep the rows with actual data
#'     df <- df |>
#'       select(contains("date", ignore.case = TRUE),
#'              contains("temp", ignore.case = TRUE))
#'     # Check that the columns kept were correct
#'     print("Columns selected:")
#'     print(names(df))
#'     
#'     df <- df |>
#'       # Rename selected columns
#'       setNames(c("datetime", "tidbit_temp_c")) |>
#'       mutate(site = site_name, 
#'              position = position, 
#'              logger_id = file_logger_id,
#'              logger_type = "temperature",
#'              .before = datetime) |>
#'       # Make datetime column POSIXct class
#'       mutate(datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
#'       filter(!is.na(tidbit_temp_c)) |>
#'       distinct()
#'     
#'     # Set NA for full day when logger was deployed and relaunched
#'     df <- df |>
#'       mutate(
#'         date = as.Date(datetime),
#'         tidbit_temp_c = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ tidbit_temp_c)
#'       ) |>
#'       select(-date)
#'     
#'     print(paste("Rows after filtering:", nrow(df)))
#'     print(paste("Finished processing", file_name))
#'   }
#'   
#'   ##### PH LOGGER #####
#'   else if (sensor_type == "ph") {
#'     
#'     lines <- readLines(temp_path, n = 5)
#'     header_line <- which(
#'       sapply(lines, function(line) {
#'         clean_line <- str_remove(line, "^#\\s*")
#'         lower_line <- tolower(clean_line)
#'         str_detect(lower_line, "date") && str_detect(lower_line, "mv")
#'       })
#'     )[1]
#'     
#'     if (is.na(header_line)) {
#'       print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
#'     }
#'     skip_n <- if (!is.na(header_line)) header_line - 1 else 0
#'     print(paste("Skipping", skip_n, "lines"))
#'     
#'     df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
#'     print("Columns detected:")
#'     print(names(df))
#'     
#'     ph_matches <- names(df)[str_detect(names(df), regex("ph", ignore_case = TRUE)) &
#'                               !str_detect(names(df), regex("calibrat", ignore_case = TRUE))]
#'     
#'     ph_col <- if (length(ph_matches) > 0) ph_matches[1] else NA_character_
#'     has_ph_col <- !is.na(ph_col)
#'     
#'     if (has_ph_col) {
#'       selected_cols <- df |>
#'         select(contains("date", ignore.case = TRUE),
#'                contains("temp", ignore.case = TRUE),
#'                contains("mv", ignore.case = TRUE),
#'                all_of(ph_col))
#'     } else {
#'       selected_cols <- df |>
#'         select(contains("date", ignore.case = TRUE),
#'                contains("temp", ignore.case = TRUE),
#'                contains("mv", ignore.case = TRUE))
#'     }
#'     
#'     print("Columns selected:")
#'     print(names(selected_cols))
#'     
#'     df <- selected_cols |>
#'       setNames(c("datetime", "ph_temp_c", "millivolts", if (has_ph_col) "pH")) |>
#'       mutate(
#'         pH = if (has_ph_col) pH else NA_real_,
#'         site = site_name,
#'         logger_id = file_logger_id,
#'         logger_type = "ph",
#'         position = position,
#'         .before = datetime
#'       ) |>
#'       mutate(datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
#'       distinct()
#'     
#'     # Set NA for full day when logger was placed in water and taken out
#'     df <- df |>
#'       mutate(
#'         date = as.Date(datetime),
#'         pH = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ pH
#'         ),
#'         millivolts = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ millivolts
#'         ),
#'         ph_temp_c = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ ph_temp_c
#'         )
#'       ) |>
#'       select(-date)
#'     
#'     print(paste("Rows after filtering:", nrow(df)))
#'     print(paste("Finished processing", file_name))
#'   }
#'   
#'   ##### WATER LEVEL LOGGER #####
#'   else if (sensor_type == "water level") {
#'     
#'     lines <- readLines(temp_path, n = 5)
#'     header_line <- which(
#'       sapply(lines, function(line) {
#'         clean_line <- str_remove(line, "^#\\s*")
#'         lower_line <- tolower(clean_line)
#'         str_detect(lower_line, "date") && str_detect(lower_line, "pres")
#'       })
#'     )[1]
#'     
#'     if (is.na(header_line)) {
#'       print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
#'     }
#'     skip_n <- if (!is.na(header_line)) header_line - 1 else 0
#'     print(paste("Skipping", skip_n, "lines"))
#'     
#'     df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE)) |>
#'       clean_names()
#'     
#'     print("Columns detected:")
#'     print(names(df))
#'     
#'     pres_col <- names(df)[str_detect(names(df), "pres")][1]
#'     datetime_col <- names(df)[str_detect(names(df), "date")][1]
#'     temp_col <- names(df)[str_detect(names(df), "temp")][1]
#'     
#'     print(paste("Pressure column:", pres_col))
#'     print(paste("Datetime column:", datetime_col))
#'     print(paste("Temp column:", temp_col))
#'     
#'     unit <- case_when(
#'       str_detect(pres_col, "psi") ~ "psi",
#'       str_detect(pres_col, "k.?pa") ~ "kpa",
#'       TRUE ~ NA_character_
#'     )
#'     
#'     print(paste("Detected unit:", unit))
#'     
#'     df <- df |>
#'       select(datetime = all_of(datetime_col),
#'              wl_temp_c = all_of(temp_col),
#'              abs_pres = all_of(pres_col)) |>
#'       mutate(abs_pres_kpa = case_when(
#'         unit == "psi" ~ as.numeric(abs_pres) * 6.89476,
#'         unit == "kpa" ~ as.numeric(abs_pres),
#'         TRUE ~ NA_real_)) |>
#'       select(-abs_pres)
#'     
#'     print("Columns selected:")
#'     print(names(df))
#'     print(paste("Rows before filtering:", nrow(df)))
#'     
#'     df <- df |>
#'       mutate(site = site_name,
#'              logger_id = file_logger_id,
#'              logger_type = "water level",
#'              position = position,
#'              .before = datetime) |>
#'       mutate(datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
#'       filter(!is.na(abs_pres_kpa)) |>
#'       distinct()
#'     
#'     # Set NA for full day when logger was placed in water and taken out
#'     df <- df |>
#'       mutate(
#'         date = as.Date(datetime),
#'         abs_pres_kpa = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ abs_pres_kpa
#'         ),
#'         wl_temp_c = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ wl_temp_c
#'         )
#'       ) |>
#'       select(-date)
#'     
#'     print(paste("Rows after filtering:", nrow(df)))
#'     print(paste("Finished processing", file_name))
#'   }
#'   
#'   
#'   ##### CONDUCTIVITY LOGGER #####
#'   else if (sensor_type == "conductivity") {
#'     
#'     lines <- readLines(temp_path, n = 5)
#'     header_line <- which(
#'       sapply(lines, function(line) {
#'         clean_line <- str_remove(line, "^#\\s*")
#'         lower_line <- tolower(clean_line)
#'         str_detect(lower_line, "date") && str_detect(lower_line, "range")
#'       })
#'     )[1]
#'     
#'     if (is.na(header_line)) {
#'       print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
#'     }
#'     skip_n <- if (!is.na(header_line)) header_line - 1 else 0
#'     print(paste("Skipping", skip_n, "lines"))
#'     
#'     df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
#'     
#'     print("Columns detected:")
#'     print(names(df))
#'     
#'     df <- df |>
#'       select(contains("date", ignore.case = TRUE),
#'              contains("temp", ignore.case = TRUE),
#'              contains("range", ignore.case = TRUE))
#'     
#'     print("Columns selected:")
#'     print(names(df))
#'     
#'     df <- df |>
#'       setNames(c("datetime", 
#'                  "con_temp_c",
#'                  "high_range_microsiemens_per_cm")) |>
#'       mutate(site = site_name,
#'              position = position,
#'              logger_id = file_logger_id,
#'              logger_type = "conductivity",
#'              .before = datetime) |>
#'       mutate(datetime = parse_date_time(datetime,
#'                                         orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
#'       filter(!is.na(high_range_microsiemens_per_cm)) |>
#'       distinct()
#'     
#'     # Set NA for full day when logger was placed in water and taken out
#'     df <- df |>
#'       mutate(
#'         date = as.Date(datetime),
#'         high_range_microsiemens_per_cm = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ high_range_microsiemens_per_cm
#'         ),
#'         con_temp_c = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ con_temp_c
#'         )
#'       ) |>
#'       select(-date)
#'     
#'     print(paste("Rows after filtering:", nrow(df)))
#'     print(paste("Finished processing", file_name))
#'   }
#'   
#'   ##### DISSOLVED OXYGEN LOGGER #####
#'   else if (sensor_type == "dissolved oxygen") {
#'     
#'     lines <- readLines(temp_path, n = 5)
#'     header_line <- which(
#'       sapply(lines, function(line) {
#'         clean_line <- str_remove(line, "^#\\s*")
#'         lower_line <- tolower(clean_line)
#'         str_detect(lower_line, "date") && str_detect(lower_line, "mg/l")
#'       })
#'     )[1]
#'     
#'     if (is.na(header_line)) {
#'       print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
#'     }
#'     skip_n <- if (!is.na(header_line)) header_line - 1 else 0
#'     print(paste("Skipping", skip_n, "lines"))
#'     
#'     df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
#'     
#'     print("Columns detected:")
#'     print(names(df))
#'     
#'     df <- df |>
#'       select(contains("date", ignore.case = TRUE),
#'              contains("temp", ignore.case = TRUE),
#'              contains("mg/l", ignore.case = TRUE))
#'     
#'     print("Columns selected:")
#'     print(names(df))
#'     
#'     df <- df |>
#'       setNames(c("datetime", "do_temp_c", "do_conc_mg_per_L")) |>
#'       mutate(site = site_name,
#'              logger_id = file_logger_id,
#'              logger_type = "dissolved oxygen",
#'              position = position,
#'              .before = datetime) |>
#'       mutate(datetime = parse_date_time(datetime,
#'                                         orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
#'       filter(!is.na(do_conc_mg_per_L)) |>
#'       distinct()
#'     
#'     
#'     # Set NA for full day when logger was placed in water and taken out
#'     df <- df |>
#'       mutate(
#'         date = as.Date(datetime),
#'         do_conc_mg_per_L = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ do_conc_mg_per_L
#'         ),
#'         do_temp_c = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ do_temp_c
#'         )
#'       ) |>
#'       select(-date)
#'     
#'     print(paste("Rows after filtering:", nrow(df)))
#'     print(paste("Finished processing", file_name))
#'   }
#'   
#'   ##### PAR LOGGER #####
#'   else if (sensor_type == "par") {
#'     
#'     # Skim the start of the file
#'     lines <- readLines(temp_path, n = 20)
#'     # Look for the start of the actual data
#'     data_start <- which(str_detect(lines, "^1,"))[1]
#'     
#'     # Start at line 10 if search does not work
#'     if (is.na(data_start)) {
#'       print(paste("Could not detect data start in", file_name, "- defaulting to line 10"))
#'       data_start <- 10
#'     }
#'     
#'     # Read in the temp file starting at the data start
#'     df <- suppressWarnings(read_csv(temp_path, skip = data_start - 1, col_names = FALSE, show_col_types = FALSE))
#'     
#'     # Add in the column names, and a warning if there is something missing
#'     if (ncol(df) < 5) {
#'       print(paste("Unexpected number of columns in PAR file:", file_name))
#'       df <- NULL
#'     } else {
#'       names(df)[1:5] <- c("scan_no", "date", "time", "raw_integrating_light", "calibrated_integrating_light")
#'       
#'       df <- df |>
#'         mutate(datetime = parse_date_time(paste(date, time),
#'                                           orders = c("dmy HMS", "dmy HM", "dmy IMp", "m/d/y HM")), 
#'                .before = raw_integrating_light) |>
#'         select(datetime, raw_integrating_light, calibrated_integrating_light)
#'     }
#'     
#'     if (!is.null(df)) {
#'       df <- df |>
#'         mutate(site = site_name,
#'                logger_id = file_logger_id,
#'                logger_type = "par",
#'                position = position,
#'                .before = datetime) |>
#'         filter(!is.na(datetime)) |>
#'         distinct()
#'       print(paste("Rows after filtering:", nrow(df)))
#'     }
#'     
#'     # Set NA for full day when logger was placed in water and taken out
#'     df <- df |>
#'       mutate(
#'         date = as.Date(datetime),
#'         raw_integrating_light = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ raw_integrating_light
#'         ),
#'         calibrated_integrating_light = case_when(
#'           !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
#'           !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
#'           TRUE ~ calibrated_integrating_light
#'         )
#'       ) |>
#'       select(-date)
#'     
#'     print(paste("Rows after filtering:", nrow(df)))
#'     print(paste("Finished processing", file_name))
#'     
#'   }
#'   
#'   unlink(temp_path)
#'   return(df)
#' } # END read_and_clean_logger_csv function ----

#' Find the DST "fall-back" Sunday for a given year (US clock-change rule) ----
#'
#' @param year Numeric year
#' @return Date of the first Sunday in November for that year
#'
first_sunday_nov <- function(year) {
  nov_days <- seq(as.Date(paste0(year, "-11-01")), as.Date(paste0(year, "-11-07")), by = "day")
  nov_days[wday(nov_days) == 1][1]
} # END first_sunday_nov function ----


#' Collapse duplicate site/position/datetime rows, but preserve legitimate ----
#' DST "fall-back" duplicates
#'
#' Most duplicate site/position/datetime rows (e.g. overlap between an old and
#' a new processing run) are still collapsed to one row, keeping the first
#' non-NA value per column - same behavior as before. The exception is rows
#' that fall in the November fall-back hour (1:00-1:59 AM local time, on the
#' fall-back Sunday) where more than one reading landed in the same 15-minute
#' bin: those are real, distinct readings, so both are kept. The second
#' reading's datetime is nudged forward by a few seconds so it stays unique
#' (site/position/datetime is otherwise assumed unique downstream), and a note
#' is stashed in a temporary `dst_fallback_note` column so it can be appended
#' to `comments` later, after the metadata join sets the deployment-level
#' comment (this function runs before that join, and comments/issue_flag may
#' not exist as columns yet at that point).
#'
#' @param df A data frame with (at least) site, position, datetime columns
#' @return A data frame with one row per site/position/datetime, except for
#'   flagged fall-back duplicates, which keep all of their rows
#'
dedupe_with_fallback <- function(df) {
  
  if (nrow(df) == 0) return(df)
  
  # Make sure we always have somewhere to stash the fall-back note
  if (!"dst_fallback_note" %in% names(df)) {
    df$dst_fallback_note <- NA_character_
  }
  
  # Look up the fall-back Sunday once per year present in the data (not per row)
  yrs <- unique(year(df$datetime))
  fallback_lookup <- setNames(vapply(yrs, function(y) as.numeric(first_sunday_nov(y)), numeric(1)), yrs)
  
  df <- df |>
    mutate(
      fallback_sunday = as.Date(fallback_lookup[as.character(year(datetime))], origin = "1970-01-01"),
      is_fallback_window = date(datetime) == fallback_sunday & hour(datetime) == 1
    ) |>
    select(-fallback_sunday) |>
    add_count(site, position, datetime, name = "n_dupe")
  
  # Everything that isn't an ambiguous fall-back duplicate: collapse as before
  others <- df |>
    filter(!(is_fallback_window & n_dupe > 1)) |>
    select(-is_fallback_window, -n_dupe) |>
    group_by(site, position, datetime) |>
    summarise(across(everything(), ~ first(na.omit(.x))[1]), .groups = "drop")
  
  # Ambiguous fall-back duplicates: keep every row, offset datetimes to stay unique
  fallback_dupes <- df |>
    filter(is_fallback_window, n_dupe > 1)
  
  if (nrow(fallback_dupes) > 0) {
    fallback_dupes_kept <- fallback_dupes |>
      select(-is_fallback_window, -n_dupe) |>
      arrange(site, position, datetime) |>
      group_by(site, position, datetime) |>
      mutate(
        dupe_index = row_number(),
        datetime = datetime + seconds(dupe_index - 1),
        dst_fallback_note = if_else(dupe_index > 1,
                                    "Duplicate reading retained - DST fall-back",
                                    dst_fallback_note)
      ) |>
      ungroup() |>
      select(-dupe_index)
    
    result <- bind_rows(others, fallback_dupes_kept) |>
      arrange(site, position, datetime)
  } else {
    result <- others
  }
  
  return(result)
} # END dedupe_with_fallback function ----

# MAIN FUNCTION: Incremental update to processes new files ----
#'
#' Updates logger data by only processing new/changed files since last update
#'
#' @param root_folder_id The Google Drive folder ID containing sensor subfolders
#' @param metadata_file_url URL to main index metadata google sheet  
#' @param sheet_name Name of the tab that the metadata is on
#' @param existing_data_prefix Prefix for existing data files (e.g., "logger_data" creates "logger_data_temp.rds", etc.)
#' @param tracking_file Path to file tracking what's been processed
#' @param force_reprocess Logical, whether to reprocess all files (default FALSE). TRUE if you want to process all files in drive
#' @return Updated combined logger data
#'
update_logger_data_incremental <- function(root_folder_id, metadata_file_url, sheet_name,
                                           existing_data_prefix = "logger_data",
                                           tracking_file = "processed_files.rds",
                                           force_reprocess = FALSE,
                                           verbose = FALSE) {
  
  cli_h1("Incremental logger data update")
  
  # ===== 1. Load associated info from file name and metadata ===== #
  
  # Define sensor types and their file names
  sensor_types <- c("temp", "ph", "do", "wl", "par", "con")
  sensor_names <- c("temperature", "pH", "dissolved_oxygen", "water_level", "PAR", "conductivity")
  sensor_logger_types <- c(temp = "temperature", ph = "ph", do = "dissolved oxygen",
                           wl = "water level", par = "par", con = "conductivity")
  
  
  # Load existing data if it exists
  existing_data <- list()
  data_dir <- here::here("data")
  if (!force_reprocess) {
    for (i in seq_along(sensor_types)) {
      sensor_file <- paste0(existing_data_prefix, "_", sensor_types[i], ".rds")
      data_file_path <- file.path(data_dir, sensor_file)
      
      if (file.exists(data_file_path)) {
        cli_alert_info(paste("Loading existing", sensor_names[i], "data..."))
        existing_data[[sensor_types[i]]] <- readRDS(data_file_path)
        cli_alert_success(paste("Existing", sensor_names[i], "data has", 
                                nrow(existing_data[[sensor_types[i]]]), "rows"))
      }
    }
  }
  
  # Get list of previously processed files
  processed_files <- character(0)
  tracking_file_path <- here::here("data", tracking_file)
  if (file.exists(tracking_file_path) && !force_reprocess) {
    processed_files <- readRDS(tracking_file_path)
    cli_alert_info(paste("Found", length(processed_files), "previously processed files"))
  }
  
  # Get current file list from Drive
  cli_alert_info("Scanning Google Drive for files...")
  all_files <- get_all_logger_csvs_by_id(root_folder_id)
  
  if (nrow(all_files) == 0) {
    cli_alert_warning("No CSV files found in Drive folder")
    return(existing_data)
  }
  
  # Identify new files to process
  if (force_reprocess) {
    files_to_process <- all_files # Process them all if we are reprocessing
    cli_alert_warning(paste("Force reprocess: will process all", nrow(files_to_process), "files"))
  } else {
    files_to_process <- all_files |>
      filter(!(name %in% processed_files))  # Only process files whos name is not in file tracker
    cli_alert_info(paste("Found", nrow(files_to_process), "new files to process"))
  }
  
  # If no new files, return existing data
  if (nrow(files_to_process) == 0) {
    cli_alert_success("No new files to process, data is up to date.")
    return(existing_data)
  }
  
  # Load metadata (always refresh this in case of updates)
  cli_alert_info("Loading metadata...")
  metadata <- read_and_clean_metadata(metadata_file_url, sheet_name)
  
  # ===== 2. Process new files ===== #
  cli_h2("Processing new files")
  cli_alert_info(paste("Processing", nrow(files_to_process), "new files..."))
  
  # Initialize lists for new data by sensor type
  new_temp_data <- list()
  new_ph_data <- list()
  new_do_data <- list()
  new_wl_data <- list()
  new_par_data <- list()
  new_con_data <- list()
  
  # Track successfully and unsuccessfully processed files
  successfully_processed_files   <- character(0)
  unsuccessfully_processed_files <- character(0)
  
  # Process each new file
  for (i in seq_len(nrow(files_to_process))) {
    file_row <- files_to_process[i, ]
    sensor_type <- str_trim(tolower(file_row$sensor_type))
    
    cli_alert_info(paste("Processing new file", i, "of", nrow(files_to_process), ":", file_row$name))
    
    tryCatch({ # trying this new way of throwing warnings/errors
      cleaned_data <- read_and_clean_logger_csv(file_row, metadata, verbose = verbose)
      
      if (is.null(cleaned_data)) {
        cli_alert_warning(paste("Skipping file (no metadata match):", file_row$name,
                                "- fix metadata entry and rerun to process this file"))
        unsuccessfully_processed_files <- c(unsuccessfully_processed_files, file_row$name)
      } else {
        if      (sensor_type == "temperature")      new_temp_data <- append(new_temp_data, list(cleaned_data))
        else if (sensor_type == "ph")               new_ph_data   <- append(new_ph_data,   list(cleaned_data))
        else if (sensor_type == "dissolved oxygen") new_do_data   <- append(new_do_data,   list(cleaned_data))
        else if (sensor_type == "water level")      new_wl_data   <- append(new_wl_data,   list(cleaned_data))
        else if (sensor_type == "par")              new_par_data  <- append(new_par_data,  list(cleaned_data))
        else if (sensor_type == "conductivity")     new_con_data  <- append(new_con_data,  list(cleaned_data))
        
        successfully_processed_files <- c(successfully_processed_files, file_row$name)
        cli_alert_success(paste("Successfully processed:", file_row$name))
      }
      
    }, error = function(e) {
      cli_alert_danger(paste("Error processing file", file_row$name, ":", e$message))
      cli_alert_info("Check that file name and metadata are as expected")
      cli_alert_info(paste("File", file_row$name, "will be retried on next run"))
      unsuccessfully_processed_files <<- c(unsuccessfully_processed_files, file_row$name)
    })
  }
  
  # Combine new data by sensor type
  new_df_temp <- if (length(new_temp_data) > 0) bind_rows(new_temp_data) else NULL
  new_df_ph <- if (length(new_ph_data) > 0) bind_rows(new_ph_data) else NULL
  new_df_do <- if (length(new_do_data) > 0) bind_rows(new_do_data) else NULL
  new_df_wl <- if (length(new_wl_data) > 0) bind_rows(new_wl_data) else NULL
  new_df_par <- if (length(new_par_data) > 0) bind_rows(new_par_data) else NULL
  new_df_con <- if (length(new_con_data) > 0) bind_rows(new_con_data) else NULL
  
  # 3. ===== Combine new and existing data by sensor type and complete timeseries ===== #
  
  cli_h2("Completing sensor datasets")
  
  final_data <- list()
  
  # Loop through each sensor type
  for (i in seq_along(sensor_types)) {
    sensor <- sensor_types[i]
    sensor_name <- sensor_names[i]
    logger_type_value <- sensor_logger_types[[sensor]]
    
    # Get new and existing data for this sensor
    new_data <- get(paste0("new_df_", sensor))
    old_data <- existing_data[[sensor]]
    
    # Skip if no data
    if (is.null(new_data) && is.null(old_data)) {
      cli_alert_info(paste("No", sensor_name, "data to process"))
      final_data[[sensor]] <- NULL
      next
    }
    
    cli_alert_info(paste("Processing", sensor_name, "data..."))
    
    # Combine new and existing data
    combined_data <- bind_rows(old_data, new_data)
    
    # ----
    # Metadata rows for this sensor type only
    meta_this_sensor <- metadata |>
      filter(logger_type == logger_type_value)
    
    # Build full in/out-of-water datetimes (date + time-of-day) for this sensor's
    # deployments, used both for the completion "anchors" below and the final
    # issue_flag/comments join.
    meta_this_sensor <- meta_this_sensor |>
      mutate(
        in_water_datetime = suppressWarnings(parse_date_time(
          paste(in_water_date, coalesce(in_water_time, "00:00")),
          orders = c("ymd HM", "ymd HMS"))),
        out_of_water_datetime = suppressWarnings(parse_date_time(
          paste(out_of_water_date, coalesce(out_of_water_time, "23:59")),
          orders = c("ymd HM", "ymd HMS")))
      )
    
    # Anchor rows: make sure the completed 15-minute time series always spans
    # each full deployment window (in_water -> out_of_water), even when real
    # data only covers part of it (e.g. the instrument stopped recording early
    # but wasn't physically recovered until much later)
    deployment_anchors <- meta_this_sensor |>
      filter(!is.na(in_water_datetime), !is.na(out_of_water_datetime)) |>
      mutate(
        in_water_datetime = round_date(in_water_datetime, "15 minutes"),
        out_of_water_datetime = round_date(out_of_water_datetime, "15 minutes")
      ) |>
      select(site, position, in_water_datetime, out_of_water_datetime) |>
      pivot_longer(cols = c(in_water_datetime, out_of_water_datetime), values_to = "datetime") |>
      mutate(logger_type = logger_type_value) |>
      select(site, position, logger_type, datetime) |>
      distinct()
    
    combined_data <- bind_rows(combined_data, deployment_anchors) |>
      # logger_type is constant for this whole loop iteration - set it directly
      # rather than relying on it surviving complete()/summarise() below, since
      # gap rows created by complete() don't inherit it (this was silently
      # breaking the metadata join further down: NA logger_type never matches
      # in an equality join, so gap rows never got flagged).
      mutate(logger_type = logger_type_value)
    
    # ----
    
    
    
    # Round timestamps to nearest 15 minutes and collapse duplicates, keeping
    # (and flagging) legitimate DST fall-back duplicates
    combined_data <- combined_data |>
      mutate(datetime = round_date(datetime, "15 minutes")) |>
      dedupe_with_fallback()
    
    # Create complete 15-minute time series (now guaranteed to span each full
    # deployment window, thanks to the anchor rows added above)
    combined_data <- combined_data |>
      group_by(site, position) |>
      complete(datetime = seq(min(datetime, na.rm = TRUE), 
                              max(datetime, na.rm = TRUE), 
                              by = "15 mins")) |>
      ungroup() |>
      mutate(logger_type = logger_type_value) |>
      dedupe_with_fallback()
    
    
    # Assign issue_flag and comments exactly once, from metadata, for the whole
    # deployment window each row falls in (this replaces both the old per-file
    # stamping in read_and_clean_logger_csv and the old second coalesce() join)
    meta_lookup <- meta_this_sensor |>
      select(site, position, logger_type, in_water_datetime, out_of_water_datetime,
             issue_flag, comments)
    
    
    
    combined_data <- combined_data |>
      select(-any_of(c("issue_flag", "comments"))) |>
      left_join(meta_lookup,
                join_by(site, position, logger_type,
                        datetime >= in_water_datetime,
                        datetime <= out_of_water_datetime)) |>
      select(-in_water_datetime, -out_of_water_datetime) |>
      # Append the DST fall-back note onto whatever deployment comment applies,
      # rather than letting either one clobber the other
      mutate(
        comments = case_when(
          !is.na(dst_fallback_note) & !is.na(comments) ~ paste0(comments, "; ", dst_fallback_note),
          !is.na(dst_fallback_note) & is.na(comments)   ~ dst_fallback_note,
          TRUE ~ comments
        )
      ) |>
      select(-dst_fallback_note) |>
      arrange(site, position, datetime) |>
      distinct()
    
    cli_alert_success(paste(sensor_name, "data:", nrow(combined_data), "rows"))
    final_data[[sensor]] <- combined_data
  }
  
  
  
  # 4. Save updated data and tracking info
  cli_h2("Saving updated data")
  
  # Create data folder if it doesn't exist
  if (!dir.exists(data_dir)) {
    dir.create(data_dir)
  }
  
  # Save each sensor type to separate file
  for (sensor in sensor_types) {
    if (!is.null(final_data[[sensor]])) {
      sensor_file <- paste0(existing_data_prefix, "_", sensor, ".rds")
      data_file_path <- file.path(data_dir, sensor_file)
      saveRDS(final_data[[sensor]], data_file_path)
      cli_alert_success(paste("Saved", sensor_names[which(sensor_types == sensor)], 
                              "data to:", data_file_path))
    }
  }
  
  # Update processed files list - ONLY with successfully processed files
  updated_processed_files <- unique(c(processed_files, successfully_processed_files))
  saveRDS(updated_processed_files, tracking_file_path)
  
  # Save unsuccessful files list for review 
  unsuccessful_file_path <- here::here("data", "unsuccessful_files.rds")
  saveRDS(unsuccessfully_processed_files, unsuccessful_file_path)
  
  cli_alert_success(paste("Processing complete!"))
  cli_alert_success(paste("Successfully processed", length(successfully_processed_files), "new files"))
  cli_alert_warning(paste("Failed to process", nrow(files_to_process) - length(successfully_processed_files), "files"))
  
  # Print unsuccessful files for review
  if (length(unsuccessfully_processed_files) > 0) {
    cli_alert_warning("The following files were not processed and need review:")
    for (f in unsuccessfully_processed_files) cli_alert_warning(paste(" -", f))
  }
  
  # Print summary for each sensor type
  for (i in seq_along(sensor_types)) {
    if (!is.null(final_data[[sensor_types[i]]])) {
      cli_alert_info(paste(sensor_names[i], "dataset:", 
                           nrow(final_data[[sensor_types[i]]]), "rows,",
                           ncol(final_data[[sensor_types[i]]]), "columns"))
    }
  }
  
  return(final_data)
} # END update_logger_data_incremental ----

#' # MAIN FUNCTION: Incremental update to processes new files ----
#' #'
#' #' Updates logger data by only processing new/changed files since last update
#' #'
#' #' @param root_folder_id The Google Drive folder ID containing sensor subfolders
#' #' @param metadata_file_url URL to main index metadata google sheet  
#' #' @param sheet_name Name of the tab that the metadata is on
#' #' @param existing_data_prefix Prefix for existing data files (e.g., "logger_data" creates "logger_data_temp.rds", etc.)
#' #' @param tracking_file Path to file tracking what's been processed
#' #' @param force_reprocess Logical, whether to reprocess all files (default FALSE). TRUE if you want to process all files in drive
#' #' @return Updated combined logger data
#' #'
#' update_logger_data_incremental <- function(root_folder_id, metadata_file_url, sheet_name,
#'                                            existing_data_prefix = "logger_data",
#'                                            tracking_file = "processed_files.rds",
#'                                            force_reprocess = FALSE,
#'                                            verbose = FALSE) {
#'   
#'   cli_alert_info("Starting incremental logger data update...")
#'   
#'   # ===== 1. Load associated info from file name and metadata ===== #
#'   
#'   # Define sensor types and their file names
#'   sensor_types <- c("temp", "ph", "do", "wl", "par", "con")
#'   sensor_names <- c("temperature", "pH", "dissolved_oxygen", "water_level", "PAR", "conductivity")
#'   sensor_logger_types <- c(temp = "temperature", ph = "ph", do = "dissolved oxygen",
#'                            wl = "water level", par = "par", con = "conductivity")
#'   
#'   
#'   # Load existing data if it exists
#'   existing_data <- list()
#'   data_dir <- here::here("data")
#'   if (!force_reprocess) {
#'     for (i in seq_along(sensor_types)) {
#'       sensor_file <- paste0(existing_data_prefix, "_", sensor_types[i], ".rds")
#'       data_file_path <- file.path(data_dir, sensor_file)
#'       
#'       if (file.exists(data_file_path)) {
#'         cli_alert_info(paste("Loading existing", sensor_names[i], "data..."))
#'         existing_data[[sensor_types[i]]] <- readRDS(data_file_path)
#'         cli_alert_info(paste("Existing", sensor_names[i], "data has", 
#'                              nrow(existing_data[[sensor_types[i]]]), "rows"))
#'       }
#'     }
#'   }
#'   
#'   # Get list of previously processed files
#'   processed_files <- character(0)
#'   tracking_file_path <- here::here("data", tracking_file)
#'   if (file.exists(tracking_file_path) && !force_reprocess) {
#'     processed_files <- readRDS(tracking_file_path)
#'     cli_alert_info(paste("Found", length(processed_files), "previously processed files"))
#'     
#'   }
#'   
#'   # Get current file list from Drive
#'   cli_alert_info("Scanning Google Drive for files...")
#'   all_files <- get_all_logger_csvs_by_id(root_folder_id)
#'   
#'   if (nrow(all_files) == 0) {
#'     cli_alert_warning("No CSV files found in Drive folder")
#'     return(existing_data)
#'   }
#'   
#'   # Identify new files to process
#'   if (force_reprocess) {
#'     files_to_process <- all_files # Process them all if we are reprocessing
#'     cli_alert_warning(paste("Force reprocess: will process all", 
#'                             nrow(files_to_process), "files"))
#'   } else {
#'     files_to_process <- all_files |>
#'       filter(!(name %in% processed_files))  # Only process files whos name is not in file tracker
#'     cli_alert_info(paste("Found", nrow(files_to_process), "new files to process"))
#'   }
#'   
#'   # If no new files, return existing data
#'   if (nrow(files_to_process) == 0) {
#'     cli_alert_success("No new files to process, data is up to date.")
#'     return(existing_data)
#'   }
#'   
#'   # Load metadata (always refresh this in case of updates)
#'   cli_alert_info("Loading metadata...")
#'   metadata <- read_and_clean_metadata(metadata_file_url, sheet_name)
#'   
#'   # ===== 2. Process new files ===== #
#'   cli_alert_info(paste("Processing", nrow(files_to_process), "new files..."))
#'   
#'   # Initialize lists for new data by sensor type
#'   new_temp_data <- list()
#'   new_ph_data <- list()
#'   new_do_data <- list()
#'   new_wl_data <- list()
#'   new_par_data <- list()
#'   new_con_data <- list()
#'   
#'   # Track successfully and unsuccessfully processed files
#'   successfully_processed_files   <- character(0)
#'   unsuccessfully_processed_files <- character(0)
#'   
#'   # Process each new file
#'   for (i in seq_len(nrow(files_to_process))) {
#'     file_row <- files_to_process[i, ]
#'     sensor_type <- str_trim(tolower(file_row$sensor_type))
#'     
#'     cli_alert_info(paste("Processing new file", i, "of", 
#'                          nrow(files_to_process), ":", file_row$name))
#'     
#'     
#'     tryCatch({ # trying this new way of throwing warnings/errors
#'       cleaned_data <- read_and_clean_logger_csv(file_row, metadata)
#'       
#'       if (is.null(cleaned_data)) {
#'         cli_alert_warning(paste("Skipping file (no metadata match):", file_row$name,
#'                                 "- fix metadata entry and rerun to process this file"))
#'         unsuccessfully_processed_files <- c(unsuccessfully_processed_files, file_row$name)
#'       } else {
#'         if      (sensor_type == "temperature")      new_temp_data <- append(new_temp_data, list(cleaned_data))
#'         else if (sensor_type == "ph")               new_ph_data   <- append(new_ph_data,   list(cleaned_data))
#'         else if (sensor_type == "dissolved oxygen") new_do_data   <- append(new_do_data,   list(cleaned_data))
#'         else if (sensor_type == "water level")      new_wl_data   <- append(new_wl_data,   list(cleaned_data))
#'         else if (sensor_type == "par")              new_par_data  <- append(new_par_data,  list(cleaned_data))
#'         else if (sensor_type == "conductivity")     new_con_data  <- append(new_con_data,  list(cleaned_data))
#'         
#'         successfully_processed_files <- c(successfully_processed_files, file_row$name)
#'         cli_alert_success(paste("Successfully processed:", file_row$name))
#'       }
#'       
#'     }, error = function(e) {
#'       cli_alert_danger(paste("Error processing file", file_row$name, ":", e$message))
#'       cli_alert_info("Check that file name and metadata are as expected")
#'       cli_alert_info(paste("File", file_row$name, "will be retried on next run"))
#'       unsuccessfully_processed_files <<- c(unsuccessfully_processed_files, file_row$name)
#'     })
#'   }
#'   
#'   # Combine new data by sensor type
#'   new_df_temp <- if (length(new_temp_data) > 0) bind_rows(new_temp_data) else NULL
#'   new_df_ph <- if (length(new_ph_data) > 0) bind_rows(new_ph_data) else NULL
#'   new_df_do <- if (length(new_do_data) > 0) bind_rows(new_do_data) else NULL
#'   new_df_wl <- if (length(new_wl_data) > 0) bind_rows(new_wl_data) else NULL
#'   new_df_par <- if (length(new_par_data) > 0) bind_rows(new_par_data) else NULL
#'   new_df_con <- if (length(new_con_data) > 0) bind_rows(new_con_data) else NULL
#'   
#'   # 3. ===== Combine new and existing data by sensor type and complete timeseries ===== #
#'   
#'   final_data <- list()
#'   
#'   # Loop through each sensor type
#'   for (i in seq_along(sensor_types)) {
#'     sensor <- sensor_types[i]
#'     sensor_name <- sensor_names[i]
#'     logger_type_value <- sensor_logger_types[[sensor]]
#'     
#'     # Get new and existing data for this sensor
#'     new_data <- get(paste0("new_df_", sensor))
#'     old_data <- existing_data[[sensor]]
#'     
#'     # Skip if no data
#'     if (is.null(new_data) && is.null(old_data)) {
#'       cli_alert_info(paste("No", sensor_name, "data to process"))
#'       final_data[[sensor]] <- NULL
#'       next
#'     }
#'     
#'     cli_alert_info(paste("Processing", sensor_name, "data..."))
#'     
#'     # Combine new and existing data
#'     combined_data <- bind_rows(old_data, new_data)
#'     
#' 
#'     # Metadata rows for this sensor type only
#'     meta_this_sensor <- metadata |>
#'       filter(logger_type == logger_type_value)
#'     
#'     # Build full in/out-of-water datetimes (date + time-of-day) for this sensor's
#'     # deployments, used both for the completion "anchors" below and the final
#'     # issue_flag/comments join.
#'     meta_this_sensor <- meta_this_sensor |>
#'       mutate(
#'         in_water_datetime = suppressWarnings(parse_date_time(
#'           paste(in_water_date, coalesce(in_water_time, "00:00")),
#'           orders = c("ymd HM", "ymd HMS"))),
#'         out_of_water_datetime = suppressWarnings(parse_date_time(
#'           paste(out_of_water_date, coalesce(out_of_water_time, "23:59")),
#'           orders = c("ymd HM", "ymd HMS")))
#'       )
#'     
#'     # Anchor rows: make sure the completed 15-minute time series always spans
#'     # each full deployment window (in_water -> out_of_water), even when real
#'     # data only covers part of it (e.g. the instrument stopped recording early
#'     # but wasn't physically recovered until much later)
#'     deployment_anchors <- meta_this_sensor |>
#'       filter(!is.na(in_water_datetime), !is.na(out_of_water_datetime)) |>
#'       mutate(
#'         in_water_datetime = round_date(in_water_datetime, "15 minutes"),
#'         out_of_water_datetime = round_date(out_of_water_datetime, "15 minutes")
#'       ) |>
#'       select(site, position, in_water_datetime, out_of_water_datetime) |>
#'       pivot_longer(cols = c(in_water_datetime, out_of_water_datetime), values_to = "datetime") |>
#'       mutate(logger_type = logger_type_value) |>
#'       select(site, position, logger_type, datetime) |>
#'       distinct()
#'     
#'     combined_data <- bind_rows(combined_data, deployment_anchors) |>
#'       # logger_type is constant for this whole loop iteration - set it directly
#'       # rather than relying on it surviving complete()/summarise() below, since
#'       # gap rows created by complete() don't inherit it (this was silently
#'       # breaking the metadata join further down: NA logger_type never matches
#'       # in an equality join, so gap rows never got flagged).
#'       mutate(logger_type = logger_type_value)
#'     
#'     # Round timestamps to nearest 15 minutes and collapse duplicates, keeping
#'     # (and flagging) legitimate DST fall-back duplicates
#'     combined_data <- combined_data |>
#'       mutate(datetime = round_date(datetime, "15 minutes")) |>
#'       dedupe_with_fallback()
#'     
#'     # Create complete 15-minute time series (now guaranteed to span each full
#'     # deployment window, thanks to the anchor rows added above)
#'     combined_data <- combined_data |>
#'       group_by(site, position) |>
#'       complete(datetime = seq(min(datetime, na.rm = TRUE), 
#'                               max(datetime, na.rm = TRUE), 
#'                               by = "15 mins")) |>
#'       ungroup() |>
#'       mutate(logger_type = logger_type_value) |>
#'       dedupe_with_fallback()
#'     
#'     
#'     # Assign issue_flag and comments exactly once, from metadata, for the whole
#'     # deployment window each row falls in (this replaces both the old per-file
#'     # stamping in read_and_clean_logger_csv and the old second coalesce() join)
#'     meta_lookup <- meta_this_sensor |>
#'       select(site, position, logger_type, in_water_datetime, out_of_water_datetime,
#'              issue_flag, comments)
#'     
#'     
#'     
#'     combined_data <- combined_data |>
#'       select(-any_of(c("issue_flag", "comments"))) |>
#'       left_join(meta_lookup,
#'                 join_by(site, position, logger_type,
#'                         datetime >= in_water_datetime,
#'                         datetime <= out_of_water_datetime)) |>
#'       select(-in_water_datetime, -out_of_water_datetime) |>
#'       # Append the DST fall-back note onto whatever deployment comment applies,
#'       # rather than letting either one clobber the other
#'       mutate(
#'         comments = case_when(
#'           !is.na(dst_fallback_note) & !is.na(comments) ~ paste0(comments, "; ", dst_fallback_note),
#'           !is.na(dst_fallback_note) & is.na(comments)   ~ dst_fallback_note,
#'           TRUE ~ comments
#'         )
#'       ) |>
#'       select(-dst_fallback_note) |>
#'       arrange(site, position, datetime) |>
#'       distinct()
#'     
#'     cli_alert_info(paste(sensor_name, "data:", nrow(combined_data), "rows"))
#'     final_data[[sensor]] <- combined_data
#'   }
#'   
#'   
#'   
#'   # 4. Save updated data and tracking info
#'   message("Saving updated data...")
#'   
#'   # Create data folder if it doesn't exist
#'   if (!dir.exists(data_dir)) {
#'     dir.create(data_dir)
#'   }
#'   
#'   # Save each sensor type to separate file
#'   for (sensor in sensor_types) {
#'     if (!is.null(final_data[[sensor]])) {
#'       sensor_file <- paste0(existing_data_prefix, "_", sensor, ".rds")
#'       data_file_path <- file.path(data_dir, sensor_file)
#'       saveRDS(final_data[[sensor]], data_file_path)
#'       message(paste("Saved", sensor_names[which(sensor_types == sensor)], 
#'                     "data to:", data_file_path))
#'     }
#'   }
#'   
#'   # Update processed files list - ONLY with successfully processed files
#'   updated_processed_files <- unique(c(processed_files, successfully_processed_files))
#'   saveRDS(updated_processed_files, tracking_file_path)
#'   
#'   # Save unsuccessful files list for review 
#'   unsuccessful_file_path <- here::here("data", "unsuccessful_files.rds")
#'   saveRDS(unsuccessfully_processed_files, unsuccessful_file_path)
#'   
#'   message(paste("Processing complete!"))
#'   message(paste("Successfully processed", length(successfully_processed_files), "new files"))
#'   message(paste("Failed to process", nrow(files_to_process) - length(successfully_processed_files), "files"))
#'   
#'   # Print unsuccessful files for review
#'   if (length(unsuccessfully_processed_files) > 0) {
#'     message("The following files were not processed and need review:")
#'     for (f in unsuccessfully_processed_files) message(paste(" -", f))
#'   }
#'   
#'   # Print summary for each sensor type
#'   for (i in seq_along(sensor_types)) {
#'     if (!is.null(final_data[[sensor_types[i]]])) {
#'       message(paste(sensor_names[i], "dataset:", 
#'                     nrow(final_data[[sensor_types[i]]]), "rows,",
#'                     ncol(final_data[[sensor_types[i]]]), "columns"))
#'     }
#'   }
#'   
#'   return(final_data)
#' } # END update_logger_data_incremental ----


#' Helper function to build a plot for a site x position combo with data issue flags
#'
#' @param data (dataframe) The data frame to visualize
#' @param site_name (character string) The site name as it appears in the data 
#' @param pos (character string) surface or bottom
#' @param sensor_col (character string) the exact name of the column with data
#' @param y_label (character string) 
#' @param title_sensor (character string) 
#'
#' @returns A ggplot
#' 
issue_plot_builder <- function(data, site_name, pos, sensor_col, y_label, title_sensor) {
  
  # Filter to this site/position and valid datetime
  plot_data <- data |>
    filter(site == site_name,
           position == pos,
           !is.na(datetime))
  
  # Skip if no data for this combo
  if (nrow(plot_data) == 0) {
    message(paste("No data found for:", site_name, pos, "- skipping"))
    return(NULL)
  }
  
  # Build flagged periods
  issue_periods <- plot_data |>
    filter(!is.na(issue_flag),
           !is.na(datetime)) |>
    arrange(datetime) |>
    mutate(time_gap = as.numeric(difftime(datetime, lag(datetime), units = "hours")),
           new_group = is.na(time_gap) |
             issue_flag != lag(issue_flag, default = "") |
             time_gap > 1,
           group_id = cumsum(new_group)) |>
    group_by(site, position, issue_flag, group_id) |>
    summarise(start = min(datetime, na.rm = TRUE),
              end = max(datetime, na.rm = TRUE),
              .groups = "drop") |>
    
    # Determine which years each issue spans
    mutate(start_year = lubridate::year(start),
           end_year = lubridate::year(end)) |>
    
    # Create one row for each year
    tidyr::uncount(end_year - start_year + 1) |>
    
    group_by(site, position, issue_flag, group_id) |>
    mutate(year = start_year + row_number() - 1,
           
           # Start of rectangle within this year
           rect_start = if_else(year == start_year,
                                start,
                                lubridate::make_datetime(year,
                                                         1, 1,
                                                         tz = lubridate::tz(start))),
           
           # End of rectangle within this year
           rect_end = if_else(year == end_year,
                              end,
                              lubridate::make_datetime(year + 1,
                                                       1, 1,
                                                       tz = lubridate::tz(end)))) |>
    ungroup()
  
  # Plot
  ggplot(plot_data |> 
           mutate(year = year(datetime)),
         aes(x = datetime, y = .data[[sensor_col]])) +
    geom_rect(data = issue_periods,
              aes(xmin = rect_start,
                  xmax = rect_end,
                  ymin = -Inf,
                  ymax = Inf,
                  fill = issue_flag),
              inherit.aes = FALSE,
              alpha = 0.2
    ) +
    geom_point(alpha = 0.3, size = 0.8) +
    scale_x_datetime(expand = c(0, 0)) +
    labs(title = paste0(site_name, ", ", str_to_title(pos), ", ", title_sensor, " (Raw)"),
         subtitle = "Data issues for deployments highlighted behind points",
         y = y_label,
         fill = "Issue Flag") +
    facet_wrap(~ year, 
               scales = "free_x", 
               ncol = 1) +
    theme_minimal(base_size = 12) +
    theme(plot.title.position = "plot")
} # END issue_plot_builder function ----


#' MAIN VIZ FUNCTION: Visualize data and issues with deployments
#' 
#' Use this function to visualize the data with issue flags for review. 
#' 
#' @param data The data frame that you would like to visualize.
#'
#' @returns A list of plots to scroll through, to manually check data issues noted in the metadata. 
#'
plot_issue_flags <- function(data, sensor_col, y_label, sensor_type) {
  
  # Get unique site/position combos
  site_pos_combos <- data |> 
    distinct(site, position)
  
  
  # Generate a plot for each site/position combo in data
  plots <- pmap(site_pos_combos, function(site, position) {
    issue_plot_builder(data, site, position,
                       sensor_col = sensor_col,
                       y_label = y_label,
                       title_sensor = sensor_type)
  })
  
  # Name the list for easy reference
  names(plots) <- paste(site_pos_combos$site, site_pos_combos$position, sep = " - ")
  
  return(plots)
  
} # END plot_issue_flags ----
