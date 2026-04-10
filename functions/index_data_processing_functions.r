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
#'
#' @param metadata_file_url URL to main index metadata google sheet
#' @param sheet_name Name of the tab that the metadata is on
#'
#' @returns A clean metadata sheet to be combined with logger data
#'
read_and_clean_metadata <- function(metadata_file_url, sheet_name){
  
  # Read in metadata sheet from google drive
  metadata_raw <- read_sheet(metadata_file_url,
                             sheet = sheet_name,
                             na = c("", "n/a", "#N/A"),
                             col_types = "c")
  
  # Cleaning
  metadata <- metadata_raw |>
    clean_names() |>
    # Rename nickname to logger_id to match logger file convention
    rename("logger_id" = "nickname") |>
    # Make position consistently lowercase
    mutate(position = tolower(position)) |>
    # Remove rows that are year separators
    filter(!(str_detect(site, "^20\\d{2}$"))) |>
    # Make site name capital with space. e.g., North Beach
    mutate(site = str_replace_all(site, "(?<=[a-z])(?=[A-Z])", " ")) |>
    # Parse dates from varying formats
    mutate(initial_deployment_date = 
             parse_date_time(initial_deployment_date,
                             orders = c("ymd", "mdy")),
           relaunch_date =
             parse_date_time(relaunch_date,
                             orders = c("ymd", "mdy")),
           recovery_date = 
             parse_date_time(recovery_date,
                             orders = c("ymd", "mdy"))) |>
    # Select columns to keep
    select(c(site, position, logger_id, sn, initial_deployment_date,
             relaunch_date, recovery_date, issue_flag_deployment,
             issue_flag_relaunch, comments_for_initial_deployment,
             comments_for_relaunch_deployment, comments_recovery)) |>
    rename("serial_number" = "sn")
  
  # Make initial deployment file
  initial <- metadata |>
    mutate(deployment_type = "initial",
           in_water_date = initial_deployment_date,
           # out of water date is either relaunch date, 
           # or if there was no relaunch, it is recovery date.
           # coalesce() finds the first non-missing value at each position
           out_of_water_date = coalesce(relaunch_date, recovery_date),
           issue_flag = issue_flag_deployment,
           comments = comments_for_initial_deployment) |>
    select(site, position, logger_id, serial_number,
           deployment_type, in_water_date, out_of_water_date,
           issue_flag, comments) |>
    # make sure dates are dates
    mutate(out_of_water_date = as.Date(out_of_water_date),
           in_water_date = as.Date(in_water_date))
  
  # Make relaunch deployment file
  relaunch <- metadata |>
    # Remove rows when sensor was not relaunched
    filter(!is.na(relaunch_date)) |>
    mutate(deployment_type = "relaunch",
           in_water_date = relaunch_date,
           out_of_water_date = recovery_date,
           issue_flag = issue_flag_relaunch,
           comments = case_when(
             !is.na(comments_for_relaunch_deployment) & 
               !is.na(comments_recovery) ~
               paste(comments_for_relaunch_deployment,
                     comments_recovery, sep = ", "),
             !is.na(comments_for_relaunch_deployment) ~
               comments_for_relaunch_deployment,
             !is.na(comments_recovery) ~
               comments_recovery,
             TRUE ~ NA_character_
           )) |>
    select(site, position, logger_id, serial_number,
           deployment_type, in_water_date, out_of_water_date,
           issue_flag, comments) |>
    # make sure dates are dates
    mutate(out_of_water_date = as.Date(out_of_water_date),
           in_water_date = as.Date(in_water_date))
  
  # Combine initial and relaunch files
  metadata_long <- bind_rows(initial, relaunch) |>
    arrange(site, position, in_water_date, logger_id)
  
  # Add logger type column
  metadata_long <- metadata_long |>
    mutate(logger_type = case_when(
      grepl("^T\\d", logger_id) ~ "temperature",
      grepl("^WLBT|^WL|^TP", logger_id) ~ "water level",
      logger_id == "AIR" ~ "water level",
      grepl("^CON", logger_id) ~ "conductivity",
      grepl("^DO", logger_id) ~ "dissolved oxygen",
      grepl("^pH", logger_id) ~ "ph",
      grepl("^PAR|^PSRF|^DNR", logger_id) ~ "par",
      .default = NA
    ))
  
  return(metadata_long)
}  # END metadata cleaning function ----


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
  
  message("Running pre-check")
  
  # Get all files from Drive
  message("Scanning Google Drive for files...")
  all_files <- get_all_logger_csvs_by_id(root_folder_id)
  message(paste("Found", nrow(all_files), "total files in Drive"))
  
  # Load metadata
  message("Loading metadata...")
  metadata <- read_and_clean_metadata(metadata_file_url, sheet_name)
  
  # Check for missing deployment dates in metadata
  missing_dates <- metadata |>
    filter(is.na(in_water_date) | is.na(out_of_water_date)) # |>
   # select(site, position, logger_id, logger_type, in_water_date, out_of_water_date)
  
  if (nrow(missing_dates) > 0) {
    message(paste(nrow(missing_dates), "metadata row(s) have missing in_water_date or out_of_water_date."))
  } else {
    message("All metadata rows have valid deployment dates.")
  }
  
  # Parse file name components for all files (same logic as read_and_clean_logger_csv)
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
    message("All files have exactly one metadata match. Good to go!")
  } else {
    message(paste(nrow(unmatched), "file(s) will not be processed:"))
    message(paste0(
      "\n  - no match:          ", sum(unmatched$match_status == "no match"),
      "\n  - multiple matches:  ", sum(unmatched$match_status == "multiple matches")
    ))
    message("Fix these in the metadata sheet before running the pipeline.")
  }
  
  return(list(
    missing_dates = missing_dates,
    unmatched = unmatched
  ))
  
} # END prerun check function ----


# Read and clean a logger CSV file from Google Drive ----
#'
#' @param file_row One row from get_all_logger_csvs_by_id() output
#' @param metadata Metadata dataframe from read_and_clean_metadata()
#' @return A cleaned logger CSV for one file
read_and_clean_logger_csv <- function(file_row, metadata) {
  
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
  
  print(paste("Site:", site_name,
              " | Logger ID:", file_logger_id,
              " | Date deployed:", deployment_date))
  
  
  # Join logger file to metadata row
  meta_row <- metadata |>
    filter(site == site_name,
           logger_id == file_logger_id,
           as.Date(in_water_date) == deployment_date)
  
  # Warning if no match in metadata
  if (nrow(meta_row) == 0) {
    warning(paste("No metadata match for file:", file_name,
                  "- check site name, logger_id, and deployment date in metadata sheet"))
    return(NULL)
  }
  
  # Warning if multiple matches
  if (nrow(meta_row) > 1) {
    warning(paste("Multiple metadata matches for:", file_name, "- using first row. Check file name and metadata sheet"))
    meta_row <- meta_row[1, ]
  }
  
  # Pull out values from matched metadata row
  position <- meta_row$position
  deployment_type <- meta_row$deployment_type
  in_water_date <- meta_row$in_water_date
  out_of_water_date <- meta_row$out_of_water_date
  issue_flag <- meta_row$issue_flag
  comments <- meta_row$comments
  
  print(paste("Position:", position, "| Type:", deployment_type,
              "| In:", in_water_date, "| Out:", out_of_water_date))
  print(paste("Issue flag:", issue_flag, "| Comments:", comments))
  print(paste("Sensor type:", sensor_type))
  
  # Download csv to temp file
  temp_path <- tempfile(fileext = ".csv")
  if (length(file_id) != 1) {     # Error if there are duplicate files
    stop("drive_download() aborted: file_id is not unique.")
  }
  drive_download(as_id(file_id), path = temp_path, overwrite = TRUE)
  print(paste("File downloaded to:", temp_path))
  
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
      print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    print(paste("Skipping", skip_n, "lines"))
    
    # Read in the csv
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
    
    # Check that the header row was correctly read in
    print("Columns detected:")
    print(names(df))
    
    # Only keep the rows with actual data
    df <- df |>
      select(contains("date", ignore.case = TRUE),
             contains("temp", ignore.case = TRUE))
    # Check that the columns kept were correct
    print("Columns selected:")
    print(names(df))
    
    df <- df |>
      # Rename selected columns
      setNames(c("datetime", "tidbit_temp_c")) |>
      mutate(site = site_name, 
             position = position, 
             logger_id = file_logger_id,
             logger_type = "temperature",
             issue_flag = issue_flag,
             comments = comments,
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
    
    print(paste("Rows after filtering:", nrow(df)))
    print(paste("Finished processing", file_name))
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
      print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    print(paste("Skipping", skip_n, "lines"))
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
    print("Columns detected:")
    print(names(df))
    
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
    
    print("Columns selected:")
    print(names(selected_cols))
    
    df <- selected_cols |>
      setNames(c("datetime", "ph_temp_c", "millivolts", if (has_ph_col) "pH")) |>
      mutate(
        pH = if (has_ph_col) pH else NA_real_,
        site = site_name,
        logger_id = file_logger_id,
        logger_type = "ph",
        position = position,
        issue_flag = issue_flag,
        comments = comments,
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
    
    print(paste("Rows after filtering:", nrow(df)))
    print(paste("Finished processing", file_name))
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
      print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    print(paste("Skipping", skip_n, "lines"))
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE)) |>
      clean_names()
    
    print("Columns detected:")
    print(names(df))
    
    pres_col <- names(df)[str_detect(names(df), "pres")][1]
    datetime_col <- names(df)[str_detect(names(df), "date")][1]
    temp_col <- names(df)[str_detect(names(df), "temp")][1]
    
    print(paste("Pressure column:", pres_col))
    print(paste("Datetime column:", datetime_col))
    print(paste("Temp column:", temp_col))
    
    unit <- case_when(
      str_detect(pres_col, "psi") ~ "psi",
      str_detect(pres_col, "k.?pa") ~ "kpa",
      TRUE ~ NA_character_
    )
    
    print(paste("Detected unit:", unit))
    
    df <- df |>
      select(datetime = all_of(datetime_col),
             wl_temp_c = all_of(temp_col),
             abs_pres = all_of(pres_col)) |>
      mutate(abs_pres_kpa = case_when(
        unit == "psi" ~ as.numeric(abs_pres) * 6.89476,
        unit == "kpa" ~ as.numeric(abs_pres),
        TRUE ~ NA_real_)) |>
      select(-abs_pres)
    
    print("Columns selected:")
    print(names(df))
    print(paste("Rows before filtering:", nrow(df)))
    
    df <- df |>
      mutate(site = site_name,
             logger_id = file_logger_id,
             logger_type = "water level",
             position = position,
             issue_flag = issue_flag,
             comments = comments,
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
    
    print(paste("Rows after filtering:", nrow(df)))
    print(paste("Finished processing", file_name))
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
      print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    print(paste("Skipping", skip_n, "lines"))
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
    
    print("Columns detected:")
    print(names(df))
    
    df <- df |>
      select(contains("date", ignore.case = TRUE),
             contains("temp", ignore.case = TRUE),
             contains("range", ignore.case = TRUE))
    
    print("Columns selected:")
    print(names(df))
    
    df <- df |>
      setNames(c("datetime", 
                 "con_temp_c",
                 "high_range_microsiemens_per_cm")) |>
      mutate(site = site_name,
             position = position,
             logger_id = file_logger_id,
             logger_type = "conductivity",
             issue_flag = issue_flag,
             comments = comments,
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
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ high_range_microsiemens_per_cm
        ),
        con_temp_c = case_when(
          !is.na(in_water_date) & date <= as.Date(in_water_date) ~ NA_real_,
          !is.na(out_of_water_date) & date >= as.Date(out_of_water_date) ~ NA_real_,
          TRUE ~ con_temp_c
        )
      ) |>
      select(-date)
    
    print(paste("Rows after filtering:", nrow(df)))
    print(paste("Finished processing", file_name))
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
      print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    print(paste("Skipping", skip_n, "lines"))
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
    
    print("Columns detected:")
    print(names(df))
    
    df <- df |>
      select(contains("date", ignore.case = TRUE),
             contains("temp", ignore.case = TRUE),
             contains("mg/l", ignore.case = TRUE))
    
    print("Columns selected:")
    print(names(df))
    
    df <- df |>
      setNames(c("datetime", "do_temp_c", "do_conc_mg_per_L")) |>
      mutate(site = site_name,
             logger_id = file_logger_id,
             logger_type = "dissolved oxygen",
             position = position,
             issue_flag = issue_flag,
             comments = comments,
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
    
    print(paste("Rows after filtering:", nrow(df)))
    print(paste("Finished processing", file_name))
  }
  
  ##### PAR LOGGER #####
  else if (sensor_type == "par") {
    
    # Skim the start of the file
    lines <- readLines(temp_path, n = 20)
    # Look for the start of the actual data
    data_start <- which(str_detect(lines, "^1,"))[1]
    
    # Start at line 10 if search does not work
    if (is.na(data_start)) {
      print(paste("Could not detect data start in", file_name, "- defaulting to line 10"))
      data_start <- 10
    }
    
    # Read in the temp file starting at the data start
    df <- suppressWarnings(read_csv(temp_path, skip = data_start - 1, col_names = FALSE, show_col_types = FALSE))
    
    # Add in the column names, and a warning if there is something missing
    if (ncol(df) < 5) {
      print(paste("Unexpected number of columns in PAR file:", file_name))
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
               issue_flag = issue_flag,
               comments = comments,
               .before = datetime) |>
        filter(!is.na(datetime)) |>
        distinct()
      print(paste("Rows after filtering:", nrow(df)))
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
    
    print(paste("Rows after filtering:", nrow(df)))
    print(paste("Finished processing", file_name))
    
  }
  
  unlink(temp_path)
  return(df)
} # END read_and_clean_logger_csv function ----


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
                                           force_reprocess = FALSE) {
  
  message("Starting incremental logger data update...")
  
  # ===== 1. Load associated info from file name and metadata ===== #
  
  # Define sensor types and their file names
  sensor_types <- c("temp", "ph", "do", "wl", "par", "con")
  sensor_names <- c("temperature", "pH", "dissolved_oxygen", "water_level", "PAR", "conductivity")
  
  # Load existing data if it exists
  existing_data <- list()
  data_dir <- here::here("data")
  if (!force_reprocess) {
    for (i in seq_along(sensor_types)) {
      sensor_file <- paste0(existing_data_prefix, "_", sensor_types[i], ".rds")
      data_file_path <- file.path(data_dir, sensor_file)
      
      if (file.exists(data_file_path)) {
        message(paste("Loading existing", sensor_names[i], "data..."))
        existing_data[[sensor_types[i]]] <- readRDS(data_file_path)
        message(paste("Existing", sensor_names[i], "data has", 
                      nrow(existing_data[[sensor_types[i]]]), "rows"))
      }
    }
  }
  
  # Get list of previously processed files
  processed_files <- character(0)
  tracking_file_path <- here::here("data", tracking_file)
  if (file.exists(tracking_file_path) && !force_reprocess) {
    processed_files <- readRDS(tracking_file_path)
    message(paste("Found", length(processed_files), "previously processed files"))
  }
  
  # Get current file list from Drive
  message("Scanning Google Drive for files...")
  all_files <- get_all_logger_csvs_by_id(root_folder_id)
  
  if (nrow(all_files) == 0) {
    warning("No CSV files found in Drive folder")
    return(existing_data)
  }
  
  # Identify new files to process
  if (force_reprocess) {
    files_to_process <- all_files # Process them all if we are reprocessing
    message(paste("Force reprocess: will process all", nrow(files_to_process), "files"))
  } else {
    files_to_process <- all_files |>
      filter(!(name %in% processed_files))  # Only process files whos name is not in file tracker
    message(paste("Found", nrow(files_to_process), "new files to process"))
  }
  
  # If no new files, return existing data
  if (nrow(files_to_process) == 0) {
    message("No new files to process. Data is up to date! Horray!")
    return(existing_data)
  }
  
  # Load metadata (always refresh this in case of updates)
  message("Loading metadata...")
  metadata <- read_and_clean_metadata(metadata_file_url, sheet_name)
  
  # ===== 2. Process new files ===== #
  message(paste("Processing", nrow(files_to_process), "new files..."))
  
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
    
    message(paste("Processing new file", i, "of", nrow(files_to_process), ":", file_row$name))
    
    tryCatch({ # trying this new way of throwing warnings/errors
      cleaned_data <- read_and_clean_logger_csv(file_row, metadata)
      
      if (is.null(cleaned_data)) {
        message(paste("Skipping file (no metadata match):", file_row$name,
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
        message(paste("Successfully processed:", file_row$name))
      }
      
    }, error = function(e) {
      warning(paste("Error processing file", file_row$name, ":", e$message))
      message("Check that file name and metadata are as expected")
      message(paste("File", file_row$name, "will be retried on next run"))
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
  
  final_data <- list()
  
  # Loop through each sensor type
  for (i in seq_along(sensor_types)) {
    sensor <- sensor_types[i]
    sensor_name <- sensor_names[i]
    
    # Get new and existing data for this sensor
    new_data <- get(paste0("new_df_", sensor))
    old_data <- existing_data[[sensor]]
    
    # Skip if no data
    if (is.null(new_data) && is.null(old_data)) {
      message(paste("No", sensor_name, "data to process"))
      final_data[[sensor]] <- NULL
      next
    }
    
    message(paste("Processing", sensor_name, "data..."))
    
    # Combine new and existing data
    combined_data <- bind_rows(old_data, new_data)
    
    
    # Round timestamps to nearest 15 minutes and handle duplicates
    combined_data <- combined_data |>
      mutate(datetime = round_date(datetime, "15 minutes")) |>
      group_by(site, position, datetime) |>
      summarise(across(everything(), ~ first(na.omit(.x))[1]), .groups = "drop")
    
    # Create complete 15-minute time series
    combined_data <- combined_data |>
      group_by(site, position) |>
      complete(datetime = seq(min(datetime, na.rm = TRUE), 
                              max(datetime, na.rm = TRUE), 
                              by = "15 mins")) |>
      ungroup() |>
      group_by(site, position, datetime) |>
      summarise(across(everything(), ~ first(na.omit(.x))[1]), .groups = "drop")
    
    # Fill issue_flag and comments for rows with missing data
    # This section will match each row with missing data to the
    # corresponding deployment period in the metadata
    meta_lookup <- metadata |>
      # Select just columns needed for matching and filtering
      select(site, position, logger_type, 
             in_water_date, out_of_water_date, issue_flag, comments) |>
      # Make dates POSIXct to match sensor data type
      mutate(in_water_date = as.POSIXct(in_water_date),
             out_of_water_date = as.POSIXct(out_of_water_date))
    
    
    combined_data <- combined_data |>
      left_join(meta_lookup |> 
                  rename(flag_fill = issue_flag, 
                         comments_fill = comments),
                join_by(site, position, logger_type,
                        datetime > in_water_date,
                        datetime < out_of_water_date)) |>
      mutate(issue_flag = coalesce(flag_fill, issue_flag),
             comments = coalesce(comments_fill, comments)) |>
      select(-flag_fill, -comments_fill, -in_water_date, -out_of_water_date) |>
      arrange(site, position, datetime) |>
      distinct()
    
    message(paste(sensor_name, "data:", nrow(combined_data), "rows"))
    final_data[[sensor]] <- combined_data
  }
  
  
  # 4. Save updated data and tracking info
  message("Saving updated data...")
  
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
      message(paste("Saved", sensor_names[which(sensor_types == sensor)], 
                    "data to:", data_file_path))
    }
  }
  
  # Update processed files list - ONLY with successfully processed files
  updated_processed_files <- unique(c(processed_files, successfully_processed_files))
  saveRDS(updated_processed_files, tracking_file_path)
  
  # Save unsuccessful files list for review 
  unsuccessful_file_path <- here::here("data", "unsuccessful_files.rds")
  saveRDS(unsuccessfully_processed_files, unsuccessful_file_path)
  
  message(paste("Processing complete!"))
  message(paste("Successfully processed", length(successfully_processed_files), "new files"))
  message(paste("Failed to process", nrow(files_to_process) - length(successfully_processed_files), "files"))
  
  # Print unsuccessful files for review
  if (length(unsuccessfully_processed_files) > 0) {
    message("The following files were not processed and need review:")
    for (f in unsuccessfully_processed_files) message(paste(" -", f))
  }
  
  # Print summary for each sensor type
  for (i in seq_along(sensor_types)) {
    if (!is.null(final_data[[sensor_types[i]]])) {
      message(paste(sensor_names[i], "dataset:", 
                    nrow(final_data[[sensor_types[i]]]), "rows,",
                    ncol(final_data[[sensor_types[i]]]), "columns"))
    }
  }
  
  return(final_data)
} # END update_logger_data_incremental ----
