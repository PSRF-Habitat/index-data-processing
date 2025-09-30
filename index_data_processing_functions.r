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


# Download and clean a logger CSV file from Google Drive ----
#'
#' @param file_row One row from get_all_logger_csvs_by_id() output
#' @param metadata Metadata dataframee from read_and_clean_metadata()
#' @return A cleaned logger CSV for one file
read_and_clean_logger_csv <- function(file_row, metadata) {
  
  # Pull out file info
  file_id <- file_row$id      # File ID for Google Drive API
  file_name <- file_row$name  # File name
  sensor_type <- tolower(file_row$sensor_type)  # Sensor type pulled from folder name, to know how to proceed processing
  sensor_type <- str_trim(sensor_type)
  
  # Grab info from file name
  parts <- str_split(file_name, "_", simplify = TRUE)
  # The first part is the site name
  site_name <- parts[1]
  site_name <- tolower(str_remove_all(site_name, "[^a-zA-Z0-9]")) # Make lowercase with no special characters
  print(paste("Site name:", site_name))
  # The second part is the logger nickname = logger_id
  logger_id <- parts[2]
  print(paste("Logger Id:", logger_id))
  # The third part is the initial deployment date or re-launch date
  deployment_date <- parts[3]
  deployment_date <- str_remove_all(deployment_date, regex(".csv", ignore_case = TRUE))  # take out the .csv ending
  print(paste("Date deployed:", deployment_date))

  # Link to metadata to pull out position in water column using clues from file name
  position <- metadata$position[metadata$site == site_name &  
                                  metadata$logger_id == logger_id &
                                  metadata$initial_deployment_date == as.Date(deployment_date)]
  # If the deployment date didn't match initial_deployment_date in metadata, try relaunch_date
  if (length(position) == 0 || all(is.na(position))) {
    position <- metadata$position[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
  } 
  
  # If there are many positions found (rare), use the first non-NA match
  if (length(position) > 1) {
    warning(paste("Multiple positions found. Using the first non-NA. Matches:", paste(position, collapse = ", ")))
    position <- position[!is.na(position)][1]
  } 
  # If there is no position found, then it is likely not noted in metadata. 
  if (length(position) == 0) {
    warning(paste("No position found, defaulting to NA"))
    position <- NA
  }
  print(paste("Position:", position))
  
  # Grab dates for filtering out-of-water time
  # Date + time logger was placed in water
  # If initial deployment file, use initial_deployment_datetime
  in_water_date <- metadata$initial_deployment_datetime[
    metadata$site == site_name &
      metadata$logger_id == logger_id &
      metadata$initial_deployment_date == as.Date(deployment_date)
  ]
  # If re-launch file, use relaunch_deployment_datetime
  if (length(in_water_date) == 0 || all(is.na(in_water_date))) {
    in_water_date <- metadata$relaunch_deployment_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
  }
  # Set NA if there is no match
  if (length(in_water_date) == 0) {
    in_water_date <- NA
    warning("No match found for date logger was placed in water. 
            May cause issues with filtering out-of-water time.")
  }
  # If there are multiple matches, use the first non-NA match
  if (length(in_water_date) > 1) {
    warning(paste("Multiple in_water_dates found found. Using the first non-NA. Matches:", paste(in_water_date, collapse = ", ")))
    in_water_date <- in_water_date[!is.na(in_water_date)][1]
  } 
  
  # Date + time logger was taken out of water
  # If initial deployment file, use relaunch_recovery_datetime
  out_of_water_date <- metadata$relaunch_recovery_datetime[
    metadata$site == site_name &
      metadata$logger_id == logger_id &
      metadata$initial_deployment_date == as.Date(deployment_date)
  ]
  # If initial deployment file has no relaunch, use recovery_datetime
  if (length(out_of_water_date) == 0 || all(is.na(out_of_water_date))) {
    out_of_water_date <- metadata$recovery_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$initial_deployment_date == as.Date(deployment_date)
    ]
  }
  # If re-launch file, use recovery_datetime
  if (length(out_of_water_date) == 0 || all(is.na(out_of_water_date))) {
    out_of_water_date <- metadata$recovery_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
  }
  # Set NA if there is no match
  if (length(out_of_water_date) == 0) {
    out_of_water_date <- NA
    warning("No match found for date logger was taken out of water. 
            May cause issues with filtering out-of-water time.")
  }
  # If there are multiple matches, use the first non-NA match
  if (length(out_of_water_date) > 1) {
    warning(paste("Multiple in_water_dates found found. Using the first non-NA. Matches:", paste(out_of_water_date, collapse = ", ")))
    out_of_water_date <- out_of_water_date[!is.na(out_of_water_date)][1]
  } 
  
  print(paste("In-water date:", in_water_date))
  print(paste("Out-of-water date:", out_of_water_date))
  
  print(paste("Reading file:", file_name))
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
      mutate(site = site_name, position = position, temp_logger_id = logger_id, .before = datetime) |>
      # Make datetime column POSIXct class
      mutate(datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
      filter(!is.na(tidbit_temp_c)) |>
      distinct()
    
    # Filtering out-of-water times 
    df <- df |>
      mutate(
        tidbit_temp_c = case_when(
          !is.na(in_water_date) & datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) & datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ tidbit_temp_c
        )
      )
    
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
        ph_logger_id = logger_id,
        position = position,
        .before = datetime
      ) |>
      mutate(datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
      distinct()
    
    df <- df |>
      mutate(
        pH = case_when(
          !is.na(in_water_date) & datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) & datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ pH
        ),
        millivolts = case_when(
          !is.na(in_water_date) & datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) & datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ millivolts
        ),
        ph_temp_c = case_when(
          !is.na(in_water_date) & datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) & datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ ph_temp_c
        )
      )
    
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
             wl_logger_id = logger_id,
             position = position,
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
      filter(!is.na(abs_pres_kpa)) |>
      distinct()
    
    df <- df |>
      mutate(
        abs_pres_kpa = case_when(
          !is.na(in_water_date) & datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) & datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ abs_pres_kpa
        ),
        wl_temp_c = case_when(
          !is.na(in_water_date) & datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) & datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ wl_temp_c
        )
      )
    
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
             con_logger_id = logger_id,
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
      filter(!is.na(high_range_microsiemens_per_cm)) |>
      distinct()
    
    # Filtering out-of-water times 
    df <- df |>
      # Set values to NA when it was out of water with a 15 minute buffer
      mutate(
        high_range_microsiemens_per_cm = case_when(
          !is.na(in_water_date) &
            datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) &
            datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ high_range_microsiemens_per_cm
        ),
        con_temp_c = case_when(
          !is.na(in_water_date) &
            datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) &
            datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ con_temp_c
        )
      )
    
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
             do_logger_id = logger_id,
             position = position,
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS", "m/d/y HM"))) |>
      filter(!is.na(do_conc_mg_per_L)) |>
      distinct()
    
    
    # Filtering out-of-water times 
    df <- df |>
      # Set values to NA when it was out of water with a 15 minute buffer
      mutate(
        do_conc_mg_per_L = case_when(
          !is.na(in_water_date) &
            datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) &
            datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ do_conc_mg_per_L
        ),
        do_temp_c = case_when(
          !is.na(in_water_date) &
            datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) &
            datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ do_temp_c
        )
      )
    
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
               par_logger_id = logger_id,
               position = position,
               .before = datetime) |>
        filter(!is.na(datetime)) |>
        distinct()
      print(paste("Rows after filtering:", nrow(df)))
    }
    
    # Filtering out-of-water times 
    df <- df |>
      # Set values to NA when it was out of water with a 15 minute buffer
      mutate(
        raw_integrating_light = case_when(
          !is.na(in_water_date) &
            datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) &
            datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ raw_integrating_light
        ),
        calibrated_integrating_light = case_when(
          !is.na(in_water_date) &
            datetime <= in_water_date + minutes(15) ~ NA_real_,
          !is.na(out_of_water_date) &
            datetime >= out_of_water_date - minutes(15) ~ NA_real_,
          TRUE ~ calibrated_integrating_light
        )
      )
    
    print(paste("Rows after filtering:", nrow(df)))
    print(paste("Finished processing", file_name))
    
  }
  
  unlink(temp_path)
  return(df)
} # END read_and_clean_logger_csv function ----


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
    rename( # "site" = "x1",
      "logger_id" = "nickname") |>
    # Make position consistently lowercase
    mutate(position = tolower(position),
           # If no logger_id in column, pull from filename
           # logger_id = if_else(is.na(logger_id), 
           #                     str_split(relaunch_file_name, "_", simplify = TRUE)[, 2],
           #                     logger_id)
    ) |>
    # Remove rows that are year separators
    filter(!(str_detect(site, "^20\\d{2}$"))) |>
    # Make site name consistently lowercase with no spaces or punctuation
    mutate(site = tolower(site),
           site = str_remove_all(site, "[^a-zA-Z0-9]")) |>
    # Dates
    mutate(launch_date_office = parse_date_time(launch_date_office,
                                                orders = c("ymd", "mdy")),
           initial_deployment_date = parse_date_time(initial_deployment_date,
                                                     orders = c("ymd", "mdy")),
           initial_deployment_time = str_remove(initial_deployment_time, "^~\\s*"),
           initial_deployment_time = parse_date_time(initial_deployment_time,
                                                     orders = c("HM", "I:M p")),
           # Combine deployment date and time
           initial_deployment_datetime = case_when(
             # if no date, datetime = NA
             is.na(initial_deployment_date) ~ as.POSIXct(NA),
             # if no time, use the date and fill 00:00:00 for time
             is.na(initial_deployment_time) ~ ymd_hms(paste(as.Date(initial_deployment_date),
                                                            "00:00:00")),
             # otherwise, combine the two 
             TRUE ~ ymd_hms(paste(as.Date(initial_deployment_date), 
                                  format(initial_deployment_time, "%H:%M:%S"))))) |>
    relocate(initial_deployment_datetime, .after = initial_deployment_time) |>
    mutate(relaunch_date = parse_date_time(relaunch_date,
                                           orders = c("ymd", "mdy"))) |>
    mutate(relaunch_recovery_time = if_else(site == "edmonds" &
                                              logger_id == "DO18" &
                                              relaunch_date == "2025-04-02",
                                            "9:30",
                                            relaunch_recovery_time),
           relaunch_recovery_time = parse_date_time(relaunch_recovery_time,
                                                    orders = c("HM", "I:M p"))) |>
    mutate( 
      # Combine relauch date and time
      relaunch_recovery_datetime = case_when(
        # if no date, datetime = NA
        is.na(relaunch_date) ~ as.POSIXct(NA),
        # if no time, use the date and fill 00:00:00 for time
        is.na(relaunch_recovery_time) ~ ymd_hms(paste(as.Date(relaunch_date),
                                                      "00:00:00")),
        # otherwise, combine the two 
        TRUE ~ ymd_hms(paste(as.Date(relaunch_date), 
                             format(relaunch_recovery_time, "%H:%M:%S")))), 
      .after = relaunch_recovery_time) |>
    mutate(
      # Relauch Deployment Datetime
      relaunch_deployment_time = str_remove(relaunch_deployment_time, "^~\\s*"),
      relaunch_deployment_time = parse_date_time(relaunch_deployment_time,
                                                 orders = c("HM", "I:M p", "I:M:S p"))) |>
    mutate(
      relaunch_deployment_datetime = case_when(
        # if no date, datetime = NA
        is.na(relaunch_date) ~ as.POSIXct(NA),
        # if no time, use the date and fill 00:00:00 for time
        is.na(relaunch_deployment_time) ~ ymd_hms(paste(as.Date(relaunch_date),
                                                        "00:00:00")),
        # otherwise, combine the two 
        TRUE ~ ymd_hms(paste(as.Date(relaunch_date), 
                             format(relaunch_deployment_time, "%H:%M:%S")))), 
      .after = relaunch_deployment_time) |>
    mutate(relaunch_data_readout_date = parse_date_time(relaunch_data_readout_date,
                                                        orders = c("ymd", "mdy"))) |>
    mutate(recovery_date = parse_date_time(recovery_date,
                                           orders = c("ymd", "mdy")),
           recovery_time = str_remove(recovery_time, "^~\\s*"),
           recovery_time = parse_date_time(recovery_time,
                                           orders = c("HM", "I:M p", "I:M:S p")),
           data_readout_date = parse_date_time(data_readout_date,
                                               orders = c("ymd", "mdy"))) |>
    mutate(
      recovery_datetime = case_when(
        # if no date, datetime = NA
        is.na(recovery_date) ~ as.POSIXct(NA),
        # if no time, use the date and fill 00:00:00 for time
        is.na(recovery_time) ~ ymd_hms(paste(as.Date(recovery_date),
                                             "00:00:00")),
        # otherwise, combine the two 
        TRUE ~ ymd_hms(paste(as.Date(recovery_date), 
                             format(recovery_time, "%H:%M:%S")))), 
      .after = recovery_time)
  # separate(file_upload_date,
  #          into = c("file_upload_date_1", "file_upload_date_2"),
  #          sep = ",") |>
  # mutate(file_upload_date_1 = parse_date_time(file_upload_date_1,
  #                                             orders = c("ymd", "mdy")),
  #        file_upload_date_2 = parse_date_time(file_upload_date_2,
  #                                             orders = c("ymd", "mdy")))
  
  return(metadata)
}  # END metadata cleaning function ----


# MAIN FUNCTION: Incremental update to processes new files ----
#'
#' Updates logger data by only processing new/changed files since last update
#'
#' @param root_folder_id The Google Drive folder ID containing sensor subfolders
#' @param metadata_file_url URL to main index metadata google sheet  
#' @param sheet_name Name of the tab that the metadata is on
#' @param existing_data_file Path to existing combined RDS data file
#' @param tracking_file Path to file tracking what's been processed
#' @param force_reprocess Logical, whether to reprocess all files (default FALSE). TRUE if you want to process all files in drive
#' @return Updated combined logger data
#'
update_logger_data_incremental <- function(root_folder_id, metadata_file_url, sheet_name,
                                           existing_data_file = "logger_data.rds",
                                           tracking_file = "processed_files.rds",
                                           force_reprocess = FALSE) {
  
  message("Starting incremental logger data update...")
  
  # 1. Load existing data if it exists
  existing_data <- NULL
  data_file_path <- here::here("data", existing_data_file)
  if (file.exists(data_file_path) && !force_reprocess) {
    message("Loading existing data...")
    existing_data <- readRDS(data_file_path)
    message(paste("Existing data has", nrow(existing_data), "rows"))
  }
  
  # 2. Get list of previously processed files
  processed_files <- character(0)
  tracking_file_path <- here::here("data", tracking_file)
  if (file.exists(tracking_file_path) && !force_reprocess) {
    processed_files <- readRDS(tracking_file_path)
    message(paste("Found", length(processed_files), "previously processed files"))
  }
  
  # 3. Get current file list from Drive
  message("Scanning Google Drive for files...")
  all_files <- get_all_logger_csvs_by_id(root_folder_id)
  
  if (nrow(all_files) == 0) {
    warning("No CSV files found in Drive folder")
    return(existing_data)
  }
  
  # 4. Identify new files to process
  if (force_reprocess) {
    files_to_process <- all_files # Process them all if we are reprocessing
    message(paste("Force reprocess: will process all", nrow(files_to_process), "files"))
  } else {
    files_to_process <- all_files |>
      filter(!(name %in% processed_files))  # Only process files whos name is not in file tracker
    message(paste("Found", nrow(files_to_process), "new files to process"))
  }
  
  # 5. If no new files, return existing data
  if (nrow(files_to_process) == 0) {
    message("No new files to process. Data is up to date! Horray!")
    return(existing_data)
  }
  
  # 6. Load metadata (always refresh this in case of updates)
  message("Loading metadata...")
  metadata <- read_and_clean_metadata(metadata_file_url, sheet_name)
  
  # 7. Process new files only
  message(paste("Processing", nrow(files_to_process), "new files..."))
  
  # Initialize lists for new data by sensor type
  new_temp_data <- list()
  new_ph_data <- list()
  new_do_data <- list()
  new_wl_data <- list()
  new_par_data <- list()
  new_con_data <- list()
  
  # Track successfully processed files
  successfully_processed_files <- character(0)
  
  # Process each new file
  for (i in seq_len(nrow(files_to_process))) {
    file_row <- files_to_process[i, ]
    sensor_type <- tolower(file_row$sensor_type)
    sensor_type <- str_trim(sensor_type)
    
    message(paste("Processing new file", i, "of", nrow(files_to_process), ":", file_row$name))
    
    tryCatch({  # trying this new way of throwing warnings/errors
      cleaned_data <- read_and_clean_logger_csv(file_row, metadata)
      
      # Store in appropriate list
      if (sensor_type == "temperature") {
        new_temp_data <- append(new_temp_data, list(cleaned_data))
      } else if (sensor_type == "ph") {
        new_ph_data <- append(new_ph_data, list(cleaned_data))
      } else if (sensor_type == "dissolved oxygen") {
        new_do_data <- append(new_do_data, list(cleaned_data))
      } else if (sensor_type == "water level") {
        new_wl_data <- append(new_wl_data, list(cleaned_data))
      } else if (sensor_type == "par") {
        new_par_data <- append(new_par_data, list(cleaned_data))
      } else if (sensor_type == "conductivity") {
        new_con_data <- append(new_con_data, list(cleaned_data))
      }
      
      # Only add to successfully processed list if we get here without error
      successfully_processed_files <- c(successfully_processed_files, file_row$name)
      message(paste("Successfully processed:", file_row$name))
      
    }, error = function(e) {
      warning(paste("Error processing file", file_row$name, ":", e$message))
      message(paste("File", file_row$name, "will be retried on next run"))
    })
  }
  
  # 8. Combine new data by sensor type
  new_df_temp <- if (length(new_temp_data) > 0) bind_rows(new_temp_data) else NULL
  new_df_ph <- if (length(new_ph_data) > 0) bind_rows(new_ph_data) else NULL
  new_df_do <- if (length(new_do_data) > 0) bind_rows(new_do_data) else NULL
  new_df_wl <- if (length(new_wl_data) > 0) bind_rows(new_wl_data) else NULL
  new_df_par <- if (length(new_par_data) > 0) bind_rows(new_par_data) else NULL
  new_df_con <- if (length(new_con_data) > 0) bind_rows(new_con_data) else NULL
  
  # 9. Combine with existing data
  if (!is.null(existing_data)) {
    message("Merging new data with existing data...")
    
    # Combine new data into single dataframe first
    new_data_combined <- NULL
    join_keys <- c("site", "datetime", "position")
    
    if (!is.null(new_df_temp)) {
      new_data_combined <- new_df_temp
      message("Added temperature data")
    }
    
    if (!is.null(new_df_ph)) {
      if (is.null(new_data_combined)) {
        new_data_combined <- new_df_ph
        message("Starting with pH data")
      } else {
        new_data_combined <- new_data_combined |> full_join(new_df_ph, by = join_keys)
        message("Added pH data")
      }
    }
    
    if (!is.null(new_df_do)) {
      if (is.null(new_data_combined)) {
        new_data_combined <- new_df_do
        message("Starting with DO data")
      } else {
        new_data_combined <- new_data_combined |> full_join(new_df_do, by = join_keys)
        message("Added DO data")
      }
    }
    
    if (!is.null(new_df_wl)) {
      if (is.null(new_data_combined)) {
        new_data_combined <- new_df_wl
        message("Starting with water level data")
      } else {
        new_data_combined <- new_data_combined |> full_join(new_df_wl, by = join_keys)
        message("Added water level data")
      }
    }
    
    if (!is.null(new_df_par)) {
      if (is.null(new_data_combined)) {
        new_data_combined <- new_df_par
        message("Starting with PAR data")
      } else {
        new_data_combined <- new_data_combined |> full_join(new_df_par, by = join_keys)
        message("Added PAR data")
      }
    }
    
    if (!is.null(new_df_con)) {
      if (is.null(new_data_combined)) {
        new_data_combined <- new_df_con
        message("Starting with conductivity data")
      } else {
        new_data_combined <- new_data_combined |> full_join(new_df_con, by = join_keys)
        message("Added conductivity data")
      }
    }
    
    # Combine existing data with new data
    all_data <- bind_rows(existing_data, new_data_combined)
    
  } else {
    # No existing data, combine new data only
    message("Creating initial dataset from new data...")
    
    all_data <- NULL
    join_keys <- c("site", "datetime", "position")
    
    if (!is.null(new_df_temp)) {
      all_data <- new_df_temp
      message("Starting with temperature data")
    }
    
    if (!is.null(new_df_ph)) {
      if (is.null(all_data)) {
        all_data <- new_df_ph
        message("Starting with pH data")
      } else {
        all_data <- all_data |> full_join(new_df_ph, by = join_keys)
        message("Added pH data")
      }
    }
    
    if (!is.null(new_df_do)) {
      if (is.null(all_data)) {
        all_data <- new_df_do
        message("Starting with DO data")
      } else {
        all_data <- all_data |> full_join(new_df_do, by = join_keys)
        message("Added DO data")
      }
    }
    
    if (!is.null(new_df_wl)) {
      if (is.null(all_data)) {
        all_data <- new_df_wl
        message("Starting with water level data")
      } else {
        all_data <- all_data |> full_join(new_df_wl, by = join_keys)
        message("Added water level data")
      }
    }
    
    if (!is.null(new_df_par)) {
      if (is.null(all_data)) {
        all_data <- new_df_par
        message("Starting with PAR data")
      } else {
        all_data <- all_data |> full_join(new_df_par, by = join_keys)
        message("Added PAR data")
      }
    }
    
    if (!is.null(new_df_con)) {
      if (is.null(all_data)) {
        all_data <- new_df_con
        message("Starting with conductivity data")
      } else {
        all_data <- all_data |> full_join(new_df_con, by = join_keys)
        message("Added conductivity data")
      }
    }
  }
  
  if (is.null(all_data)) {
    warning("No data was successfully processed")
    return(existing_data)
  }
  
  # Filter to keep only 15-minute interval timestamps
  # all_data <- all_data |>
  #   filter(minute(datetime) %in% c(0, 15, 30, 45) & second(datetime) == 0)
  
  # Round all timestamps to nearest 15 minutes and handle duplicates
  all_data <- all_data |>
    mutate(datetime = round_date(datetime, "15 minutes")) |>
    # Group by all key columns and aggregate duplicates
    group_by(site, position, datetime) |>
    summarise(across(everything(), ~ first(na.omit(.x))[1]), .groups = "drop")
 
  if (!is.null(all_data)) {
    message("Creating complete 15-minute time series...")
    all_data <- all_data |>
      group_by(site, position) |>
      complete(datetime = seq(min(datetime, na.rm = TRUE), 
                              max(datetime, na.rm = TRUE), 
                              by = "15 mins")) |>
      ungroup() |>
      # Remove any remaining duplicates
      group_by(site, position, datetime) |>
      summarise(across(everything(), ~ first(na.omit(.x))[1]), .groups = "drop")
  }

  # Sort final dataset
  all_data <- all_data |>
    arrange(site, position, datetime) |>
    distinct()  # Remove any duplicate lines
  
  # And arrange columns
  expected_cols <- c("site", "position", "datetime",
                     "ph_logger_id", "pH", "millivolts",
                     "do_logger_id", "do_conc_mg_per_L",
                     "wl_logger_id", "abs_pres_kpa",
                     "con_logger_id", "high_range_microsiemens_per_cm",
                     "par_logger_id", "raw_integrating_light", "calibrated_integrating_light",
                     "temp_logger_id", "tidbit_temp_c",
                     "ph_temp_c",  "do_temp_c",  "wl_temp_c",  "con_temp_c")
  
  available_cols <- expected_cols[expected_cols %in% names(all_data)]
  
  missing_cols <- setdiff(expected_cols, names(all_data))
  if (length(missing_cols) > 0) {
    warning("Missing columns in all_data: ", paste(missing_cols, collapse = ", "))
  }
  
  all_data <- all_data[, available_cols]
  
  # 10. Save updated data and tracking info
  message("Saving updated data...")
  
  # Create data folder if it doesn't exist
  data_dir <- here::here("data")
  if (!dir.exists(data_dir)) {
    dir.create(data_dir)
  }
  
  # Save to data folder
  data_file_path <- here::here("data", existing_data_file)
  tracking_file_path <- here::here("data", tracking_file)
  
  saveRDS(all_data, data_file_path)
  
  # Update processed files list - ONLY with successfully processed files
  updated_processed_files <- unique(c(processed_files, successfully_processed_files))
  saveRDS(updated_processed_files, tracking_file_path)
  
  message(paste("Processing complete!"))
  message(paste("Final dataset has", nrow(all_data), "rows and", ncol(all_data), "columns"))
  message(paste("Successfully processed", length(successfully_processed_files), "new files"))
  message(paste("Failed to process", nrow(files_to_process) - length(successfully_processed_files), "files"))
  message(paste("Data saved to:", data_file_path))
  
  return(all_data)
}
