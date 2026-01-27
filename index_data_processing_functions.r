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
#' @param metadata Metadata dataframe from read_and_clean_metadata()
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
  site_name <- str_replace_all(site_name, "(?<=[a-z])(?=[A-Z])", " ") # Make site name two words, e.g., North Beach
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
  
  # Determine deployment type (deployment, relaunch) and get flags and comments from metadata
  # Check if this is an initial deployment file
  is_initial_deployment <- FALSE
  initial_match <- metadata$initial_deployment_date[
    metadata$site == site_name &
      metadata$logger_id == logger_id &
      metadata$initial_deployment_date == as.Date(deployment_date)
  ]
  if (length(initial_match) > 0 && !all(is.na(initial_match))) {
    is_initial_deployment <- TRUE
  }
  
  # Check if this is a relaunch file
  is_relaunch <- FALSE
  relaunch_match <- metadata$relaunch_date[
    metadata$site == site_name &
      metadata$logger_id == logger_id &
      metadata$relaunch_date == as.Date(deployment_date)
  ]
  if (length(relaunch_match) > 0 && !all(is.na(relaunch_match))) {
    is_relaunch <- TRUE
  }
  
  # Extract relevant metadata columns based on deployment type
  if (is_initial_deployment) {
    issue_flag <- metadata$issue_flag_deployment[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$initial_deployment_date == as.Date(deployment_date)
    ]
    comments <- metadata$comments_initial_deployment[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$initial_deployment_date == as.Date(deployment_date)
    ]
    deployment_type <- "initial"
    
  } else if (is_relaunch) {
    issue_flag <- metadata$issue_flag_relaunch[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
    
    # For relaunch: combine relaunch and recovery comments
    comments_relaunch <- metadata$comments_relaunch[ 
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
    comments_recovery <- metadata$comments_recovery[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
    
    # Combine non-NA comments with comma separator
    comment_parts <- c()
    if (length(comments_relaunch) > 0 && !is.na(comments_relaunch[1])) {
      comment_parts <- c(comment_parts, comments_relaunch[1])
    }
    if (length(comments_recovery) > 0 && !is.na(comments_recovery[1])) {
      comment_parts <- c(comment_parts, comments_recovery[1])
    }
    
    comments <- if (length(comment_parts) > 0) {
      paste(comment_parts, collapse = ", ")
    } else {
      NA_character_
    }
    
    deployment_type <- "relaunch"
    
  } else {
    issue_flag <- NA_character_
    comments <- NA_character_
    deployment_type <- NA_character_
    warning(paste("Could not determine if file is initial deployment or relaunch for:", file_name))
  }
  
  # Handle multiple matches
  if (length(issue_flag) > 1) {
    warning(paste("Multiple issue_flags found. Using the first non-NA."))
    issue_flag <- issue_flag[!is.na(issue_flag)][1]
  }
  if (length(issue_flag) == 0) issue_flag <- NA_character_
  
  if (length(comments) > 1) {
    warning(paste("Multiple comments found. Using the first non-NA."))
    comments <- comments[!is.na(comments)][1]
  }
  if (length(comments) == 0) comments <- NA_character_
  
  print(paste("Deployment type:", deployment_type))
  print(paste("Issue flag:", issue_flag))
  print(paste("Comments:", comments))
  
  
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
      mutate(site = site_name, 
             position = position, 
             temp_logger_id = logger_id,
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
        ph_logger_id = logger_id,
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
             wl_logger_id = logger_id,
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
             con_logger_id = logger_id,
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
             do_logger_id = logger_id,
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
               par_logger_id = logger_id,
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

  return(metadata)
}  # END metadata cleaning function ----


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
      message("Check that file name and metadata are as expected")
      message(paste("File", file_row$name, "will be retried on next run"))
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
    if (!is.null(old_data) && !is.null(new_data)) {
      combined_data <- bind_rows(old_data, new_data)
    } else if (!is.null(new_data)) {
      combined_data <- new_data
    } else {
      combined_data <- old_data
    }
    
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
    
    # For gaps filled with NA, search metadata for matching issue_flags and comments
    # Create metadata lookup for this sensor
    metadata_periods <- metadata |>
      select(site, logger_id, position,
             initial_deployment_datetime, relaunch_recovery_datetime,
             relaunch_deployment_datetime, recovery_datetime,
             issue_flag_deployment, issue_flag_relaunch,
             comments_initial_deployment, comments_relaunch, comments_recovery)
    
    # Fill in metadata for gap rows
    combined_data <- combined_data |>
      rowwise() |>
      mutate(
        # If issue_flag or comments is NA, try to fill from metadata
        needs_metadata = is.na(issue_flag) | is.na(comments),
        gap_metadata = list(if (needs_metadata) {
          # Find matching deployment period using current row's site and position
          current_site <- site
          current_position <- position
          current_datetime <- datetime
          
          matching <- metadata_periods |>
            filter(
              site == current_site,
              position == current_position
            ) |>
            filter(
              # Falls within initial deployment
              (current_datetime >= initial_deployment_datetime & 
                 current_datetime <= relaunch_recovery_datetime) |
                # Falls within relaunch deployment  
                (current_datetime >= relaunch_deployment_datetime &
                   current_datetime <= recovery_datetime)
            )
          
          if (nrow(matching) > 0) {
            row <- matching[1, ]
            
            # Determine which period
            in_initial <- !is.na(row$initial_deployment_datetime) &&
              current_datetime >= row$initial_deployment_datetime &&
              (!is.na(row$relaunch_recovery_datetime) && 
                 current_datetime <= row$relaunch_recovery_datetime)
            
            if (in_initial) {
              list(
                flag = row$issue_flag_deployment,
                comment = row$comments_initial_deployment
              )
            } else {
              # Combine relaunch and recovery comments
              comment_parts <- c()
              if (!is.na(row$comments_relaunch)) {
                comment_parts <- c(comment_parts, row$comments_relaunch)
              }
              if (!is.na(row$comments_recovery)) {
                comment_parts <- c(comment_parts, row$comments_recovery)
              }
              
              list(
                flag = row$issue_flag_relaunch,
                comment = if (length(comment_parts) > 0) {
                  paste(comment_parts, collapse = ", ")
                } else {
                  NA_character_
                }
              )
            }
          } else {
            list(flag = NA_character_, comment = NA_character_)
          }
        } else {
          # Already has metadata, keep it
          list(flag = issue_flag, comment = comments)
        }
        )
      ) |>
      ungroup() |>
      mutate(
        issue_flag = sapply(gap_metadata, function(x) x$flag),
        comments = sapply(gap_metadata, function(x) x$comment)
      ) |>
      select(-needs_metadata, -gap_metadata)
    
    # Sort and clean
    combined_data <- combined_data |>
      arrange(site, position, datetime) |>
      distinct()
    
    message(paste(sensor_name, "data:", nrow(combined_data), "rows"))
    final_data[[sensor]] <- combined_data
  }
  

  # 10. Save updated data and tracking info
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
  
  message(paste("Processing complete!"))
  message(paste("Successfully processed", length(successfully_processed_files), "new files"))
  message(paste("Failed to process", nrow(files_to_process) - length(successfully_processed_files), "files"))
  
  # Print summary for each sensor type
  for (i in seq_along(sensor_types)) {
    if (!is.null(final_data[[sensor_types[i]]])) {
      message(paste(sensor_names[i], "dataset:", 
                    nrow(final_data[[sensor_types[i]]]), "rows,",
                    ncol(final_data[[sensor_types[i]]]), "columns"))
    }
  }
  
  return(final_data)
}
