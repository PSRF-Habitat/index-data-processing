########## PROCESSING INDEX SITE LOGGER DATA ##########

# List all .csv files from a Google Drive folder by ID ----
#'
#' @param root_folder_id The ID of the folder containing sensor subfolders (e.g., "TEMP", "PH")
#' @return A data frame of all .csv files found, with an added `sensor_type` column inhereted from subfolder name
#'
get_all_logger_csvs_by_id <- function(root_folder_id = "1k5u8iOhR5alnymc7BVU-JJjcSnQStgWv") {
  # Create a list to store drive's metadata for all .csv files found
  all_files <- list()
  
  # This function will look into each folder (TEMP, PH, etc.) and collect csvs
  crawl_folder <- function(folder_id) {
    # List all items in the current folder
    items <- drive_ls(as_id(folder_id))
    
    # Only keep subfolders in csvfiles folder (eg, TEMP, CON, PH...)
    # mimeType == "application/vnd.google-apps.folder" is how Google marks folders in its API
    subfolders <- items |>
      filter(drive_resource[[1]]$mimeType == "application/vnd.google-apps.folder")
    
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
  
  # Pull out file info ----
  file_id <- file_row$id      # File ID for Google Drive API
  file_name <- file_row$name  # File name
  sensor_type <- tolower(file_row$sensor_type)  # Sensor type, to know how to proceed processing
  
  # Grab info from file name
  parts <- str_split(file_name, "_", simplify = TRUE)
  # The first part is the site name
  site_name <- parts[1]
  site_name <- tolower(str_remove_all(site_name, "[^a-zA-Z0-9]")) # Make lowercase with no special characters
  # Normalize Squaxin Island
  if(site_name == "squaxinisland"){
    site_name = "squaxin"
  }
  print(paste("Site name:", site_name))
  # The second part is the logger nickname = logger_id
  logger_id <- parts[2]
  print(paste("Logger Id:", logger_id))
  # The third part is the deployment date (??? seems inconsistent, check with team!!) ----
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
  # If the deployment date didn't match relaunch_date in metadata, try launch_date_office
  if (length(position) == 0 || all(is.na(position))) {
    position <- metadata$position[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$launch_date_office == as.Date(deployment_date)
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
  
  # Grab dates for filtering out-of-water time ----
  # Initial deployment date
  initial_deployment_date <- metadata$initial_deployment_datetime[
    metadata$site == site_name &
      metadata$logger_id == logger_id &
      metadata$initial_deployment_date == as.Date(deployment_date)
  ]
  if (length(initial_deployment_date) == 0 || all(is.na(initial_deployment_date))) {
    initial_deployment_date <- metadata$initial_deployment_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
  }
  if (length(initial_deployment_date) == 0 || all(is.na(initial_deployment_date))) {
    initial_deployment_date <- metadata$initial_deployment_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$launch_date_office == as.Date(deployment_date)
    ]
  }
  
  # Relaunch recovery date
  relaunch_recovery_date <- metadata$relaunch_recovery_datetime[
    metadata$site == site_name &
      metadata$logger_id == logger_id &
      metadata$initial_deployment_date == as.Date(deployment_date)
  ]
  if (length(relaunch_recovery_date) == 0 || all(is.na(relaunch_recovery_date))) {
    relaunch_recovery_date <- metadata$relaunch_recovery_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
  }
  if (length(relaunch_recovery_date) == 0 || all(is.na(relaunch_recovery_date))) {
    relaunch_recovery_date <- metadata$relaunch_recovery_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$launch_date_office == as.Date(deployment_date)
    ]
  }
  
  # Relaunch deployment date
  relaunch_deployment_date <- metadata$relaunch_deployment_datetime[
    metadata$site == site_name &
      metadata$logger_id == logger_id &
      metadata$initial_deployment_date == as.Date(deployment_date)
  ]
  if (length(relaunch_deployment_date) == 0 || all(is.na(relaunch_deployment_date))) {
    relaunch_deployment_date <- metadata$relaunch_deployment_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
  }
  if (length(relaunch_deployment_date) == 0 || all(is.na(relaunch_deployment_date))) {
    relaunch_deployment_date <- metadata$relaunch_deployment_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$launch_date_office == as.Date(deployment_date)
    ]
  }
  
  # Recovery date
  recovery_date <- metadata$recovery_datetime[
    metadata$site == site_name &
      metadata$logger_id == logger_id &
      metadata$initial_deployment_date == as.Date(deployment_date)
  ]
  if (length(recovery_date) == 0 || all(is.na(recovery_date))) {
    recovery_date <- metadata$recovery_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
  }
  if (length(recovery_date) == 0 || all(is.na(recovery_date))) {
    recovery_date <- metadata$recovery_datetime[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$launch_date_office == as.Date(deployment_date)
    ]
  }
  
  # Set NA for missing dates
  if (length(relaunch_recovery_date) == 0) relaunch_recovery_date <- NA
  if (length(relaunch_deployment_date) == 0) relaunch_deployment_date <- NA
  if (length(recovery_date) == 0) recovery_date <- NA
  if (length(initial_deployment_date) == 0) initial_deployment_date <- NA
  
  print(paste("Launch date:", initial_deployment_date))
  print(paste("Re-launch recovery date:", relaunch_recovery_date))
  print(paste("Re-launch deployment date:", relaunch_deployment_date))
  print(paste("Recovery date:", recovery_date))
  
  
  print(paste("Reading file:", file_name))
  print(paste("Sensor type:", sensor_type))
  
  # Download csv to temp file ----
  temp_path <- tempfile(fileext = ".csv")
  if (length(file_id) != 1) {     # Error if there are duplicate files
    stop("drive_download() aborted: file_id is not unique.")
  }
  drive_download(as_id(file_id), path = temp_path, overwrite = TRUE)
  print(paste("File downloaded to:", temp_path))
  
  ##### TEMP LOGGER #####
  if (sensor_type == "temp") {
    
    # Read the first few lines and find the header row
    lines <- readLines(temp_path, n = 5)
    header_line <- which(
      sapply(lines, function(line) {
        any(str_detect(tolower(line), "date")) &&
          any(str_detect(tolower(line), "temp"))
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
      setNames(c("datetime", "temp_c")) |>
      mutate(site = site_name,             # Add site column
             position = position,          # Position in water column
             temp_logger_id = logger_id,   # Logger nickname (pulled from filename)
             .before = datetime) |>
      # Make datetime column POSIXct class
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS"))) |>
      # Only keep rows where temp is not NA
      filter(!is.na(temp_c)) |>
      # Remove identical rows
      distinct()
    
    # Filtering out-of-water times 
    df <- df |>
      # Add in missing times as NA to complete timeseries
      group_by(site, position) |>
      complete(datetime = seq(min(datetime), max(datetime), by = "15 mins")) |>
      ungroup() |>
      mutate(
        temp_c = case_when(
          !is.na(initial_deployment_date) &
            datetime <= initial_deployment_date ~ NA_real_,
          !is.na(relaunch_recovery_date) & !is.na(relaunch_deployment_date) &
            datetime >= relaunch_recovery_date & datetime <= relaunch_deployment_date ~ NA_real_,
          !is.na(recovery_date) &
            datetime >= recovery_date ~ NA_real_,
          TRUE ~ temp_c
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
        any(str_detect(tolower(line), "date")) &&
          any(str_detect(tolower(line), "mv"))
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
    
    # Find the correct ph column
    ph_matches <- names(df)[str_detect(names(df), regex("ph", ignore_case = TRUE)) &
                              !str_detect(names(df), regex("calibrat", ignore_case = TRUE))]
    
    ph_col <- if (length(ph_matches) > 0) ph_matches[1] else NA_character_
    
    # Check if a valid ph column was found
    has_ph_col <- !is.na(ph_col)
    
    # Select the columns
    if (has_ph_col) {
      selected_cols <- df |>
        select(
          contains("date", ignore.case = TRUE),
          contains("mv", ignore.case = TRUE),
          all_of(ph_col)
        )
    } else {
      selected_cols <- df |>
        select(
          contains("date", ignore.case = TRUE),
          contains("mv", ignore.case = TRUE)
        )
    }
    
    
    print("Selected columns for pH logger:")
    print(names(selected_cols))
    
    # Rename and continue
    df <- selected_cols |>
      setNames(c("datetime", "millivolts", if (has_ph_col) "pH")) |>
      mutate(
        pH = if (has_ph_col) pH else NA_real_,
        site = site_name,
        ph_logger_id = logger_id,
        position = position,
        .before = datetime
      ) |>
      mutate(
        datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS"))
      ) |>
      distinct()
    
    # Filtering out-of-water times 
    df <- df |>
      # Add in missing times as NA to complete timeseries
      group_by(site, position) |>
      complete(datetime = seq(min(datetime), max(datetime), by = "15 mins")) |>
      ungroup() |>
      mutate(
        pH = case_when(
          !is.na(initial_deployment_date) &
            datetime <= initial_deployment_date ~ NA_real_,
          !is.na(relaunch_recovery_date) & !is.na(relaunch_deployment_date) &
            datetime >= relaunch_recovery_date & datetime <= relaunch_deployment_date ~ NA_real_,
          !is.na(recovery_date) &
            datetime >= recovery_date ~ NA_real_,
          TRUE ~ pH
        ),
        millivolts = case_when(
          !is.na(initial_deployment_date) &
            datetime <= initial_deployment_date ~ NA_real_,
          !is.na(relaunch_recovery_date) & !is.na(relaunch_deployment_date) &
            datetime >= relaunch_recovery_date & datetime <= relaunch_deployment_date ~ NA_real_,
          !is.na(recovery_date) &
            datetime >= recovery_date ~ NA_real_,
          TRUE ~ millivolts
        )
      )
    
    print(paste("Rows after cleaning:", nrow(df)))
  }
  
  ##### WATER LEVEL LOGGER #####
  else if (sensor_type == "wl") {
    
    lines <- readLines(temp_path, n = 5)
    header_line <- which(
      sapply(lines, function(line) {
        any(str_detect(tolower(line), "date")) &&
          any(str_detect(tolower(line), "pres"))
      })
    )[1]
    
    if (is.na(header_line)) {
      print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    print(paste("Skipping", skip_n, "lines"))
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE)) |>
      clean_names()
    print(paste("Columns detected in WL file:", paste(names(df), collapse = ", ")))
    
    pres_col <- names(df)[str_detect(names(df), "pres")]
    datetime_col <- names(df)[str_detect(names(df), "date")][1]
    
    print(paste("Pressure column:", pres_col))
    print(paste("Datetime column:", datetime_col))
    
    # Detect unit
    unit <- case_when(
      str_detect(pres_col, "psi") ~ "psi",
      str_detect(pres_col, "k.?pa") ~ "kpa",
      TRUE ~ NA_character_
    )
    
    print(paste("Detected unit:", unit))
    
    df <- df |>
      select(datetime = all_of(datetime_col), abs_pres = all_of(pres_col)) |>
      mutate(abs_pres_kpa = case_when(
        unit == "psi" ~ as.numeric(abs_pres) * 6.89476,
        unit == "kpa" ~ as.numeric(abs_pres),
        TRUE ~ NA_real_)) |>
      select(-abs_pres)
    
    print(paste("Rows before filtering:", nrow(df)))
    
    df <- df |>
      mutate(site = site_name,
             wl_logger_id = logger_id,
             position = position,
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS"))) |>
      filter(!is.na(abs_pres_kpa)) |>
      distinct()
    
    # Filtering out-of-water times 
    df <- df |>
      # Add in missing times as NA to complete timeseries
      group_by(site, position) |>
      complete(datetime = seq(min(datetime), max(datetime), by = "15 mins")) |>
      ungroup() |>
      mutate(
        abs_pres_kpa = case_when(
          !is.na(initial_deployment_date) &
            datetime <= initial_deployment_date ~ NA_real_,
          !is.na(relaunch_recovery_date) & !is.na(relaunch_deployment_date) &
            datetime >= relaunch_recovery_date & datetime <= relaunch_deployment_date ~ NA_real_,
          !is.na(recovery_date) &
            datetime >= recovery_date ~ NA_real_,
          TRUE ~ abs_pres_kpa
        )
      )
    
    print(paste("Rows after filtering:", nrow(df)))
  }
  
  ##### CONDUCTIVITY LOGGER #####
  else if (sensor_type == "con") {
    
    lines <- readLines(temp_path, n = 5)
    header_line <- which(
      sapply(lines, function(line) {
        any(str_detect(tolower(line), "date")) &&
          any(str_detect(tolower(line), "range"))
      })
    )[1]
    
    if (is.na(header_line)) {
      print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    print(paste("Skipping", skip_n, "lines"))
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
    
    df <- df |>
      select(contains("date", ignore.case = TRUE),
             contains("range", ignore.case = TRUE))
    
    print("Selected columns:")
    print(names(df))
    
    df <- df |>
      setNames(c("datetime", "high_range_microsiemens_per_cm")) |>
      mutate(site = site_name,
             position = position,
             con_logger_id = logger_id,
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS"))) |>
      filter(!is.na(high_range_microsiemens_per_cm)) |>
      distinct()
    
    # Filtering out-of-water times 
    df <- df |>
      # Add in missing times as NA to complete timeseries
      group_by(site, position) |>
      complete(datetime = seq(min(datetime), max(datetime), by = "15 mins")) |>
      ungroup() |>
      mutate(
        high_range_microsiemens_per_cm = case_when(
          !is.na(initial_deployment_date) &
            datetime <= initial_deployment_date ~ NA_real_,
          !is.na(relaunch_recovery_date) & !is.na(relaunch_deployment_date) &
            datetime >= relaunch_recovery_date & datetime <= relaunch_deployment_date ~ NA_real_,
          !is.na(recovery_date) &
            datetime >= recovery_date ~ NA_real_,
          TRUE ~ high_range_microsiemens_per_cm
        )
      )
    
    print(paste("Rows after filtering:", nrow(df)))
  }
  
  ##### DISSOLVED OXYGEN LOGGER #####
  else if (sensor_type == "do") {
    
    lines <- readLines(temp_path, n = 5)
    header_line <- which(
      sapply(lines, function(line) {
        any(str_detect(tolower(line), "date")) &&
          any(str_detect(tolower(line), "do conc"))
      })
    )[1]
    
    if (is.na(header_line)) {
      print(paste("No valid header found in", file_name, "- defaulting to skip = 0"))
    }
    skip_n <- if (!is.na(header_line)) header_line - 1 else 0
    print(paste("Skipping", skip_n, "lines"))
    
    df <- suppressWarnings(read_csv(temp_path, skip = skip_n, show_col_types = FALSE))
    
    df <- df |>
      select(contains("date", ignore.case = TRUE),
             contains("do conc", ignore.case = TRUE)) |>
      setNames(c("datetime", "do_conc_mg_per_L")) |>
      mutate(site = site_name,
             do_logger_id = logger_id,
             position = position,
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS"))) |>
      filter(!is.na(do_conc_mg_per_L)) |>
      distinct()
    
    # Filtering out-of-water times 
    df <- df |>
      # Add in missing times as NA to complete timeseries
      group_by(site, position) |>
      complete(datetime = seq(min(datetime), max(datetime), by = "15 mins")) |>
      ungroup() |>
      mutate(
        do_conc_mg_per_L = case_when(
          !is.na(initial_deployment_date) &
            datetime <= initial_deployment_date ~ NA_real_,
          !is.na(relaunch_recovery_date) & !is.na(relaunch_deployment_date) &
            datetime >= relaunch_recovery_date & datetime <= relaunch_deployment_date ~ NA_real_,
          !is.na(recovery_date) &
            datetime >= recovery_date ~ NA_real_,
          TRUE ~ do_conc_mg_per_L
        )
      )
    
    print(paste("Rows after filtering:", nrow(df)))
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
                                          orders = c("dmy HMS", "dmy HM", "dmy IMp")), 
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
      # Add in missing times as NA to complete timeseries
      group_by(site, position) |>
      complete(datetime = seq(min(datetime), max(datetime), by = "15 mins")) |>
      ungroup() |>
      mutate(
        raw_integrating_light = case_when(
          !is.na(initial_deployment_date) &
            datetime <= initial_deployment_date ~ NA_real_,
          !is.na(relaunch_recovery_date) & !is.na(relaunch_deployment_date) &
            datetime >= relaunch_recovery_date & datetime <= relaunch_deployment_date ~ NA_real_,
          !is.na(recovery_date) &
            datetime >= recovery_date ~ NA_real_,
          TRUE ~ raw_integrating_light
        ),
        calibrated_integrating_light = case_when(
          !is.na(initial_deployment_date) &
            datetime <= initial_deployment_date ~ NA_real_,
          !is.na(relaunch_recovery_date) & !is.na(relaunch_deployment_date) &
            datetime >= relaunch_recovery_date & datetime <= relaunch_deployment_date ~ NA_real_,
          !is.na(recovery_date) &
            datetime >= recovery_date ~ NA_real_,
          TRUE ~ calibrated_integrating_light
        )
      )
    
    
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
read_and_clean_metadata <- function(metadata_file_url = "https://docs.google.com/spreadsheets/d/1JJ4Vtb_pI9FJRvMYg7pbhQzM6JPcFzGVU3rR9IdYnGQ/edit?gid=1334564098#gid=1334564098",
                                    sheet_name = " Deployments_IndexOnly"){
  
  # Read in metadata sheet from google drive
  metadata_raw <- read_sheet(metadata_file_url,
                             sheet = sheet_name,
                             na = c("", "n/a", "#N/A"),
                             col_types = "c")
  
  # Cleaning
  metadata <- metadata_raw |>
    clean_names() |>
    # Rename nickname to logger_id to match logger file convention
    rename("site" = "x1",
           "logger_id" = "nickname") |>
    separate(relaunch_file_name_recovery_file_name,
             into = c("relaunch_file_name", "recovery_file_name"),
             sep = ",") |>
    # Make position consistently lowercase
    mutate(position = tolower(position),
           # If no logger_id in column, pull from filename
           logger_id = if_else(is.na(logger_id), 
                               str_split(relaunch_file_name, "_", simplify = TRUE)[, 2],
                               logger_id)) |>
    # Remove rows that are year seperators
    filter(!(str_detect(site, "^20\\d{2}$"))) |>
    # Make site name consistently lowercase with no spaces or puncuation
    mutate(site = tolower(site),
           site = str_remove_all(site, "[^a-zA-Z0-9]"),
           site = ifelse(site == "squaxinisland", "squaxin", site)) |>
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
    mutate(relaunch_date = if_else(site == "edmonds" &
                                     logger_id == "DO18" &
                                     relaunch_date == "2025-04-02",
                                   "2025-03-13",
                                   relaunch_date),
           relaunch_date = parse_date_time(relaunch_date,
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
      .after = recovery_time) |>
    separate(file_upload_date,
             into = c("file_upload_date_1", "file_upload_date_2"),
             sep = ",") |>
    mutate(file_upload_date_1 = parse_date_time(file_upload_date_1,
                                                orders = c("ymd", "mdy")),
           file_upload_date_2 = parse_date_time(file_upload_date_2,
                                                orders = c("ymd", "mdy")))
  
  return(metadata)
}  # END metadata cleaning function ----


# Main function to process all data and create wide format CSV ----
#'
#' Crawl Drive, read and clean all logger CSVs, and combine into wide format
#'
#' @param root_folder_id The Google Drive folder ID containing sensor subfolders (TEMP, PH, etc.)
#' @param metadata_file_url URL to main index metadata google sheet
#' @param sheet_name Name of the tab that the metadata is on
#' @return A single tibble with all cleaned sensor data combined in wide format
#'
process_all_logger_data <- function(root_folder_id = "1k5u8iOhR5alnymc7BVU-JJjcSnQStgWv",
                                    metadata_file_url = "https://docs.google.com/spreadsheets/d/1JJ4Vtb_pI9FJRvMYg7pbhQzM6JPcFzGVU3rR9IdYnGQ/edit?gid=1334564098#gid=1334564098",
                                    sheet_name = " Deployments_IndexOnly") {
  
  # 1. List out all logger files from drive
  message("Crawling Drive folder structure...")
  file_list <- get_all_logger_csvs_by_id(root_folder_id)
  
  if (nrow(file_list) == 0) {
    warning("No CSV files found in provided folder.")
    return(NULL)
  }
  
  message(paste("Found", nrow(file_list), "CSV files"))
  
  # 2. Retrieve and clean main metadata file
  message("Reading and cleaning metadata...")
  metadata <- read_and_clean_metadata(metadata_file_url, sheet_name)
  
  # 3. Clean each file one-by-one and collect results by sensor type
  message("Reading and cleaning each CSV file...")
  
  # Initialize lists to store data by sensor type
  temp_data <- list()
  ph_data <- list()
  do_data <- list()
  wl_data <- list()
  par_data <- list()
  con_data <- list()
  
  # Process each file
  for (i in seq_len(nrow(file_list))) {
    file_row <- file_list[i, ]
    sensor_type <- tolower(file_row$sensor_type)
    
    message(paste("Processing file", i, "of", nrow(file_list), ":", file_row$name))
    
    tryCatch({
      cleaned_data <- read_and_clean_logger_csv(file_row, metadata)
      
      # Store in appropriate list based on sensor type
      if (sensor_type == "temp") {
        temp_data <- append(temp_data, list(cleaned_data))
      } else if (sensor_type == "ph") {
        ph_data <- append(ph_data, list(cleaned_data))
      } else if (sensor_type == "do") {
        do_data <- append(do_data, list(cleaned_data))
      } else if (sensor_type == "wl") {
        wl_data <- append(wl_data, list(cleaned_data))
      } else if (sensor_type == "par") {
        par_data <- append(par_data, list(cleaned_data))
      } else if (sensor_type == "con") {
        con_data <- append(con_data, list(cleaned_data))
      } else {
        warning(paste("Unknown sensor type:", sensor_type, "for file:", file_row$name))
      }
    }, error = function(e) {
      warning(paste("Error processing file", file_row$name, ":", e$message))
    })
  }
  
  # 4. Combine data by sensor type
  message("Combining data by sensor type...")
  
  df_temp <- if (length(temp_data) > 0) bind_rows(temp_data) else NULL
  df_ph <- if (length(ph_data) > 0) bind_rows(ph_data) else NULL
  df_do <- if (length(do_data) > 0) bind_rows(do_data) else NULL
  df_wl <- if (length(wl_data) > 0) bind_rows(wl_data) else NULL
  df_par <- if (length(par_data) > 0) bind_rows(par_data) else NULL
  df_con <- if (length(con_data) > 0) bind_rows(con_data) else NULL
  
  # 5. Join all sensor types in wide format
  message("Creating wide format dataset...")
  
  # Start with the first non-null dataset
  all_data <- NULL
  join_keys <- c("site", "datetime", "position")
  
  if (!is.null(df_temp)) {
    all_data <- df_temp
    message("Starting with temperature data")
  }
  
  if (!is.null(df_ph)) {
    if (is.null(all_data)) {
      all_data <- df_ph
      message("Starting with pH data")
    } else {
      all_data <- all_data |> full_join(df_ph, by = join_keys)
      message("Added pH data")
    }
  }
  
  if (!is.null(df_do)) {
    if (is.null(all_data)) {
      all_data <- df_do
      message("Starting with DO data")
    } else {
      all_data <- all_data |> full_join(df_do, by = join_keys)
      message("Added DO data")
    }
  }
  
  if (!is.null(df_wl)) {
    if (is.null(all_data)) {
      all_data <- df_wl
      message("Starting with water level data")
    } else {
      all_data <- all_data |> full_join(df_wl, by = join_keys)
      message("Added water level data")
    }
  }
  
  if (!is.null(df_par)) {
    if (is.null(all_data)) {
      all_data <- df_par
      message("Starting with PAR data")
    } else {
      all_data <- all_data |> full_join(df_par, by = join_keys)
      message("Added PAR data")
    }
  }
  
  if (!is.null(df_con)) {
    if (is.null(all_data)) {
      all_data <- df_con
      message("Starting with conductivity data")
    } else {
      all_data <- all_data |> full_join(df_con, by = join_keys)
      message("Added conductivity data")
    }
  }
  
  if (is.null(all_data)) {
    warning("No data was successfully processed")
    return(NULL)
  }
  
  # 6. Sort the final dataset
  all_data <- all_data |>
    arrange(site, position, datetime)
  
  # And arrange columns
  all_data <- all_data[, c("site", "position", "datetime", 
                           "temp_logger_id", "temp_c", 
                           "ph_logger_id", "pH", "millivolts",
                           "do_logger_id", "do_conc_mg_per_L", 
                           "wl_logger_id", "abs_pres_kpa", 
                           "par_logger_id", "raw_integrating_light", "calibrated_integrating_light", 
                           "con_logger_id","high_range_microsiemens_per_cm")]
  
  
  # 7. Data ready for return
  
  message(paste("Processing complete! Final dataset has", nrow(all_data), "rows and", ncol(all_data), "columns"))
  
  # Print summary of what was included
  sensor_summary <- c()
  if (!is.null(df_temp)) sensor_summary <- c(sensor_summary, "Temperature")
  if (!is.null(df_ph)) sensor_summary <- c(sensor_summary, "pH")
  if (!is.null(df_do)) sensor_summary <- c(sensor_summary, "Dissolved Oxygen")
  if (!is.null(df_wl)) sensor_summary <- c(sensor_summary, "Water Level")
  if (!is.null(df_par)) sensor_summary <- c(sensor_summary, "PAR")
  if (!is.null(df_con)) sensor_summary <- c(sensor_summary, "Conductivity")
  
  message(paste("Sensor types included:", paste(sensor_summary, collapse = ", ")))
  
  # Print detailed summary by site, position, and sensor type
  message("Sites, positions, and sensor data included:")
  
  # Create summary for each sensor type separately
  sensor_summaries <- list()
  
  # Temperature data
  if ("temp_c" %in% names(all_data)) {
    temp_summary <- all_data |>
      filter(!is.na(temp_c)) |>
      group_by(site, position) |>
      summarise(
        sensor_type = "TEMP",
        min_date = min(datetime, na.rm = TRUE),
        max_date = max(datetime, na.rm = TRUE),
        .groups = "drop"
      )
    sensor_summaries <- append(sensor_summaries, list(temp_summary))
  }
  
  # pH data
  if (any(c("pH", "millivolts") %in% names(all_data))) {
    ph_summary <- all_data |>
      filter(!is.na(pH) | !is.na(millivolts)) |>
      group_by(site, position) |>
      summarise(
        sensor_type = "PH",
        min_date = min(datetime, na.rm = TRUE),
        max_date = max(datetime, na.rm = TRUE),
        .groups = "drop"
      )
    sensor_summaries <- append(sensor_summaries, list(ph_summary))
  }
  
  # DO data
  if ("do_conc_mg_per_L" %in% names(all_data)) {
    do_summary <- all_data |>
      filter(!is.na(do_conc_mg_per_L)) |>
      group_by(site, position) |>
      summarise(
        sensor_type = "DO",
        min_date = min(datetime, na.rm = TRUE),
        max_date = max(datetime, na.rm = TRUE),
        .groups = "drop"
      )
    sensor_summaries <- append(sensor_summaries, list(do_summary))
  }
  
  # Water level data
  if ("abs_pres_kpa" %in% names(all_data)) {
    wl_summary <- all_data |>
      filter(!is.na(abs_pres_kpa)) |>
      group_by(site, position) |>
      summarise(
        sensor_type = "WL",
        min_date = min(datetime, na.rm = TRUE),
        max_date = max(datetime, na.rm = TRUE),
        .groups = "drop"
      )
    sensor_summaries <- append(sensor_summaries, list(wl_summary))
  }
  
  # PAR data
  if (any(c("raw_integrating_light", "calibrated_integrating_light") %in% names(all_data))) {
    par_summary <- all_data |>
      filter(!is.na(raw_integrating_light) | !is.na(calibrated_integrating_light)) |>
      group_by(site, position) |>
      summarise(
        sensor_type = "PAR",
        min_date = min(datetime, na.rm = TRUE),
        max_date = max(datetime, na.rm = TRUE),
        .groups = "drop"
      )
    sensor_summaries <- append(sensor_summaries, list(par_summary))
  }
  
  # Conductivity data
  if ("high_range_microsiemens_per_cm" %in% names(all_data)) {
    con_summary <- all_data |>
      filter(!is.na(high_range_microsiemens_per_cm)) |>
      group_by(site, position) |>
      summarise(
        sensor_type = "CON",
        min_date = min(datetime, na.rm = TRUE),
        max_date = max(datetime, na.rm = TRUE),
        .groups = "drop"
      )
    sensor_summaries <- append(sensor_summaries, list(con_summary))
  }
  
  # Combine all summaries and sort
  if (length(sensor_summaries) > 0) {
    combined_summary <- bind_rows(sensor_summaries) |>
      arrange(site, position, sensor_type)
    
    # Print each line
    for (i in seq_len(nrow(combined_summary))) {
      date_range <- paste(as.Date(combined_summary$min_date[i]), "to", as.Date(combined_summary$max_date[i]))
      message(paste("  -", combined_summary$site[i], combined_summary$position[i], 
                    paste0(combined_summary$sensor_type[i], ":"), date_range))
    }
  }
  
  return(all_data)
}

# Incremental update function - only processes new files ----
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
update_logger_data_incremental <- function(root_folder_id = "1k5u8iOhR5alnymc7BVU-JJjcSnQStgWv",
                                           metadata_file_url = "https://docs.google.com/spreadsheets/d/1JJ4Vtb_pI9FJRvMYg7pbhQzM6JPcFzGVU3rR9IdYnGQ/edit?gid=1334564098#gid=1334564098",
                                           sheet_name = " Deployments_IndexOnly",
                                           existing_data_file = "logger_data.rds",
                                           tracking_file = "processed_files.rds",
                                           force_reprocess = FALSE) {
  
  message("Starting incremental logger data update...")
  
  # 1. Load existing data if it exists
  existing_data <- NULL
  if (file.exists(existing_data_file) && !force_reprocess) {
    message("Loading existing data...")
    existing_data <- readRDS(existing_data_file)
    message(paste("Existing data has", nrow(existing_data), "rows"))
  }
  
  # 2. Get list of previously processed files
  processed_files <- character(0)
  # If both the file tracking file exists (data has been processed) AND we do not want to reprocess
  if (file.exists(tracking_file) && !force_reprocess) {
    # Load the tracking file
    processed_files <- readRDS(tracking_file)
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
  
  # Process each new file
  for (i in seq_len(nrow(files_to_process))) {
    file_row <- files_to_process[i, ]
    sensor_type <- tolower(file_row$sensor_type)
    
    message(paste("Processing new file", i, "of", nrow(files_to_process), ":", file_row$name))
    
    tryCatch({  # trying this new way of throwing warnings/errors
      cleaned_data <- read_and_clean_logger_csv(file_row, metadata)
      
      # Store in appropriate list
      if (sensor_type == "temp") {
        new_temp_data <- append(new_temp_data, list(cleaned_data))
      } else if (sensor_type == "ph") {
        new_ph_data <- append(new_ph_data, list(cleaned_data))
      } else if (sensor_type == "do") {
        new_do_data <- append(new_do_data, list(cleaned_data))
      } else if (sensor_type == "wl") {
        new_wl_data <- append(new_wl_data, list(cleaned_data))
      } else if (sensor_type == "par") {
        new_par_data <- append(new_par_data, list(cleaned_data))
      } else if (sensor_type == "con") {
        new_con_data <- append(new_con_data, list(cleaned_data))
      }
    }, error = function(e) {
      warning(paste("Error processing file", file_row$name, ":", e$message))
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
    
    # For incremental updates, we'll remove any overlapping data and re-add
    # Is this how we want to do this? Check with team!!
    # This handles cases where files might have been updated with more recent data
    # Note that they would need a new name.
    
    # Get site/position/date ranges from new data to remove from existing
    sites_positions_to_update <- tibble()
    
    if (!is.null(new_df_temp)) {
      temp_ranges <- new_df_temp |>
        group_by(site, position) |>
        summarise(min_date = min(datetime), max_date = max(datetime), .groups = "drop")
      sites_positions_to_update <- bind_rows(sites_positions_to_update, temp_ranges)
    }
    
    if (!is.null(new_df_ph)) {
      ph_ranges <- new_df_ph |>
        group_by(site, position) |>
        summarise(min_date = min(datetime), max_date = max(datetime), .groups = "drop")
      sites_positions_to_update <- bind_rows(sites_positions_to_update, ph_ranges)
    }
    
    if (!is.null(new_df_do)) {
      do_ranges <- new_df_do |>
        group_by(site, position) |>
        summarise(min_date = min(datetime), max_date = max(datetime), .groups = "drop")
      sites_positions_to_update <- bind_rows(sites_positions_to_update, do_ranges)
    }
    
    if (!is.null(new_df_wl)) {
      wl_ranges <- new_df_wl |>
        group_by(site, position) |>
        summarise(min_date = min(datetime), max_date = max(datetime), .groups = "drop")
      sites_positions_to_update <- bind_rows(sites_positions_to_update, wl_ranges)
    }
    
    if (!is.null(new_df_par)) {
      par_ranges <- new_df_par |>
        group_by(site, position) |>
        summarise(min_date = min(datetime), max_date = max(datetime), .groups = "drop")
      sites_positions_to_update <- bind_rows(sites_positions_to_update, par_ranges)
    }
    
    if (!is.null(new_df_con)) {
      con_ranges <- new_df_con |>
        group_by(site, position) |>
        summarise(min_date = min(datetime), max_date = max(datetime), .groups = "drop")
      sites_positions_to_update <- bind_rows(sites_positions_to_update, con_ranges)
    }
    
    # Remove overlapping data from existing dataset. CHECK WITH TEAM ----
    if (nrow(sites_positions_to_update) > 0) {
      sites_positions_to_update <- sites_positions_to_update |>
        group_by(site, position) |>
        summarise(min_date = min(min_date), max_date = max(max_date), .groups = "drop")
      
      message("Removing overlapping data from existing dataset...")
      
      for (i in seq_len(nrow(sites_positions_to_update))) {
        site_name <- sites_positions_to_update$site[i]
        position_name <- sites_positions_to_update$position[i]
        min_date <- sites_positions_to_update$min_date[i]
        max_date <- sites_positions_to_update$max_date[i]
        
        existing_data <- existing_data |>
          filter(!(site == site_name & position == position_name & 
                     datetime >= min_date & datetime <= max_date))
      }
    }
    
    # Separate existing data by sensor type (if columns exist)
    existing_temp <- if ("temp_c" %in% names(existing_data)) {
      existing_data |> 
        select(site, datetime, position, contains("temp_")) |> 
        filter(!is.na(temp_c))
    } else { NULL }
    
    existing_ph <- if (any(c("pH", "millivolts") %in% names(existing_data))) {
      existing_data |> 
        select(site, datetime, position, contains("ph_"), pH, millivolts) |> 
        filter(!is.na(pH) | !is.na(millivolts))
    } else { NULL }
    
    existing_do <- if ("do_conc_mg_per_L" %in% names(existing_data)) {
      existing_data |> 
        select(site, datetime, position, contains("do_")) |> 
        filter(!is.na(do_conc_mg_per_L))
    } else { NULL }
    
    existing_wl <- if ("abs_pres_kpa" %in% names(existing_data)) {
      existing_data |> 
        select(site, datetime, position, contains("wl_"), abs_pres_kpa) |> 
        filter(!is.na(abs_pres_kpa))
    } else { NULL }
    
    existing_par <- if (any(c("raw_integrating_light", "calibrated_integrating_light") %in% 
                            names(existing_data))) {
      existing_data |> 
        select(site, datetime, position, contains("par_"), 
               raw_integrating_light, calibrated_integrating_light) |> 
        filter(!is.na(raw_integrating_light) | !is.na(calibrated_integrating_light))
    } else { NULL }
    
    existing_con <- if ("high_range_microsiemens_per_cm" %in% names(existing_data)) {
      existing_data |> 
        select(site, datetime, position, contains("con_")) |> 
        filter(!is.na(high_range_microsiemens_per_cm))
    } else { NULL }
    
    # Combine existing with new data for each sensor type
    df_temp <- bind_rows(existing_temp, new_df_temp)
    df_ph <- bind_rows(existing_ph, new_df_ph) 
    df_do <- bind_rows(existing_do, new_df_do)
    df_wl <- bind_rows(existing_wl, new_df_wl)
    df_par <- bind_rows(existing_par, new_df_par)
    df_con <- bind_rows(existing_con, new_df_con)
    
  } else {
    # No existing data, use new data as-is
    df_temp <- new_df_temp
    df_ph <- new_df_ph
    df_do <- new_df_do
    df_wl <- new_df_wl
    df_par <- new_df_par
    df_con <- new_df_con
  }
  
  # 10. Create final wide format (same as original function)
  message("Creating final wide format dataset...")
  
  all_data <- NULL
  join_keys <- c("site", "datetime", "position")
  
  if (!is.null(df_temp)) {
    all_data <- df_temp
    message("Added temperature data")
  }
  
  if (!is.null(df_ph)) {
    if (is.null(all_data)) {
      all_data <- df_ph
      message("Starting with pH data")
    } else {
      all_data <- all_data |> full_join(df_ph, by = join_keys)
      message("Added pH data")
    }
  }
  
  if (!is.null(df_do)) {
    if (is.null(all_data)) {
      all_data <- df_do
      message("Starting with DO data")
    } else {
      all_data <- all_data |> full_join(df_do, by = join_keys)
      message("Added DO data")
    }
  }
  
  if (!is.null(df_wl)) {
    if (is.null(all_data)) {
      all_data <- df_wl
      message("Starting with water level data")
    } else {
      all_data <- all_data |> full_join(df_wl, by = join_keys)
      message("Added water level data")
    }
  }
  
  if (!is.null(df_par)) {
    if (is.null(all_data)) {
      all_data <- df_par
      message("Starting with PAR data")
    } else {
      all_data <- all_data |> full_join(df_par, by = join_keys)
      message("Added PAR data")
    }
  }
  
  if (!is.null(df_con)) {
    if (is.null(all_data)) {
      all_data <- df_con
      message("Starting with conductivity data")
    } else {
      all_data <- all_data |> full_join(df_con, by = join_keys)
      message("Added conductivity data")
    }
  }
  
  if (is.null(all_data)) {
    warning("No data was successfully processed")
    return(existing_data)
  }
  
  # Sort final dataset
  all_data <- all_data |>
    arrange(site, position, datetime) |>
    distinct()  # Remove any duplicates
  
  # 11. Save updated data and tracking info
  message("Saving updated data...")
  saveRDS(all_data, existing_data_file)
  
  # Update processed files list
  updated_processed_files <- unique(c(processed_files, files_to_process$name))
  saveRDS(updated_processed_files, tracking_file)
  
  message(paste("Incremental update complete!"))
  message(paste("Final dataset has", nrow(all_data), "rows and", ncol(all_data), "columns"))
  message(paste("Processed", nrow(files_to_process), "new files"))
  message(paste("Data saved to:", existing_data_file))
  
  return(all_data)
}
