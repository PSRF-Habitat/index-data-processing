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
#' @return A cleaned tibble with standardized columns
read_and_clean_logger_csv <- function(file_row, metadata) {
  
  # Pull out file info ----
  file_id <- file_row$id
  file_name <- file_row$name
  sensor_type <- tolower(file_row$sensor_type)
  parts <- str_split(file_name, "_", simplify = TRUE)
  site_name <- parts[1]
  site_name <- tolower(str_remove_all(site_name, "[^a-zA-Z0-9]"))
  if(site_name == "squaxinisland"){
    site_name == "squaxin"
  }
  
  print(paste("Site name:", site_name))
  logger_id <- parts[2]
  print(paste("Logger Id:", logger_id))
  deployment_date <- parts[3]
  deployment_date <- str_remove_all(deployment_date, regex(".csv", ignore_case = TRUE))
  print(paste("Date deployed:", deployment_date))
  position <- metadata$position[metadata$site == site_name &
                                  metadata$logger_id == logger_id &
                                  metadata$initial_deployment_date == as.Date(deployment_date)]
  if (length(position) == 0 || all(is.na(position))) {
    position <- metadata$position[
      metadata$site == site_name &
        metadata$logger_id == logger_id &
        metadata$relaunch_date == as.Date(deployment_date)
    ]
  } 
  
  if (length(position) > 1) {
    warning(paste("Multiple positions found. Using the first non-NA. Matches:", paste(position, collapse = ", ")))
    position <- position[!is.na(position)][1]
  } 
  
  if (length(position) == 0) {
    warning(paste("No position found, defaulting to NA"))
    position <- NA
  }
  
  print(paste("Position:", position))
  
  print(paste("Reading file:", file_name))
  print(paste("Sensor type:", sensor_type))
  
  # Download csv to temp file ----
  temp_path <- tempfile(fileext = ".csv")
  if (length(file_id) != 1) {
    stop("drive_download() aborted: file_id is not unique.")
  }
  
  drive_download(as_id(file_id), path = temp_path, overwrite = TRUE)
  print(paste("File downloaded to:", temp_path))
  
  ##### TEMP LOGGER #####
  if (sensor_type == "temp") {
    
    lines <- readLines(temp_path, n = 5)
    header_line <- which(
      sapply(lines, function(line) {
        any(str_detect(tolower(line), "date")) &&
          any(str_detect(tolower(line), "temp"))
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
             contains("temp", ignore.case = TRUE))
    
    print("Columns selected:")
    print(names(df))
    
    df <- df |>
      setNames(c("datetime", "temp_c")) |>
      mutate(site = site_name,
             position = position,
             temp_logger_id = logger_id,
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS"))) |>
      filter(!is.na(temp_c)) |>
      distinct()
    
    print(paste("Rows after filtering:", nrow(df)))
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
        .before = datetime
      ) |>
      mutate(
        datetime = parse_date_time(datetime, orders = c("m/d/y I:M:S p", "m/d/y HMS"))
      ) |>
      distinct()
    
    
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
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS"))) |>
      filter(!is.na(abs_pres_kpa)) |>
      distinct()
    
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
             .before = datetime) |>
      mutate(datetime = parse_date_time(datetime,
                                        orders = c("m/d/y I:M:S p", "m/d/y HMS"))) |>
      filter(!is.na(do_conc_mg_per_L)) |>
      distinct()
    
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
               .before = datetime) |>
        filter(!is.na(datetime)) |>
        distinct()
      print(paste("Rows after filtering:", nrow(df)))
    }
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
    separate(file_upload_date,
             into = c("file_upload_date_1", "file_upload_date_2"),
             sep = ",") |>
    mutate(file_upload_date_1 = parse_date_time(file_upload_date_1,
                                                orders = c("ymd", "mdy")),
           file_upload_date_2 = parse_date_time(file_upload_date_2,
                                                orders = c("ymd", "mdy")))
  
  return(metadata)
}  # END metadata cleaning function ----


# Putting it all together ----
#' Crawl Drive, read and clean all logger CSVs, and return 1 combined tibble
#'
#' @param root_folder_id The Google Drive folder ID containing sensor subfolders (TEMP, PH, etc.)
#' @return A single tibble with all cleaned sensor data combined
combine_all_logger_data <- function(root_folder_id = "1k5u8iOhR5alnymc7BVU-JJjcSnQStgWv") {
  
  # 1. List out all logger files from drive
  message("Crawling Drive folder structure...")
  file_list <- get_all_logger_csvs_by_id(root_folder_id)
  
  if (nrow(file_list) == 0) {
    warning("No CSV files found in provided folder.")
    return(NULL)
  }
  
  # 2. Retrieve and clean main metadata file
  
 # metadata <- 
  
  # 3. Clean each file one-by-one and collect results
  message("Reading and cleaning each CSV file...")
  all_cleaned <- purrr::map_dfr(seq_len(nrow(file_list)), function(i) {
    read_and_clean_logger_csv(file_list[i, ])
  })
  
  return(all_cleaned)
}


