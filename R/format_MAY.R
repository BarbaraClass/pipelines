#' Construct standard format for data from Mayachino, Russia.
#'
#' A pipeline to produce the standard format for the study site at
#' Mayachino, Russia, administered by the Institute of Biology at the Karelian Research Centre.
#'
#' This section provides details on data management choices that are unique to
#' these data. For a general description of the standard format please see
#' \href{https://github.com/SPI-Birds/documentation/blob/master/standard_protocol/SPI_Birds_Protocol_v2.0.0.pdf}{here}.
#
#' \strong{Lay dates & hatch dates}: Some lay dates and hatch dates contain special characters (e.g., "<", "?"), which mean they are unknwon. Dates between brackets are probable dates and brackets are ignored. Some lay dates and hatch dates in primary data are a range of values (e.g., "31-34"). These are converted into the minimum (minimumLayDate, minimumHatchDate) and maximum dates (maximumLayDate, maximumHatchDate); observed dates (observedLayDate, observedHatchDate) are given by the average of the range (rounded down; i.e. "32" in the example).
#'
#' \strong{Clutch size, brood size, fledgling number}: Some clutch sizes, brood sizes, and fledgling numbers contain special characters (e.g., ?", "()"), which means incomplete clutches or size unknown. These are interpreted as NA. Some clutch sizes, brood sizes, and fledgling numbers are written as an arithmetic expression (e.g., "7+4"), and result from brood manipulation experiments (enlarged or reduced clutches in 1984-1987) or double clutches.Keep the final brood size.
#'
#' \strong{Capture dates}: No exact information on capture dates are given. These are therefore put as NA rather than inferring inaccurate dates (to avoid confusing data users).
#'#'
#' \strong{captureTagID, releaseTagID}: First captures of all individuals are assumed to be tagging events, and thus captureTagID is set to NA.
#'
#' Only when individual IDs start with two letters, followed by 5 or 6 digits, the ID is considered a ring number and stored in captureTagID and/or releaseTagID. When, for example, the letters are missing, the ring number is considered incomplete and captureTagID and releaseTagID are set to NA.
#' The problem is that releaseTagID does not allow for NAs..
#'
#'#' \strong{TreatmentID}: Refers to brood size manipulation experiments carried out between 1984 and 1987 in which the numbers of eggs or nestlings were enlarged/reduced in some broods
#'
#' @inheritParams pipeline_params
#'
#' @return Generates either 6 .csv files or 6 data frames in the standard format.
#' @export
#'

format_MAY <- function(db = choose_directory(),
                       species = NULL,
                       site = NULL,
                       optional_variables = NULL,
                       path = ".",
                       output_type = "R") {

  # Force choose_directory() if used
  force(db)

  #Assign to database location
  db <- paste0(gsub("\\\\", "/", db), "\\MAY_PrimaryData.xls")

  # Assign species for filtering
  if(is.null(species)){

    species <- species_codes$speciesID

  }

  # If all optional variables are requested, retrieve all names
  if(!is.null(optional_variables) & "all" %in% optional_variables) optional_variables <- names(unlist(unname(utility_variables)))

  # Record start time to provide processing time to the user
  start_time <- Sys.time()

  message("Importing primary data...")

  # Read in pied flycatcher data
  pf_data <- suppressMessages(readxl::read_xls(path=db,sheet="Ficedula hypoleuca 1979-2023 ",guess = 4000) %>%
                                # Convert all cols to snake_case
                                janitor::clean_names() %>%
                                # Create IDs
                                dplyr::mutate(no_nest_box = stringr::str_replace_all(.data$no_nest_box, c("-" = "", " " = "")),
                                              studyID = "MAY-1",
                                              siteID = "MAY",
                                              # Ensure unique plotIDs; add siteID prefix
                                              plotID = paste0("MAY_", toupper(.data$the_line_of_nest_boxes)),
                                              # Ensure unique locationIDs; requires plot & nestbox
                                              locationID = paste(.data$the_line_of_nest_boxes, .data$no_nest_box, sep = "_"),
                                              # Ensure unique broodIDs; plot, nestbox ID, and "no string"
                                              broodID = paste(.data$year, .data$the_line_of_nest_boxes,
                                                              .data$no_nest_box, .data$no_string_1, sep = "_"),
                                              speciesID = species_codes$speciesID[species_codes$speciesCode == "10003"]) %>%
                                # Convert dates
                                dplyr::mutate(dplyr::across(.cols = c("date_of_female_molt_survey",
                                                                      "date_of_male_molt_survey"),
                                                            .fns = ~{

                                                              as.Date(.x)

                                                            }))
                              )

  # Read in great tit data
  gt_data <- suppressMessages(readxl::read_xls(path=db,sheet="Parus_major 1979-2023" ,guess = 4000,range=c("A2:V549")) %>%
                                # Remove trailing columns that contain no information
                                dplyr::select(1:22) %>%
                                # Convert all cols to snake_case
                                janitor::clean_names() %>%
                                # Create IDs
                                dplyr::mutate(no_nest_box = stringr::str_replace_all(.data$no_nest_box, c("-" = "", " " = "")),
                                              studyID = "MAY-1",
                                              siteID = "MAY",
                                              # Ensure unique plotIDs; add siteID prefix
                                              plotID = paste0("MAY_", toupper(.data$the_line_of_nest_boxes)),
                                              # Ensure unique locationIDs; requires plot & nestbox
                                              locationID = paste(.data$the_line_of_nest_boxes, .data$no_nest_box, sep = "_"),
                                              # Ensure unique broodIDs; plot, nestbox ID, and "no string"
                                              broodID = paste(.data$year, .data$the_line_of_nest_boxes,
                                                              .data$no_nest_box, .data$no_string_1, sep = "_"),
                                              speciesID = species_codes$speciesID[species_codes$speciesCode == "10001"])
  )

  # BROOD DATA

  message("Compiling brood data....")

  Brood_data <- create_brood_MAY(gt_data = gt_data,
                                 pf_data = pf_data,
                                 species_filter = species,
                                 optional_variables = optional_variables)

  # CAPTURE DATA

  message("Compiling capture data....")

  Capture_data <- create_capture_MAY(gt_data = gt_data,
                                     pf_data = pf_data,
                                     species_filter = species,
                                     optional_variables = optional_variables)

  # INDIVIDUAL DATA

  message("Compiling individual data....")

  Individual_data <- create_individual_MAY(capture_data = Capture_data,
                                           species_filter = species,
                                           optional_variables = optional_variables)

  # LOCATION DATA

  message("Compiling location data....")

  Location_data <- create_location_MAY(gt_data = gt_data,
                                       pf_data = pf_data)

  # MEASUREMENT DATA

  message("Compiling measurement data....")

  Measurement_data <- create_measurement_MAY(capture_data = Capture_data)

  # EXPERIMENT DATA

  message("Compiling experiment data....")

  Experiment_data <- create_experiment_MAY(brood_data = Brood_data)


  # WRANGLE DATA FOR EXPORT

  # - Brood data
  Brood_data <- Brood_data %>%
    # Add row ID
    dplyr::mutate(row = 1:dplyr::n(),
                  # Ensure that individuals are unique: add institutionID as prefix to femaleID & maleID
                  dplyr::across(.cols = c("femaleID", "maleID"),
                                .fns = ~{

                                  dplyr::case_when(is.na(.x) ~ NA_character_,
                                                   TRUE ~ paste0("MAY_", .x))

                                })) %>%
    # Keep only columns that are in the standard format or in the list of optional variables
    dplyr::select(names(data_templates$v2.0$Brood_data), dplyr::contains(names(utility_variables$Brood_data),
                                                                         ignore.case = FALSE))

  # - Capture data
  Capture_data <- Capture_data %>%
    # Add row ID
    dplyr::mutate(row = 1:dplyr::n(),
                  # Ensure that individuals are unique: add institutionID as prefix to individualID and captureID
                  dplyr::across(.cols = c("individualID", "captureID"),
                                .fns = ~{

                                  paste0("MAY_", .x)

                                })) %>%
    # Add missing columns
    dplyr::bind_cols(data_templates$v2.0$Capture_data[1, !(names(data_templates$v2.0$Capture_data) %in% names(.))]) %>%
    # Keep only columns that are in the standard format or in the list of optional variables
    dplyr::select(names(data_templates$v2.0$Capture_data), dplyr::contains(names(utility_variables$Capture_data),
                                                                           ignore.case = FALSE))

  # - Individual data
  Individual_data <- Individual_data %>%
    # Add row ID
    dplyr::mutate(row = 1:dplyr::n(),
                  # Ensure that individuals are unique: add institutionID as prefix to individualID and captureID
                  individualID = paste0("MAY_", .data$individualID)) %>%
    # Add missing columns
    dplyr::bind_cols(data_templates$v2.0$Individual_data[1, !(names(data_templates$v2.0$Individual_data) %in% names(.))]) %>%
    # Keep only columns that are in the standard format or in the list of optional variables
    dplyr::select(names(data_templates$v2.0$Individual_data), dplyr::contains(names(utility_variables$Individual_data),
                                                                              ignore.case = FALSE))

  # - Location data
  Location_data <- Location_data %>%
    # Add row ID
    dplyr::mutate(row = 1:dplyr::n()) %>%
    # Add missing columns
    dplyr::bind_cols(data_templates$v2.0$Location_data[1, !(names(data_templates$v2.0$Location_data) %in% names(.))]) %>%
    # Keep only columns that are in the standard format
    dplyr::select(names(data_templates$v2.0$Location_data))

  # - Measurement data
  Measurement_data <- Measurement_data %>%
    # Add row ID
    dplyr::mutate(row = 1:dplyr::n(),
                  # Add institutionID as prefix to recordID to match captureID
                  recordID = paste0("MAY_", .data$recordID)) %>%
    # Add missing columns
    dplyr::bind_cols(data_templates$v2.0$Measurement_data[1, !(names(data_templates$v2.0$Measurement_data) %in% names(.))]) %>%
    # Keep only columns that are in the standard format
    dplyr::select(names(data_templates$v2.0$Measurement_data))

  # - Experiment data
  Experiment_data <- Experiment_data %>%
    # Add row ID
    dplyr::mutate(row = 1:dplyr::n()) %>%
    # Add missing columns
    dplyr::bind_cols(data_templates$v2.0$Experiment_data[1, !(names(data_templates$v2.0$Experiment_data) %in% names(.))]) %>%
    # Keep only columns that are in the standard format
    dplyr::select(names(data_templates$v2.0$Experiment_data))


  # TIME

  time <- difftime(Sys.time(), start_time, units = "sec")

  message(paste0("All tables generated in ", round(time, 2), " seconds"))

  # OUTPUT

  if(output_type == "csv"){

    message("Saving .csv files...")

    utils::write.csv(x = Brood_data, file = paste0(path, "\\Brood_data_MAY.csv"), row.names = FALSE)

    utils::write.csv(x = Individual_data, file = paste0(path, "\\Individual_data_MAY.csv"), row.names = FALSE)

    utils::write.csv(x = Capture_data, file = paste0(path, "\\Capture_data_MAY.csv"), row.names = FALSE)

    utils::write.csv(x = Measurement_data, file = paste0(path, "\\Measurement_data_MAY.csv"), row.names = FALSE)

    utils::write.csv(x = Location_data, file = paste0(path, "\\Location_data_MAY.csv"), row.names = FALSE)

    utils::write.csv(x = Experiment_data, file = paste0(path, "\\Experiment_data_MAY.csv"), row.names = FALSE)

    invisible(NULL)

  }

  if(output_type == "R"){

    message("Returning R objects...")

    return(list(Brood_data = Brood_data,
                Capture_data = Capture_data,
                Individual_data = Individual_data,
                Measurement_data = Measurement_data,
                Location_data = Location_data,
                Experiment_data = Experiment_data))

  }

}


#' Create brood data table for Mayachino, Russia.
#'
#' Create brood data table in standard format for data from Mayachino, Russia.
#'
#' @param gt_data Data frame. Great tit data from Mayachino, Russia.
#' @param pf_data Data frame. Pied flycatcher data from Mayachino, Russia.
#' @param species_filter Species of interest. The 6-letter code of species as listed in the \href{https://github.com/SPI-Birds/documentation/blob/master/standard_protocol/SPI_Birds_Appendices.pdf}{Appendices} to the standard format. If blank will return all major species.
#' @param optional_variables A character vector of names of optional variables (generated by standard utility functions) to be included in the pipeline output.
#'
#' @return A data frame.
#'

create_brood_MAY <- function(gt_data,
                             pf_data,
                             species_filter,
                             optional_variables) {

    # Pied flycatcher data
    pf_broods <- pf_data %>%
      dplyr::filter(!is.na(.data$year))%>%
      # Create female & male IDs
      tidyr::unite(femaleID, females_ring_series, females_ring, remove = FALSE, na.rm = TRUE, sep = "") %>%
      tidyr::unite(maleID,males_ring_series, males_ring, remove = FALSE, na.rm = TRUE, sep = "") %>%
      dplyr::mutate(dplyr::across(.cols = c("femaleID", "maleID"),
                                  .fns = ~{

                                    stringr::str_replace_all(dplyr::na_if(.x, ""), pattern = " ", replacement = "")

                                  }),
                    # If femaleID & maleID differ from expected format, set to NA
                    femaleID = dplyr::case_when(stringr::str_detect(.data$femaleID, "^[:upper:]{0,2}[:digit:]{5,6}$") ~ .data$femaleID,
                                                TRUE ~ NA_character_),
                    maleID = dplyr::case_when(stringr::str_detect(.data$maleID, "^[:upper:]{0,2}[:digit:]{5,6}$") ~ .data$maleID,
                                              TRUE ~ NA_character_)) %>%
      # Convert dates from May days (1 = 1st of May) to year, month, day
      # Days formatted as e.g., "<32", ">32", "32?", or "?"are interpreted as NA
      #"(32)"  are interpreted without the special characters
      # Days formatted as e.g., "32-35" are interpreted as a minimum and maximum; observed dates are taken as the rounded down average
      dplyr::mutate(dplyr::across(.cols = c("start_date_of_laying_1_may_1", "hatching_date_1_may_1"),
                                  .fns = ~{

                                    dplyr::case_when(
                                      is.na(.x) ~ NA_character_,
                                      stringr::str_detect(.x, "<") ~ NA_character_,
                                      stringr::str_detect(.x, ">") ~ NA_character_,
                                      stringr::str_detect(.x, "^\\?") ~ NA_character_,
                                      stringr::str_detect(.x, ".*\\?") ~ NA_character_,
                                      stringr::str_detect(.x, "\\(") ~ stringr::str_extract(.x, "(?<=\\()[:digit:]{1,2}(?=\\))"),
                                      stringr::str_detect(.x, ".-") ~ as.character(floor((as.integer(stringr::str_extract(.x, "[:digit:]{1,2}(?=-)")) + as.integer(stringr::str_extract(.x, "(?<=-)[:digit:]{1,2}"))) / 2)),
                                      TRUE ~ .x
                                    )

                                  },
                                  .names = "{.col}_observedMayDate"),
                    dplyr::across(.cols = c("start_date_of_laying_1_may_1", "hatching_date_1_may_1"),
                                  .fns = ~{

                                    dplyr::case_when(
                                      stringr::str_detect(.x, ".-") ~ stringr::str_extract(.x, "[:digit:]{1,2}(?=-)"),
                                      TRUE ~ NA_character_
                                    )

                                  },
                                  .names = "{.col}_minimumMayDate"),
                    dplyr::across(.cols = c("start_date_of_laying_1_may_1", "hatching_date_1_may_1"),
                                  .fns = ~{

                                    dplyr::case_when(
                                      stringr::str_detect(.x, ".-") ~ stringr::str_extract(.x, "(?<=-)[:digit:]{1,2}"),
                                      TRUE ~ NA_character_
                                    )

                                  },
                                  .names = "{.col}_maximumMayDate"),
                    dplyr::across(.cols = dplyr::ends_with(c("observedMayDate", "minimumMayDate", "maximumMayDate")),
                                  .fns = ~{

                                    lubridate::as_date(x = paste0(.data$year, "-04-30")) + as.integer(.x)

                                  }),
                    observedLayYear = dplyr::case_when(is.na(.data$start_date_of_laying_1_may_1_observedMayDate) ~ as.integer(.data$year),
                                                       TRUE ~ as.integer(lubridate::year(.data$start_date_of_laying_1_may_1_observedMayDate))),
                    observedLayMonth = as.integer(lubridate::month(.data$start_date_of_laying_1_may_1_observedMayDate)),
                    observedLayDay = as.integer(lubridate::day(.data$start_date_of_laying_1_may_1_observedMayDate)),
                    minimumLayYear = as.integer(lubridate::year(.data$start_date_of_laying_1_may_1_minimumMayDate)),
                    minimumLayMonth = as.integer(lubridate::month(.data$start_date_of_laying_1_may_1_minimumMayDate)),
                    minimumLayDay = as.integer(lubridate::day(.data$start_date_of_laying_1_may_1_minimumMayDate)),
                    maximumLayYear = as.integer(lubridate::year(.data$start_date_of_laying_1_may_1_maximumMayDate)),
                    maximumLayMonth = as.integer(lubridate::month(.data$start_date_of_laying_1_may_1_maximumMayDate)),
                    maximumLayDay = as.integer(lubridate::day(.data$start_date_of_laying_1_may_1_maximumMayDate)),
                    observedHatchYear = dplyr::case_when(is.na(.data$hatching_date_1_may_1_observedMayDate) ~ as.integer(.data$year),
                                                         TRUE ~ as.integer(lubridate::year(.data$hatching_date_1_may_1_observedMayDate))),
                    observedHatchMonth = as.integer(lubridate::month(.data$hatching_date_1_may_1_observedMayDate)),
                    observedHatchDay = as.integer(lubridate::day(.data$hatching_date_1_may_1_observedMayDate)),
                    minimumHatchYear = as.integer(lubridate::year(.data$hatching_date_1_may_1_minimumMayDate)),
                    minimumHatchMonth = as.integer(lubridate::month(.data$hatching_date_1_may_1_minimumMayDate)),
                    minimumHatchDay = as.integer(lubridate::day(.data$hatching_date_1_may_1_minimumMayDate)),
                    maximumHatchYear = as.integer(lubridate::year(.data$hatching_date_1_may_1_maximumMayDate)),
                    maximumHatchMonth = as.integer(lubridate::month(.data$hatching_date_1_may_1_maximumMayDate)),
                    maximumHatchDay = as.integer(lubridate::day(.data$hatching_date_1_may_1_maximumMayDate))) %>%

      # Identify experiments: in 1984-1987 brood size manipulation at different stages
      dplyr::mutate(treatmentID=dplyr::case_when(.data$year %in% c(1984:1987) & stringr::str_detect(.data$clutch_size,"\\+")~ "clutchsize_enlarged",
                                                 .data$year %in% c(1984:1987) & stringr::str_detect(.data$clutch_size,"\\-")~ "clutchsize_reduced",
                                                 .data$year %in% c(1984:1987) & stringr::str_detect(.data$number_of_hatched_nestlings,"\\+")~ "broodsize_enlarged",
                                                 .data$year %in% c(1984:1987) & stringr::str_detect(.data$number_of_hatched_nestlings,"\\-")~ "broodsize_reduced",
                                                 .data$year %in% c(1984:1987) & stringr::str_detect(.data$the_cause_of_the_nests_death,"experiment") &
                                                   stringr::str_detect(.data$clutch_size,"\\+")==FALSE & stringr::str_detect(.data$clutch_size,"\\-")==FALSE &
                                                   stringr::str_detect(.data$number_of_hatched_nestlings,"\\+")==FALSE & stringr::str_detect(.data$number_of_hatched_nestlings,"\\-")==FALSE &
                                                   .data$number_of_fledlings==0 ~ "simulated_nest_death",
                                                 TRUE~NA_character_))%>%
      # Convert numbers (clutch size, brood size, fledgling number)
      # Values formatted as e.g. "7+4" or "7-4" are interpreted as arithmetic calculations, yielding values of 11 and 3, respectively
      # Values formatted as e.g. "(4)" or "5?" are interpreted as NA
      dplyr::mutate(dplyr::across(.cols = c("clutch_size", "number_of_hatched_nestlings", "number_of_fledlings"),
                                  .fns = ~{

                                    stringr::str_replace_all(.x, pattern = " ", replacement = "")

                                  }),
                    dplyr::across(.cols = c("clutch_size", "number_of_hatched_nestlings", "number_of_fledlings"),
                                  .fns = ~ {

                                    dplyr::case_when(
                                      stringr::str_detect(.x, "\\?") ~ NA_character_,
                                      stringr::str_detect(.x, "\\(") ~ NA_character_,
                                      stringr::str_detect(.x, ".*[:alpha:]+.*") ~ NA_character_,
                                      TRUE ~ .x
                                    )

                                  }),
                    dplyr::across(.cols = c("clutch_size", "number_of_hatched_nestlings", "number_of_fledlings"),
                                  .fns = ~ {

                                    sapply(.x, function(x) eval(parse(text = x)))

                                  }),
                    observedClutchSize = as.integer(.data$clutch_size),
                    observedBroodSize = as.integer(.data$number_of_hatched_nestlings),
                    observedNumberFledged = as.integer(.data$number_of_fledlings),
                    observedClutchType= dplyr::case_when(stringr::str_detect(.data$status_and_re_nesting, "first")~ "first",
                                                         stringr::str_detect(.data$status_and_re_nesting, "repeat")~ "replacement",
                                                         stringr::str_detect(.data$status_and_re_nesting, "second")~ "second",
                                                         stringr::str_detect(.data$status_and_re_nesting, "third")~ "second",
                                                         TRUE~NA_character_))%>%
      dplyr::ungroup()

  # Great tit data
  gt_broods <- gt_data %>%
    # Create female & male IDs
    tidyr::unite(femaleID, females_ring_series, females_ring, remove = FALSE, na.rm = TRUE, sep = "") %>%
    tidyr::unite(maleID, males_ring_series, males_ring, remove = FALSE, na.rm = TRUE, sep = "") %>%
    dplyr::mutate(dplyr::across(.cols = c("femaleID", "maleID"),
                                .fns = ~{

                                  stringr::str_replace_all(dplyr::na_if(.x, ""), pattern = " ", replacement = "")

                                }),
                  # If femaleID & maleID differ from expected format, set to NA
                  femaleID = dplyr::case_when(stringr::str_detect(.data$femaleID, "^[:upper:]{2}[:digit:]{5,6}$") ~ .data$femaleID,
                                              TRUE ~ NA_character_),
                  maleID = dplyr::case_when(stringr::str_detect(.data$maleID, "^[:upper:]{2}[:digit:]{5,6}$") ~ .data$maleID,
                                            TRUE ~ NA_character_)) %>%
    # Convert dates from May days (1 = 1st of May) to year, month, day
    # Days formatted as e.g., "<32", ">32", "32?", or "?"are interpreted as NA
    #"(32)"  are interpreted without the special characters
    # Days formatted as e.g., "32-35" are interpreted as a minimum and maximum; observed dates are taken as the rounded down average
    dplyr::mutate(dplyr::across(.cols = c("start_date_of_laying_1_may_1", "hatching_date_1_may_1"),
                                .fns = ~{

                                  dplyr::case_when(
                                    is.na(.x) ~ NA_character_,
                                    stringr::str_detect(.x, "<") ~ NA_character_,
                                    stringr::str_detect(.x, "<-") ~ NA_character_,
                                    stringr::str_detect(.x, "^\\?") ~ NA_character_,
                                    stringr::str_detect(.x, ".*\\?") ~ NA_character_,
                                    stringr::str_detect(.x, "\\(") ~ stringr::str_extract(.x, "(?<=\\()[:digit:]{1,2}(?=\\))"),
                                    stringr::str_detect(.x, ".-") ~ as.character(floor((as.integer(stringr::str_extract(.x, "[:digit:]{1,2}(?=-)")) + as.integer(stringr::str_extract(.x, "(?<=-)[:digit:]{1,2}"))) / 2)),
                                    TRUE ~ .x
                                  )

                                },
                                .names = "{.col}_observedMayDate"),
                  dplyr::across(.cols = c("start_date_of_laying_1_may_1", "hatching_date_1_may_1"),
                                .fns = ~{

                                  dplyr::case_when(
                                    stringr::str_detect(.x, ".-") ~ stringr::str_extract(.x, "[:digit:]{1,2}(?=-)"),
                                    TRUE ~ NA_character_
                                  )

                                },
                                .names = "{.col}_minimumMayDate"),
                  dplyr::across(.cols = c("start_date_of_laying_1_may_1", "hatching_date_1_may_1"),
                                .fns = ~{

                                  dplyr::case_when(
                                    stringr::str_detect(.x, ".-") ~ stringr::str_extract(.x, "(?<=-)[:digit:]{1,2}"),
                                    TRUE ~ NA_character_
                                  )

                                },
                                .names = "{.col}_maximumMayDate"),
                  dplyr::across(.cols = dplyr::ends_with(c("observedMayDate", "minimumMayDate", "maximumMayDate")),
                                .fns = ~{

                                  lubridate::as_date(x = paste0(.data$year, "-04-30")) + as.integer(.x)

                                }),
                  observedLayYear = dplyr::case_when(is.na(.data$start_date_of_laying_1_may_1_observedMayDate) ~ as.integer(.data$year),
                                                     TRUE ~ as.integer(lubridate::year(.data$start_date_of_laying_1_may_1_observedMayDate))),
                  observedLayMonth = as.integer(lubridate::month(.data$start_date_of_laying_1_may_1_observedMayDate)),
                  observedLayDay = as.integer(lubridate::day(.data$start_date_of_laying_1_may_1_observedMayDate)),
                  minimumLayYear = as.integer(lubridate::year(.data$start_date_of_laying_1_may_1_minimumMayDate)),
                  minimumLayMonth = as.integer(lubridate::month(.data$start_date_of_laying_1_may_1_minimumMayDate)),
                  minimumLayDay = as.integer(lubridate::day(.data$start_date_of_laying_1_may_1_minimumMayDate)),
                  maximumLayYear = as.integer(lubridate::year(.data$start_date_of_laying_1_may_1_maximumMayDate)),
                  maximumLayMonth = as.integer(lubridate::month(.data$start_date_of_laying_1_may_1_maximumMayDate)),
                  maximumLayDay = as.integer(lubridate::day(.data$start_date_of_laying_1_may_1_maximumMayDate)),
                  observedHatchYear = dplyr::case_when(is.na(.data$hatching_date_1_may_1_observedMayDate) ~ as.integer(.data$year),
                                                       TRUE ~ as.integer(lubridate::year(.data$hatching_date_1_may_1_observedMayDate))),
                  observedHatchMonth = as.integer(lubridate::month(.data$hatching_date_1_may_1_observedMayDate)),
                  observedHatchDay = as.integer(lubridate::day(.data$hatching_date_1_may_1_observedMayDate)),
                  minimumHatchYear = as.integer(lubridate::year(.data$hatching_date_1_may_1_minimumMayDate)),
                  minimumHatchMonth = as.integer(lubridate::month(.data$hatching_date_1_may_1_minimumMayDate)),
                  minimumHatchDay = as.integer(lubridate::day(.data$hatching_date_1_may_1_minimumMayDate)),
                  maximumHatchYear = as.integer(lubridate::year(.data$hatching_date_1_may_1_maximumMayDate)),
                  maximumHatchMonth = as.integer(lubridate::month(.data$hatching_date_1_may_1_maximumMayDate)),
                  maximumHatchDay = as.integer(lubridate::day(.data$hatching_date_1_may_1_maximumMayDate))) %>%
    # Convert numbers (clutch size, brood size, fledgling number)
    # Values formatted as e.g. "7+4" or "7-4" are interpreted as arithmetic calculations, yielding values of 11 and 3, respectively
    # Values formatted as e.g. "(4)" or "5?" are interpreted as NA
    dplyr::mutate(dplyr::across(.cols = c("clutch_size_in_brackets_possibly_number_of_eggs",
                                          "number_of_hatched_nestlings_in_brackets_possibly_number_of_nestlings",
                                          "number_of_fledlings"),
                                .fns = ~{

                                  dplyr::case_when(
                                    stringr::str_detect(.x, "\\?") ~ NA_character_,
                                    stringr::str_detect(.x, "\\>") ~ NA_character_,
                                    stringr::str_detect(.x, "\\(") ~ NA_character_,
                                    stringr::str_detect(.x, ".*[:alpha:]+.*") ~ NA_character_,
                                    TRUE ~ .x
                                  )

                                }),
                  dplyr::across(.cols = c("clutch_size_in_brackets_possibly_number_of_eggs",
                                          "number_of_hatched_nestlings_in_brackets_possibly_number_of_nestlings",
                                          "number_of_fledlings"),
                                .fns = ~ {

                                  sapply(.x, function(x) eval(parse(text = x)))

                                }),
                  observedClutchSize = as.integer(.data$clutch_size_in_brackets_possibly_number_of_eggs),
                  observedBroodSize = as.integer(.data$number_of_hatched_nestlings_in_brackets_possibly_number_of_nestlings),
                  observedNumberFledged = as.integer(.data$number_of_fledlings)) %>%
    # Convert clutch type
    dplyr::rename(clutchType = "no_of_brood_1_normal_first_2_normal_second_1_or_2_repeat_after_losing_1_or_2_brood_1_or_2_in_brackets_possibly_first_second_or_repeat_brood") %>%
    dplyr::mutate(observedClutchType = dplyr::case_when(.data$clutchType %in% c("1", "(1)") ~ "first",
                                                        stringr::str_detect(.data$clutchType, "1.*repeat") ~ "replacement",
                                                        stringr::str_detect(.data$clutchType, "[2-3]") ~ "second",
                                                        TRUE ~ NA_character_))

  # Add optional variables
  pf_output <- pf_broods %>%
    {if("breedingSeason" %in% optional_variables) calc_season(data = ., season = .data$year) else .} %>%
    {if("calculatedClutchType" %in% optional_variables) calc_clutchtype(data = ., na.rm = FALSE, protocol_version = "2.0") else .} %>%
    {if("nestAttemptNumber" %in% optional_variables) calc_nestattempt(data = ., season = .data$breedingSeason) else .}

  gt_output <- gt_broods %>%
    {if("breedingSeason" %in% optional_variables) calc_season(data = ., season = .data$year) else .} %>%
    {if("calculatedClutchType" %in% optional_variables) calc_clutchtype(data = ., na.rm = FALSE, protocol_version = "2.0") else .} %>%
    {if("nestAttemptNumber" %in% optional_variables) calc_nestattempt(data = ., season = .data$breedingSeason) else .}

  # Combine pied flycatcher and great tit breeding data
  pf_output <- pf_output %>%
    # Add missing columns
    dplyr::bind_cols(data_templates$v2.0$Brood_data[1, !(names(data_templates$v2.0$Brood_data) %in% names(.))]) %>%
    # Keep only columns that are in the standard format or in the list of optional variables
    dplyr::select(names(data_templates$v2.0$Brood_data), dplyr::contains(names(utility_variables$Brood_data),
                                                                         ignore.case = FALSE))

  gt_output <- gt_output %>%
    # Add missing columns
    dplyr::bind_cols(data_templates$v2.0$Brood_data[1, !(names(data_templates$v2.0$Brood_data) %in% names(.))]) %>%
    # Keep only columns that are in the standard format or in the list of optional variables
    dplyr::select(names(data_templates$v2.0$Brood_data), dplyr::contains(names(utility_variables$Brood_data),
                                                                         ignore.case = FALSE))

  output <- dplyr::bind_rows(pf_output, gt_output) %>%
    # Filter species
    dplyr::filter(speciesID %in% {{species_filter}}) %>%
    dplyr::arrange(.data$observedLayYear, .data$observedLayMonth, .data$observedLayDay, .data$locationID)

  return(output)

}


#' Create capture data table for Mayachino, Russia.
#'
#' Create capture data table in standard format for data from Mayachino, Russia.
#'
#' @param gt_data Data frame. Great tit data from Mayachino, Russia.
#' @param pf_data Data frame. Pied flycatcher data from Mayachino, Russia.
#' @param species_filter Species of interest. The 6-letter code of species as listed in the \href{https://github.com/SPI-Birds/documentation/blob/master/standard_protocol/SPI_Birds_Appendices.pdf}{Appendices} to the standard format. If blank will return all major species.
#' @param optional_variables A character vector of names of optional variables (generated by standard utility functions) to be included in the pipeline output.
#'
#' @return A data frame.
#'

create_capture_MAY <- function(gt_data,
                               pf_data,
                               species_filter,
                               optional_variables) {

  # 1. Retrieve capture information of pied flycatcher parents
  pf_parents <- pf_data %>%
    dplyr::filter(!is.na(.data$year))%>%
    # Create female & male IDs
    tidyr::unite(femaleID, females_ring_series, females_ring, remove = FALSE, na.rm = TRUE, sep = "") %>%
    tidyr::unite(maleID, males_ring_series, males_ring, remove = FALSE, na.rm = TRUE, sep = "") %>%
    dplyr::mutate(dplyr::across(.cols = c("femaleID", "maleID"),
                                .fns = ~{

                                  stringr::str_replace_all(dplyr::na_if(.x, ""), pattern = " ", replacement = "")

                                }),
                  # If femaleID & maleID differ from expected format, set to NA
                  femaleID = dplyr::case_when(stringr::str_detect(.data$femaleID, "^[:upper:]{0,2}[:digit:]{5,6}$") ~ .data$femaleID,
                                              TRUE ~ NA_character_),
                  maleID = dplyr::case_when(stringr::str_detect(.data$maleID, "^[:upper:]{0,2}[:digit:]{5,6}$") ~ .data$maleID,
                                            TRUE ~ NA_character_),
                  captureDate = NA_character_,
                  captureYear = as.integer(.data$year),
                  captureMonth = NA_integer_,
                  captureDay = NA_integer_) %>%
    # Pivot information on females and males into rows
    tidyr::pivot_longer(cols = c("femaleID", "maleID"),
                        names_to = "sex",
                        values_to = "individualID") %>%
    # Remove unknown individualIDs
    dplyr::filter(!is.na(.data$individualID)) %>%
    dplyr::mutate(observedSex = dplyr::case_when(grepl(pattern = "^f", x = .data$sex) ~ "F",
                                                 grepl(pattern = "^m", x = .data$sex) ~ "M"),
                  age = dplyr::case_when(.data$observedSex == "F" ~ .data$females_age,#Age was directly provided in calendar years based on plumage at first capture or year of hatching.
                                         .data$observedSex == "M" ~ .data$males_age),
                  age = dplyr::case_when(.data$age == "registered earlier" ~ NA_character_,
                                         TRUE ~ .data$age),
                  # Add measurements for create_measurement_MAY()
                  wingLength = dplyr::case_when(.data$observedSex == "F" ~ .data$female_wing_length,
                                                .data$observedSex == "M" ~ .data$male_wing_length),
                  wingLength = dplyr::case_when(.data$wingLength == "registered earlier" ~ NA_character_,
                                                TRUE ~ .data$wingLength),
                  wingLength = as.numeric(.data$wingLength),
                  tarsus = dplyr::case_when(.data$observedSex == "F" ~ as.character(.data$female_tarsus_length),
                                            .data$observedSex == "M" ~ .data$male_tarsus_length),
                  tarsus = dplyr::case_when(.data$tarsus == "registered earlier" ~ NA_character_,
                                            TRUE ~ .data$tarsus),
                  tarsus = as.numeric(.data$tarsus),
                  molt = dplyr::case_when(.data$observedSex == "F" ~ as.character(.data$female_molt_stage_number_of_shedding_primary_feathers),
                                          .data$observedSex == "M" ~ .data$male_molt_stage_number_of_shedding_primary_feathers),
                  molt = dplyr::case_when(.data$molt == "registered earlier" ~ NA_character_,
                                          TRUE ~ .data$molt),
                  molt = as.integer(.data$molt),
                  moltDate = dplyr::case_when(.data$observedSex == "F" ~ .data$date_of_female_molt_survey,
                                              .data$observedSex == "M" ~ .data$date_of_male_molt_survey),
                  drost = dplyr::case_when(.data$observedSex == "F" ~ NA_character_,
                                           .data$observedSex == "M" ~ .data$drost_score),
                  drost = dplyr::case_when(.data$drost == "registered earlier" ~ NA_character_,
                                          TRUE ~ .data$drost),
                  drost = as.numeric(.data$drost),
                  chickAge = NA_integer_)

  # 2. Retrieve capture information of pied flycatcher chicks
  message("Completing sequence of pied flycatcher chick IDs")
  pb_pf <- progress::progress_bar$new(total = nrow(pf_data))

  pf_chicks <- pf_data %>%
    # Translate incomplete chickID notation to complete chick IDs
    dplyr::mutate(individualID = purrr::map(.x = .data$nestling_rings,
                                            .f = ~{

                                              pb_pf$tick()

                                              retrieve_chickIDs_MAY(.x)

                                            })) %>%
    # Unnest to long format (i.e., each chick in a row)
    tidyr::unnest(cols = individualID) %>%
    # Still some errors may persist. Check format used for femaleID/maleID
    dplyr::mutate(individualID = dplyr::case_when(stringr::str_detect(.data$individualID,
                                                                      "^[:upper:]{0,2}[:digit:]{5,6}$") ~ .data$individualID,
                                                  TRUE ~ NA_character_)) %>%
    # Remove unknown individualIDs
    dplyr::filter(!is.na(.data$individualID)) %>%
    dplyr::mutate(captureDate = NA_character_,
                  captureYear = as.integer(.data$year),
                  captureMonth = NA_integer_,
                  captureDay = NA_integer_,
                  observedSex = NA_character_,
                  chickAge = NA_integer_,
                  age = "chick")

  # 3. Retrieve capture information of great tit parents
  gt_parents <- gt_data %>%
    dplyr::filter(!is.na(gt_data$year))%>%
    # Create female & male IDs
    tidyr::unite(femaleID, females_ring_series, females_ring, remove = FALSE, na.rm = TRUE, sep = "") %>%
    tidyr::unite(maleID, males_ring_series, males_ring, remove = FALSE, na.rm = TRUE, sep = "") %>%
    dplyr::mutate(dplyr::across(.cols = c("femaleID", "maleID"),
                                .fns = ~{

                                  stringr::str_replace_all(dplyr::na_if(.x, ""), pattern = " ", replacement = "")

                                }),
                  # If femaleID & maleID differ from expected format, set to NA
                  femaleID = dplyr::case_when(stringr::str_detect(.data$femaleID, "^[:upper:]{2}[:digit:]{5,6}$") ~ .data$femaleID,
                                              TRUE ~ NA_character_),
                  maleID = dplyr::case_when(stringr::str_detect(.data$maleID, "^[:upper:]{2}[:digit:]{5,6}$") ~ .data$maleID,
                                            TRUE ~ NA_character_),
                  captureDate = NA_character_,
                  captureYear = as.integer(.data$year),
                  captureMonth = NA_integer_,
                  captureDay = NA_integer_) %>%
    # Pivot information on females and males into rows
    tidyr::pivot_longer(cols = c("femaleID", "maleID"),
                        names_to = "sex",
                        values_to = "individualID") %>%
    # Remove unknown individualIDs
    dplyr::filter(!is.na(.data$individualID)) %>%
    # Rename long variables
    dplyr::rename(femaleAge = "females_age_1_one_year_old_bird_hatched_last_breeding_season_2_two_or_more_years_old_an_adult_hatched_before_the_last_calendar_year_3_or_4_age_3_4_or_more_years",
                  maleAge = "males_age_1_one_year_old_bird_hatched_last_breeding_season_2_two_or_more_years_old_an_adult_hatched_before_the_last_calendar_year_3_or_4_age_3_4_or_more_years") %>%
    dplyr::mutate(observedSex = dplyr::case_when(grepl(pattern = "f", x = .data$sex) ~ "F",
                                                 grepl(pattern = "m", x = .data$sex) ~ "M"),
                  age = dplyr::case_when(.data$observedSex == "F" ~ .data$femaleAge,  #Age was directly provided in calendar years based on plumage at first capture or year of hatching.
                                         .data$observedSex == "M" ~ .data$maleAge),
                  age = dplyr::case_when(.data$age == "registered earlier this season" ~ NA_character_,
                                         TRUE ~ .data$age),
                  chickAge = NA_integer_)

  # 4. Retrieve capture information of great tit chicks
  message("Completing sequence of great tit chick IDs")
  pb_gt <- progress::progress_bar$new(total = nrow(gt_data))

  gt_chicks <- gt_data %>%
    # Translate incomplete chickID notation to complete chick IDs
    dplyr::mutate(individualID = purrr::map(.x = .data$nestling_rings,
                                            .f = ~{

                                              pb_gt$tick()

                                              retrieve_chickIDs_MAY(.x)

                                            })) %>%
    # Unnest to long format (i.e., each chick in a row)
    tidyr::unnest(cols = .data$individualID) %>%
    # Still some errors may persist. Check format used for femaleID/maleID
    dplyr::mutate(individualID = dplyr::case_when(stringr::str_detect(.data$individualID,
                                                                      "^[:upper:]{0,2}[:digit:]{5,6}$") ~ .data$individualID,
                                      TRUE ~ NA_character_)) %>%
    # Remove unknown individualIDs
    dplyr::filter(!is.na(individualID)) %>%
    dplyr::mutate(captureDate = NA_character_,
                  captureYear = as.integer(.data$year),
                  captureMonth = NA_integer_,
                  captureDay = NA_integer_,
                  observedSex = NA_character_,
                  chickAge = NA_integer_,
                  age = "chick")

  # 5. Combine capture tables
  captures <- dplyr::bind_rows(pf_parents, pf_chicks, gt_parents, gt_chicks) %>%
    dplyr::mutate(captureSiteID = .data$siteID,
                  releaseSiteID = .data$siteID,
                  capturePlotID = .data$plotID,
                  releasePlotID = .data$plotID,
                  captureLocationID = .data$locationID,
                  releaseLocationID = .data$locationID,
                  captureAlive = TRUE,
                  releaseAlive = TRUE,
                  capturePhysical = TRUE) %>%
    # Arrange chronologically for each individual
    dplyr::arrange(.data$individualID, .data$captureYear, .data$captureMonth, .data$captureDay) %>%
    dplyr::group_by(.data$individualID) %>%
    # First captures are assumed to be tagging events, and thus captureTagID = NA.
    # NB: Only add tag numbers if full tag number (letters + numbers) are recorded#Probably not a good idea as you get >50% individuals with no ID for a column that does not allow NAs
    dplyr::mutate(captureTagID = dplyr::case_when(dplyr::row_number() == 1 ~ NA_character_,
                                                       #stringr::str_detect(.data$individualID,"^[:digit:]") ~ NA_character_,
                                                       TRUE ~ .data$individualID),
                  # All releases are alive (also see releaseAlive), so no NAs in releaseTagID
                  releaseTagID =.data$individualID) %>%
    dplyr::ungroup() %>%
    # Filter species
    dplyr::filter(speciesID %in% {{species_filter}}) %>%

    # Create captureID
    dplyr::group_by(.data$individualID) %>%
    dplyr::mutate(captureID = paste(.data$individualID, 1:dplyr::n(), sep = "_")) %>%
    dplyr::ungroup() %>%
    dplyr::select("captureID", everything())

  # 6. Add optional variables
  output <- captures %>%
    {if("exactAge" %in% optional_variables | "minimumAge" %in% optional_variables) calc_age(data = .,############This doesn't work anymore because the function uses capture date
                                                                                            Age = .data$age,
                                                                                            Year = .data$captureYear,
                                                                                            protocol_version = "2.0") %>%
        dplyr::select(tidyselect::any_of(c(names(captures), optional_variables))) else .}

  return(output)

}

#' Create individual data table for Mayachino, Russia.
#'
#' Create individual data table in standard format for data from Mayachino, Russia.
#'
#' @param capture_data Data frame. Output from \code{\link{create_capture_MAY}}.
#' @param species_filter Species of interest. The 6-letter code of species as listed in the \href{https://github.com/SPI-Birds/documentation/blob/master/standard_protocol/SPI_Birds_Appendices.pdf}{Appendices} to the standard format. If blank will return all major species.
#' @param optional_variables A character vector of names of optional variables (generated by standard utility functions) to be included in the pipeline output.
#'
#' @return A data frame.
#'

create_individual_MAY <- function(capture_data,
                                  species_filter,
                                  optional_variables) {

  # Create a list of individuals from capture data
  individuals <- capture_data %>%
    # Convert ages to stages
    # According to data custodian:
    # - 1: one year old, hatched previous breeding season
    # - 2-6: X or more years old
    dplyr::mutate(age = dplyr::case_when(.data$age == "chick" ~ "chick",
                                         .data$age == "1" ~ "subadult",
                                         TRUE ~ "adult")) %>%
    # Arrange data for each individual chronologically
    dplyr::arrange(.data$individualID, .data$captureYear) %>%
    # For every individual ...
    dplyr::group_by(.data$individualID) %>%
    # ... determine first stage, brood, tag year, month, day, and tag site of each individual
    dplyr::summarise(firstBrood = dplyr::first(.data$broodID),
                     tagStage = dplyr::first(.data$age),
                     tagDate = NA_character_,
                     tagYear = dplyr::first(.data$captureYear),
                     tagSiteID = dplyr::first(.data$siteID),
                     speciesID = dplyr::case_when(length(unique(.data$speciesID)) == 2 ~ "CCCCCC",
                                                  TRUE ~ dplyr::first(.data$speciesID))) %>%
    dplyr::mutate(tagMonth = NA_integer_,
                  tagDay = NA_integer_,
                  # Only assign a brood ID if they were first caught as a chick
                  broodIDLaid = dplyr::case_when(tagStage != "chick" ~ NA_character_,
                                                 TRUE ~ .data$firstBrood),
                  # We have no information on cross-fostering, so we assume the brood laid and ringed are the same
                  broodIDFledged = .data$broodIDLaid) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(studyID = "MAY-1",
                  siteID = "MAY") %>%
    # Filter species
    dplyr::filter(speciesID %in% {{species_filter}})

  # Add optional variables
  output <- individuals %>%
    {if("calculatedSex" %in% optional_variables) calc_sex(individual_data = .,
                                                          capture_data = capture_data) else .}

  return(output)

}

#' Create location data table for Mayachino, Russia.
#'
#' Create location data table in standard format for data from Mayachino, Russia.
#'
#' @param gt_data Data frame. Great tit data from Mayachino, Russia.
#' @param pf_data Data frame. Pied flycatcher data from Mayachino, Russia.
#'
#' @return A data frame.
#'

create_location_MAY <- function(gt_data,
                                pf_data) {

  # Combine great tit and pied flycatcher location columns
  data <- pf_data %>%
    dplyr::select("studyID", "siteID", "plotID", "locationID", "year") %>%
    dplyr::bind_rows({gt_data %>% dplyr::select("siteID", "plotID", "locationID", "year")})

  # There are no coordinates or box type information for now...waiting for it
  locations <- data %>%
    #dplyr::select("studyID", "siteID", "plotID", "locationID") %>%
    tidyr::drop_na() %>%
    dplyr::mutate(locationType = "nest",
                  decimalLatitude = NA_real_,
                  decimalLongitude = NA_real_,
                  startYear = as.integer(min(.data$year)),
                  endYear = NA_integer_,
                  habitatID = NA_character_) %>%
    dplyr::select(-year)%>%
    dplyr::distinct()

  return(locations)

}


#' Create measurement data table for Mayachino, Russia.
#'
#' Create measurement data table in standard format for data from Mayachino, Russia.
#'
#' @param capture_data Data frame. Output from \code{\link{create_capture_MAY}}.
#'
#' @return A data frame.
#'

create_measurement_MAY <- function(capture_data) {

  # Measurements are only taken of individuals (during captures), not of locations,
  # so we use capture_data as input
  measurements <- capture_data %>%#
    dplyr::select(recordID = "captureID",
                  "studyID",
                  siteID = "captureSiteID",
                  measurementDeterminedYear = "captureYear",
                  measurementDeterminedMonth = "captureMonth",
                  measurementDeterminedDay = "captureDay",
                  "tarsus",
                  "wingLength",
                  "molt",
                  "moltDate",
                  plumageColour = "drost") %>%
    # Measurements in Capture data are stored as columns, but we want each individual measurement as a row
    # Therefore, we pivot each separate measurement of an individual to a row
    # NAs are removed
    tidyr::pivot_longer(cols = c("tarsus", "wingLength", "molt", "plumageColour"),
                        names_to = "measurementType",
                        values_to = "measurementValue",
                        values_drop_na = TRUE) %>%
    dplyr::mutate(measurementID = 1:dplyr::n(),
                  measurementSubject = "capture",
                  measurementUnit = dplyr::case_when(.data$measurementType == "plumageColour" ~ NA_character_,
                                                     .data$measurementType == "molt" ~ "number of shedding primary feathers",
                                                     TRUE ~ "mm"),
                  measurementMethod = dplyr::case_when(.data$measurementType == "tarsus" ~ "alternative",
                                                       .data$measurementType == "plumageColour" ~ "drost score",
                                                       TRUE ~ NA_character_),
                  # Convert measurementType from camel case to lower case & space-separated
                  # (e.g., wingLength -> wing length)
                  measurementType = tolower(gsub("([[:upper:]])", " \\1", .data$measurementType)),
                  # Use different date column for measurement date for molt scores
                  measurementDeterminedYear = dplyr::case_when(.data$measurementType == "molt" & !is.na(.data$moltDate)~ as.integer(lubridate::year(.data$moltDate)),
                                                               TRUE ~ .data$measurementDeterminedYear),
                  measurementDeterminedMonth = dplyr::case_when(.data$measurementType == "molt" & !is.na(.data$moltDate)~ as.integer(lubridate::month(.data$moltDate)),
                                                                TRUE ~ .data$measurementDeterminedMonth),
                  measurementDeterminedDay = dplyr::case_when(.data$measurementType == "molt"& !is.na(.data$moltDate) ~ as.integer(lubridate::day(.data$moltDate)),
                                                              TRUE ~ .data$measurementDeterminedDay)) %>%

    dplyr::arrange(.data$measurementDeterminedYear,
                   .data$measurementDeterminedMonth,
                   .data$measurementDeterminedDay)

  return(measurements)

}


#' Create experiment data table for Mayachino, Russia.
#'
#' Create experiment data table in standard format for data from Mayachino, Russia.
#'
#' @param brood_data Data frame. Output from \code{\link{create_brood_MAY}}.
#'
#' @return A data frame.
#'

create_experiment_MAY <- function(brood_data) {

  experiments <- brood_data %>%
    dplyr::filter(!is.na(.data$treatmentID)) %>%
    dplyr::select("treatmentID",
                  "observedLayYear",
                  "studyID",
                  "siteID")%>%
    dplyr::mutate(experimentType="clutch size or brood size manipulation",
                  treatmentDetails= dplyr::case_when(.data$treatmentID=="broodsize_enlarged"~"added nestlings to the nest",
                                                         .data$treatmentID=="broodsize_reduced"~"removed nestlings from the nest",
                                                         .data$treatmentID=="clutchsize_enlarged"~"added eggs to the nest",
                                                         .data$treatmentID=="clutchsize_reduced"~ "removed eggs from the nest",
                                                         .data$treatmentID=="simulated_nest_death"~ "all eggs or nestlings removed from the nest"),
                  treatmentStartYear = as.integer(1984),
                  treatmentEndYear = as.integer(1987))%>%
    dplyr::select(-observedLayYear)%>%
    dplyr::distinct()

  return(experiments)

}


#' Retrieve chick IDs in MAY pipeline
#'
#' In MAY primary data, the chick IDs in a brood are stored as series of partially incomplete character sequences (e.g., "856840,1,55-62", "099362-65"). This function extracts the full sequence of characters for each ID in the series. "-" are interpreted as a range; "," are interpreted as a regular separator. Values in other formats (e.g., "without a rings", "531094.95999999996") and sequences that lead to an excessive number of IDs (e.g. "54522-291") are set to NA.
#'
#' @param chickID Character. The series of partially incomplete chick IDs of a brood.
#'
#' @return A vector with the complete chick IDs of a brood, or NA.
#'
#' @export
#'
#' @examples
#'
#' retrieve_chickIDs_MAY("856840,1,55-62")
#' retrieve_chickIDs_MAY("XV 54798-800; XE 63101")
#' retrieve_chickIDs_MAY("177497-0+XB721822-3")
#' #NOTE: a lot of duplicate ring numbers in pf chicks (385) and 1 in GT. This is probably due to missing ring series letters.

retrieve_chickIDs_MAY <- function(chickID) {

  # Replace spaces between two numbers by comma
  chickID <- stringr::str_replace_all(chickID, pattern = "(?<=[:digit:])[:space:](?=[:digit:])", replacement = ",")

  #replace space between two series that have different letters by a comma
  chickID <- stringr::str_replace_all(chickID, pattern = "(?<=[:digit:])[:space:](?=[:alpha:])", replacement = ",")

  #Fix potential double dashes e.g. "XJ 98280-86-29" (happened only once so far): remove the extra dash and 2 digits
  if(!is.na(chickID) & stringr::str_detect(chickID, pattern = "[\\-][:digit:]{2}[\\-][:digit:]{2}")==TRUE){
    chickID <- stringr::str_extract_all(chickID, pattern = "[:alpha:]{2}[:space:][:digit:]{5}[\\-][:digit:]{2}")
  }

  # Remove spaces, and brackets
  chickID <- stringr::str_remove_all(chickID, pattern = " ")
  chickID <- stringr::str_remove_all(chickID, pattern = "\\(.*\\)")

  #Remove everything that follows a "+" if followed by "without"or Cyrillic characters, otherwise replace + by a comma
  chickID.tmp <- strsplit(chickID,"\\+")[[1]]
  if(!is.na(chickID) & sum(stringr::str_detect(chickID.tmp,pattern="without")) + sum(stringr::str_detect(chickID.tmp,pattern="[\\p{Cyrillic}]"))>0) {
    chickID <- strsplit(chickID,"\\+")[[1]][1]
  }

  chickID <- stringr::str_replace_all(chickID, pattern = "\\+", replacement = ",")
  # Replace semicolons and pluses by commas
  chickID <- stringr::str_replace_all(chickID, pattern = ";", replacement = ",")

  #replace 00 or 000 by 0
  chickID <- stringr::str_replace_all(chickID, pattern = "-00", replacement = "-0")
  chickID <- stringr::str_replace_all(chickID, pattern = "-000", replacement = "-0")

  # Set chickID to NA if format is not a series, i.e.,
  # - if it does not contain "-" or ","
  # - if it contains ".", "without" or cyrillic characters
  # - if it is NA
  na_strings <- c("without", "\\.", "[\\p{Cyrillic}]")

  if(stringr::str_detect(chickID, "[-,]", negate = TRUE) | stringr::str_detect(chickID, paste(na_strings, collapse="|")) | is.na(chickID)) {

    output <- NA

    # Else, retrieve series of complete chick IDs
  } else {


    # Extract starting letters, if present
    id_letters <- stringr::str_extract_all(chickID, "[:alpha:]{1,2}")
    id_numbers <- stringr::str_remove_all(chickID, "[:alpha:]{1,2}")

    # Split series of chick IDs
    id_series <- stringr::str_split(id_numbers, pattern = "[-,]")[[1]]

    # Extract special characters
    special_chars <- str_extract_all(chickID, "[-,]")[[1]]

    # Determine reference ID(s)
    id_ref <- map_dbl(2:length(id_series), ~{

      if(any(nchar(id_series) > nchar(id_series[.x]))) {

        ref <- which(nchar(id_series) > nchar(id_series[.x]))
        max(ref[.x > ref])

      } else {

        ref <- 1

      }

    })

    id_ref <- unique(c(1, id_ref))

    # Pad incomplete IDs based on reference ID(s)
    # NB: If two reference IDs are found, first use the latter
    #     For example, in the case of "79699-700,157-60",
    #     "60" is first matched with "157",
    #     before all values are matched to "79699"
    if(length(id_ref) > 1) {

      id_series <- purrr::map_chr(.x = seq_len(length(id_series)),
                                  .f = ~ {

                                    # IDs that appear after the second reference ID are matched to that reference ID
                                    # if the number of characters in that ID are smaller than the number of characters
                                    # in the reference ID
                                    # For example, in the case of 580662,95-700",
                                    # "700" should not be matched to "95" (as it is likely meant to be "695"),
                                    # but rather to the first reference ID
                                    if(.x > id_ref[2] & nchar(id_series[.x]) < nchar(id_series[id_ref[2]])) {

                                      ref_length <- nchar(id_series[id_ref[2]])
                                      no_length <- nchar(id_series[.x])

                                      paste0(stringr::str_sub(id_series[id_ref[2]], start = 1, end = ref_length - no_length),
                                             id_series[.x])

                                      # IDs that appear before the second reference ID remain untouched
                                    } else {

                                      id_series[.x]

                                    }

                                  })

    }

    # Now pad all IDs to the first reference ID
    new_series <- purrr::map_chr(.x = seq_len(length(id_series)),
                                 .f = ~ {

                                   ref_length <- nchar(id_series[id_ref[1]])
                                   no_length <- nchar(id_series[.x])
                                   if (no_length==1 & id_series[.x]==0){#special case when the next number ends a 0  like in 999-0 (need to make sure the ring series goes up and not down)
                                       as.character(seq(as.numeric(id_series[id_ref[1]]),as.numeric(id_series[id_ref[1]]) +14 ))[min(grep("0$",as.character(seq(as.numeric(id_series[id_ref[1]]),as.numeric(id_series[id_ref[1]]) +14 ))))]#add 14 and find the first number that ends with 0
                                   }

                                   else
                                   paste0(stringr::str_sub(id_series[1], start = 1, end = ref_length - no_length),
                                          id_series[.x])

                                 })


    # Zip chick ID vector with special character vector
    # Replace "-" by ":"
    new_string <- paste0("c(",stringr::str_replace_all(stringr::str_flatten(c(new_series, special_chars)[order(c(seq_along(new_series), seq_along(special_chars)))]), pattern = "-", replacement = ":"),")")

    # Parse text to retrieve sequence of chick IDs
    new_ids <- eval(parse(text = new_string))

    # Add zero if chick IDs started with 0
    output <- stringr::str_pad(as.character(new_ids), width = nchar(id_series[1]), side = "left", pad = 0)

    # Add letters to each ID, if present
    if(!is.na(id_letters) & length(id_letters[[1]])==1) {

      output <- paste0(id_letters, output)

    }

    if(!is.na(id_letters) & length(id_letters[[1]])>1) {#if different series use different letters, map the letters back to the numbers using the first 2 digits

      series2digits<-stringr::str_split(id_numbers, pattern = ",")[[1]]
      series2digits<-sapply(series2digits,function(x) stringr::str_extract_all(x, "[\\d]{2}")[[1]][1])
      x<-1:length(id_letters[[1]])
      output<-unlist(sapply(x,function(x) paste0(id_letters[[1]][x],output[which(stringr::str_detect(output,series2digits[x]))]) ))


    }

  }

  # Set chick IDs to NA if the number of IDs is unlikely large
  if(length(output) > 15) {

    output <- NA

  }

  return(output)

}


#----------------------#

# TODO: Check individual IDs: many missing "ring series" letters
# TODO: Check chick rings with data custodian (missing letters?)
# TODO: Check location info: location type, start year of boxes, coordinates, habitat type

