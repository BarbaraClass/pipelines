#'Construct standard format for data from MTA, Hungary (VES and SZE)
#'
#'A pipeline to produce the standard format for the nest box population in VES and SZE, Hungary, administered by Gabor Seress
#'
#'This section provides details on data management choices that are unique to
#'this data. For a general description of the standard format please see
#'\href{https://github.com/SPI-Birds/documentation/blob/master/standard_protocol/SPI_Birds_Protocol_v1.1.0.pdf}{here}.
#'
#'\strong{BroodID}:
#'@inheritParams pipeline_params
#'
#'@return Generates either 4 .csv files or 4 data frames in the standard format.
#'@export
#'

format_MTA <- function(db = choose_directory(),
                       path = ".",
                       species = NULL,
                       pop = NULL,
                       output_type = 'R'){

  #Force choose_directory() if used
  #force(db)

  #Assign to database location
  db <- paste0(gsub("\\\\", "/", db), "\\MTA_PrimaryData.xlsx")

  #### Determine species and population codes for filtering
  if(is.null(species)){

    species_filter <- species_codes$Species

  } else {

    species_filter <- species

  }

  if(is.null(pop)){

    pop_filter <- pop_codes$PopID

  } else {

    pop_filter <- pop

  }

  ## Start time
  start_time <- Sys.time()

  message("Importing primary data...")

  ## Set options
  options(dplyr.summarise.inform = FALSE)


  #########################################################
  ###Get primary data in a form that can be used, fix bugs
  #########################################################

  ## 1- Get brood information from primary data

  brood_data <- readxl::read_xlsx(path = db, guess = 5000,sheet= "brood_data", na= "NA") %>%
    tibble::as_tibble()%>%
    dplyr::transmute(BroodID= as.character(.data$brood_id),
                  PopID = as.character(case_when(site == "Szentgal_erdo" ~ "SZE",
                                    site == "Veszprem" ~ "VES",
                                    TRUE ~ site)),

                  BreedingSeason=as.integer(.data$year),
                  Species=as.character(.data$species),
                  Plot=NA_character_,
                  LocationID=as.character(paste(.data$nestbox_new, .data$year,sep="_")),
                  FemaleID=as.character(.data$mother_ID),
                  MaleID=as.character(.data$father_ID),
                  ClutchType_observed = NA_character_,
                  LayDate_observed = as.Date(.data$firstegg_date),
                  LayDate_min = as.Date(NA),
                  LayDate_max = as.Date(NA),
                  ClutchSize_observed = as.integer(case_when(.data$clutch_size_comment==1~.data$clutch_size,
                                                             TRUE~NA)),
                  ClutchSize_min = NA_integer_,
                  ClutchSize_max = NA_integer_,
                  HatchDate_observed = as.Date(NA),#apparently not available?
                  HatchDate_min = as.Date(NA),
                  HatchDate_max = as.Date(NA),
                  BroodSize_observed = NA_integer_,
                  BroodSize_min = NA_integer_,
                  BroodSize_max = NA_integer_,
                  FledgeDate_observed = as.Date(NA),
                  FledgeDate_min = as.Date(NA),
                  FledgeDate_max = as.Date(NA),
                  NumberFledged_observed = as.integer(.data$fledged),
                  NumberFledged_min = NA_integer_,
                  NumberFledged_max = NA_integer_,
                  AvEggMass = NA_real_,
                  NumberEggs = NA_integer_,
                  OriginalTarsusMethod = "Alternative",
                  ExperimentID = NA_character_) %>%# Add the calculated clutch type later
    dplyr::filter(is.na(ClutchSize_observed) |ClutchSize_observed>0)%>%
    dplyr::filter(!(is.na(.data$ClutchSize_observed)&#remove nests in which nothing happened
                      is.na(.data$LayDate_observed)&
                      is.na(NumberFledged_observed)))%>%
    dplyr::ungroup()

  ## 2- Get capture information from primary data

  capture_data <- readxl::read_xlsx(path = db, guess = 5000,sheet= "ring_data", na= "NA") %>%
    tibble::as_tibble()%>%
    dplyr::transmute(PopID= as.character(case_when(site == "Szentgal_erdo" ~ "SZE",
                                                   site == "Veszprem" ~ "VES",
                                                   TRUE ~ site)),
                     IndvID=as.character(.data$ringid),
                     Species=as.character(.data$species),
                     Sex_observed=as.character(case_when(.data$sex == "female" ~ "F", .data$sex == "male" ~ "M",TRUE ~ .data$sex)),
                     BreedingSeason=as.integer(.data$year),
                     CaptureDate=as.Date(.data$date),
                     CaptureTime= NA_character_,
                     ObserverID=as.character(.data$observer),
                     LocationID=as.character(paste(.data$nestbox_new, .data$year,sep="_")),
                     CaptureAlive=as.logical(NA),
                     ReleaseAlive=as.logical(NA),
                     CapturePopID=PopID,
                     CapturePlot=NA_character_,
                     ReleasePopID=PopID,
                     ReleasePlot=NA_character_,
                     Mass=.data$mass,
                     Tarsus=.data$tarsus,
                     OriginalTarsusMethod="Alternative",#check with data custodian
                     WingLength=.data$wing,
                     Age_observed=as.integer(case_when(.data$age == "pull" ~ "1", .data$age == "1y" ~ "3", .data$age == "1+" ~ "4", .data$age == "2y" ~ "5",
                                            .data$age == "2+" ~ "6", .data$age == "F" ~ NA_character_, TRUE ~ NA_character_)),#check with data custodian
                     ChickAge=as.integer(NA),#NA because hatching date is unknown
                     ExperimentID=NA_character_,
                     BroodID=as.character(.data$brood_id))%>%#check with data custodian
  dplyr::ungroup()#Very few ringed chicks (118). Is this normal?

  ### 3- Get location information from primary data

  location_data_SZE <- readxl::read_xlsx(path = db, guess = 5000,sheet= "coord_data_Szg", na= "NA") %>%
    tibble::as_tibble()%>%
    dplyr::mutate(PopID="SZE",
                  HabitatType="deciduous")

  location_data_VES <- readxl::read_xlsx(path = db, guess = 5000,sheet= "coord_data_Vp", na= "NA") %>%
    tibble::as_tibble()%>%
    dplyr::mutate(PopID="VES",
                  HabitatType="urban")



  ###########################################
  ###Define all the functions
  ##########################################

  #' Create brood data table for  MTA, Hungary
  #'
  #' @param brood_data Brood data compiled from primary data from MTA, Hungary
  #'
  #' @param capture_data Ringing data compiled from primary data from MTA, Hungary
  #'
  #' @return A data frame.

  create_brood_MTA   <- function(brood_data,capture_data) {
    ## Create brood data
    Brood_data_temp <- brood_data %>%

      ## Get chick summary stats
      dplyr::left_join(capture_data %>%
                         dplyr::filter(.data$Age_observed==1) %>%
                         dplyr::select(BroodID,
                                       IndvID,
                                       Mass,
                                       Tarsus) %>%

                         ## For each brood, get summary stats
                         dplyr::group_by(.data$BroodID) %>%
                         dplyr::summarise(AvgChickMass = round(mean(Mass, na.rm = T), 1),
                                          NumberChicksMass = sum(!is.na(.data$Mass)),
                                          AvgTarsus = round(mean(Tarsus, na.rm = T), 2),
                                          NumberChicksTarsus = sum(!is.na(.data$Tarsus))) %>%

                         ## Replace NaNs and 0 with NA
                         dplyr::mutate(dplyr::across(c(AvgChickMass,AvgTarsus), ~dplyr::na_if(., NaN)))) %>%

      ##Add calculated clutch type
      dplyr::mutate(ClutchType_calculated = calc_clutchtype(data = ., na.rm = FALSE, protocol_version = "1.1"))%>%

      tibble::as_tibble()

    return(Brood_data_temp)

  }

  #' Create capture data table in standard format for data from MTA, Hungary
  #'
  #' @param capture_data Data frame. Primary data from ringing records.
  #'
  #' @return A data frame.

  create_capture_MTA <- function(capture_data) {

    ## Captures from ringing data
    Capture_data_temp <- capture_data %>%
      ## Remove NAs from key columns
      dplyr::filter_at(dplyr::vars( CapturePopID,
                                    IndvID,
                                    BreedingSeason,
                                    Species,
                                    CaptureDate),
                       dplyr::all_vars(!is.na(.))) %>%
      ## Create capture  ID
      dplyr::arrange(.data$BreedingSeason,
                     .data$IndvID,
                     .data$CaptureDate) %>%
      dplyr::group_by(.data$IndvID) %>%
      dplyr::mutate(CaptureID = paste(.data$IndvID, dplyr::row_number(), sep = "_"))%>%

      #Add age_calculated
      calc_age(ID = .data$IndvID, Age = .data$Age_observed,
               Date = .data$CaptureDate, Year = .data$BreedingSeason) %>%


      return(Capture_data_temp)

  }

  #' Create individual data table in standard format for data from MTA, Hungary
  #'
  #' @param Capture_data_temp Data frame. Output from \code{\link{create_capture_MTA}}.
  #'
  #' @return A data frame.


  create_individual_MTA <- function(Capture_data_temp) {

    ## Create individual data from capture data
    Individual_data_temp <- Capture_data_temp %>%

      ## Arrange
      dplyr::arrange(.data$IndvID, .data$CaptureDate) %>%

      #### Format and create new data columns
      dplyr::group_by(.data$IndvID, .data$CapturePopID) %>%
      dplyr::mutate(PopID = .data$CapturePopID) %>%
      dplyr::group_by(.data$IndvID) %>%
      dplyr::mutate(Species = .data$Species,
                    Sex_calculated = purrr::map_chr(.x = list(unique(stats::na.omit(.data$Sex_observed))),
                                                    .f = ~{
                                                      if(length(..1) == 0){
                                                        return(NA_character_)
                                                      } else if(length(..1) == 1){
                                                        return(..1)
                                                      } else {
                                                        return("C")
                                                      }
                                                    }),
                    Sex_genetic = NA_character_,
                    RingSeason = min(.data$BreedingSeason, na.rm = T),
                    RingAge = purrr::pmap_chr(.l = list(dplyr::first(.data$Age_observed)),
                                              .f = ~{
                                                if(is.na(..1)){
                                                  return("adult")
                                                } else if(..1 <= 3L){
                                                  return("chick")
                                                } else if(..1 > 3L){
                                                  return("adult")
                                                }
                                              }),
                    BroodIDLaid = case_when(RingAge=="chick"~as.character(.data$BroodID),
                                            TRUE~NA_character_),
                    BroodIDFledged = BroodIDLaid) %>%

      ## Keep distinct records by PopID and InvdID
      dplyr::distinct(.data$PopID, .data$IndvID, .keep_all = TRUE) %>%
      dplyr::ungroup() %>%

      ## Reorder columns
      dplyr::select(dplyr::any_of(names(individual_data_template)), dplyr::everything())

    return(Individual_data_temp)

  }

  #' Create location data table in standard format for data from MTA, Hungary

  #' @param location_data_SZE Data frame. Primary data on nest coordinates for SZE.
  #' @param location_data_VES Data frame. Primary data on nest coordinates for VES.
  #'
  #' @return A data frame.

  create_location_MTA <- function(location_data_SZE,location_data_VES) {

    Location_data_temp<-dplyr::bind_rows(location_data_SZE,
                                        location_data_VES)%>%
      dplyr::transmute(LocationID=as.character(paste(.data$nestbox_new, .data$year,sep="_")),
                       NestboxID=LocationID,
                       LocationType="NB",
                       PopID=as.character(.data$PopID),
                       Latitude=.data$y_coord,
                       Longitude=.data$x_coord,
                       StartSeason=as.integer(.data$year),
                       EndSeason=StartSeason,
                       HabitatType=.data$HabitatType)%>%
      dplyr::ungroup()

    return(Location_data_temp)

  }

  ###########################################
  ###Run all the functions and save data
  ##########################################

  # BROOD DATA

  message("Compiling brood information...")

  Brood_data_temp <- create_brood_MTA(brood_data,capture_data)


  # CAPTURE DATA

  message("Compiling capture information...")

  Capture_data_temp <- create_capture_MTA(capture_data)


  # INDIVIDUAL DATA

  message("Compiling individual information...")

  Individual_data_temp <- create_individual_MTA(Capture_data_temp)


  # LOCATION DATA

  message("Compiling location information...")

  Location_data_temp <- create_location_MTA(location_data_SZE,location_data_VES)


  #### PROCESSING FINAL DATA TO EXPORT

  ## Brood data
  Brood_data <- Brood_data_temp %>%

    ## Keep only necessary columns
    dplyr::select(dplyr::contains(names(brood_data_template))) %>%

    ## Add missing columns
    dplyr::bind_cols(brood_data_template[0, !(names(brood_data_template) %in% names(.))] %>%
                       tibble::add_row()) %>%

    ## Reorder columns
    dplyr::select(names(brood_data_template)) %>%
    dplyr::mutate(LocationID=as.character(.data$LocationID))%>%
    dplyr::ungroup()


  # ## Check column classes
  # purrr::map_df(brood_data_template, class) == purrr::map_df(Brood_data, class)


  ## Capture data
  Capture_data <- Capture_data_temp %>%

    ## Keep only necessary columns
    dplyr::select(dplyr::contains(names(capture_data_template))) %>%

    ## Add missing columns
    dplyr::bind_cols(capture_data_template[0, !(names(capture_data_template) %in% names(.))] %>%
                       tibble::add_row()) %>%

    ## Reorder columns
    dplyr::select(names(capture_data_template)) %>%

    dplyr::ungroup()

  # ## Check column classes
  # purrr::map_df(capture_data_template, class) == purrr::map_df(Capture_data, class)


  ## Individual data
  Individual_data <- Individual_data_temp %>%

    ## Keep only necessary columns
    dplyr::select(dplyr::contains(names(individual_data_template))) %>%

    ## Add missing columns
    dplyr::bind_cols(individual_data_template[0, !(names(individual_data_template) %in% names(.))] %>%
                       tibble::add_row()) %>%

    ## Reorder columns
    dplyr::select(names(individual_data_template))  %>%
    dplyr::ungroup()

  # ## Check column classes
  # purrr::map_df(individual_data_template, class) == purrr::map_df(Individual_data, class)


  ## Location data
  Location_data <- Location_data_temp %>%

    ## Keep only necessary columns
    dplyr::select(dplyr::contains(names(location_data_template))) %>%

    ## Add missing columns
    dplyr::bind_cols(location_data_template[0, !(names(location_data_template) %in% names(.))] %>%
                       tibble::add_row()) %>%

    ## Reorder columns
    dplyr::select(names(location_data_template))  %>%
    dplyr::mutate(LocationID=as.character(.data$LocationID))%>%
    dplyr::mutate(NestboxID=as.character(.data$NestboxID))%>%
    dplyr::mutate(StartSeason=as.integer(.data$StartSeason))%>%

    dplyr::ungroup()

  # ## Check column classes
  # purrr::map_df(location_data_template, class) == purrr::map_df(Location_data, class)

  # EXPORT DATA

  time <- difftime(Sys.time(), start_time, units = "sec")

  message(paste0("All tables generated in ", round(time, 2), " seconds"))


  if (output_type == 'csv') {

    message("Saving .csv files...")

    utils::write.csv(x = Brood_data, file = paste0(path, "\\Brood_data_MTA.csv"), row.names = F)

    utils::write.csv(x = Capture_data, file = paste0(path, "\\Capture_data_MTA.csv"), row.names = F)

    utils::write.csv(x = Individual_data, file = paste0(path, "\\Individual_data_MTA.csv"), row.names = F)

    utils::write.csv(x = Location_data, file = paste0(path, "\\Location_data_MTA.csv"), row.names = F)

    invisible(NULL)

  }

  if (output_type == "R") {

    message("Returning R objects...")

    return(list(Brood_data = Brood_data,
                Capture_data = Capture_data,
                Individual_data = Individual_data,
                Location_data = Location_data))

  }

}




