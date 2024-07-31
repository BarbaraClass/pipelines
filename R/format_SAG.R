library(tidyverse)
library(dplyr)
#'Construct standard format for data from Sagunto, Spain
#'
#'A pipeline to produce the standard format for the nest box population in Sagunto, Spain, administered by Emilio Barba.
#'
#'This section provides details on data management choices that are unique to
#'this data. For a general description of the standard format please see
#'\href{https://github.com/SPI-Birds/documentation/blob/master/standard_protocol/SPI_Birds_Protocol_v1.1.0.pdf}{here}.
#'
#'\strong{Species}:
#'
#'@inheritParams pipeline_params
#'
#'@return Generates either 4 .csv files or 4 data frames in the standard format.
#'@export


format_SAG <- function(db = choose_directory(),
                       path = ".",
                       species = NULL,
                       pop = NULL,
                       output_type = 'R'){

  #Force choose_directory() if used
  force(db)
  #Assign to database location
  db <- paste0(gsub("\\\\", "/", db), "\\SAG_PrimaryData.accdb")

  #### Determine species and population codes for filtering
  if(is.null(species)){

    species_filter <- NULL

  } else {

    species_filter <- species_codes$Species

  }

  if(is.null(pop)){

    pop_filter <- NULL

  } else {

    pop_filter <- pop_codes$PopID

  }

  ## Start time
  start_time <- Sys.time()

  message("Importing primary data...")

  ###N.B. IF THE ACCESS DRIVER AND VERSION OF R ARE NOT 64 BIT THIS WILL RETURN AN ERROR
  #Connect to the SAG database backend.
  connection <- DBI::dbConnect(drv = odbc::odbc(),
                               .connection_string = paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};Dbq=", db, ";Uid=Admin;Pwd=;"))

  ## Get brood information from primary data

  ## TODO: Ask about dates that are off
  ## TODO: Ask about experiments
  brood_data <- dplyr::tbl(connection, "Datos de Nidificaci\u00f3n") %>%
    tibble::as_tibble() %>%
    dplyr::rename_with(~gsub(pattern = "~|'|\\?",
                             replacement = "",
                             iconv(.,
                                   from = "UTF-8",
                                   to = 'ASCII//TRANSLIT')),
                       .cols = tidyselect::everything()) %>%
    janitor::clean_names(case = "upper_camel") %>%
    dplyr::mutate(Ano=sprintf("%02d", as.numeric(.data$Ano)))%>%  #make sure breeding seasons have 2 digits
    dplyr::transmute(PopID = "SAG",
                     Species = species_codes[species_codes$SpeciesID == 14640,]$Species,
                     BreedingSeason = as.numeric(dplyr::case_when(.data$Ano <= 99 & .data$Ano > 84 ~ paste0(19L,.data$Ano),
                                                                  .data$Ano <= 80 ~ paste0(20L,.data$Ano))),
                     Plot= NA_character_,#Plots are not independent
                     LocationID = paste(toupper(.data$Caja),BreedingSeason,sep="_"),#the same box can be used in a different plot between years. Paste year to it?
                     ClutchType_observed = dplyr::case_when(.data$TipoDePuesta == "Primera" ~ "first",
                                                            .data$TipoDePuesta == "Segunda" ~ "second",
                                                            .data$TipoDePuesta == "Reposici\x97n" ~ "replacement",
                                                            .data$TipoDePuesta == "Desconocida" ~ NA_character_,
                                                            TRUE ~ NA_character_),
                     BroodID = .data$CodigoNido,
                     AvgEggMass = NA_character_,#there is only data on egg volume but not mass
                     NumberEggs = .data$TamanoDePuesta,
                     LayDate_observed = as.Date(as.integer(.data$FechaDeInicioDePuesta),
                                                origin = as.Date(paste0(.data$BreedingSeason, "-03-31"))),
                     ClutchSize_observed = .data$TamanoDePuesta,
                     HatchDate_observed = as.Date(as.integer(.data$FechaDeEclosion),
                                                  origin = as.Date(paste0(.data$BreedingSeason, "-03-31"))),
                     BroodSize_observed = .data$NumeroDeEclosiones,
                     NumberFledged_observed = .data$NumeroDePollosQueVuelan,
                     ExperimentID = dplyr::case_when(grepl("S", .data$Experimentos) ~ "OTHER",
                                                     TRUE ~ NA_character_)) %>%

    dplyr::mutate(dplyr::across(c(ClutchSize_observed,
                                  BroodSize_observed,
                                  NumberFledged_observed), as.integer)) %>%

    dplyr::arrange(.data$BreedingSeason, .data$LocationID)


  ## Get ringing information from primary data
  rr_data_ring <- dplyr::tbl(connection, "DATOS PRIMER ANILLAMIENTO") %>%
    tibble::as_tibble() %>%
    dplyr::rename_with(~gsub(pattern = "~|'|\\?",
                             replacement = "",
                             iconv(.,
                                   from = "UTF-8",
                                   to = 'ASCII//TRANSLIT')),
                       .cols = tidyselect::everything()) %>%
    janitor::clean_names(case = "upper_camel")%>%

    ## Process relevant columns
    ## TODO: Check on missing capture dates
    dplyr::transmute(table = "ringing",
                     PopID = "SAG",
                     Species = species_codes[species_codes$SpeciesID == 14640,]$Species,
                     IndvID = toupper(dplyr::case_when(stringr::str_detect(.data$Anilla,  "^[:alnum:]{6,8}$") ~ .data$Anilla,
                                                       TRUE ~ NA_character_)),
                     Plot= NA_character_,
                     BroodID = .data$CodigoNido,
                     Sex_observed = dplyr::case_when(.data$Sexo == "H" ~ "M",
                                                     .data$Sexo == "M" ~ "F",
                                                     TRUE ~ NA_character_),
                     Age_observed = as.integer(.data$EdadEuring),
                     CaptureDate = format(.data$FechaCaptura, "%Y-%m-%d"),
                     BreedingSeason = as.numeric(format(.data$FechaCaptura, "%Y")),
                     LocationID = paste(toupper(.data$CodigoCaja),BreedingSeason,sep="_"),
                     Mass = round(as.numeric(Peso),1),
                     WingLength = round(as.numeric(.data$LongitudAla),1),
                     Tarsus = round(as.numeric(.data$LongitudTarso),2),
                     ExperimentID = dplyr::case_when(!is.na(.data$Transponder) ~ "SURVIVAL"))

  #some errors in broodIDs causing 3 of them to be duplicated. These are correct in the brood data
  cap_broods_dups_dup <- aggregate(LocationID~BroodID, rr_data_ring[which(rr_data_ring$Age_observed==1),],function(x) length(unique(x)))
  cap_broods_dups_dup<-cap_broods_dups_dup[cap_broods_dups_dup$LocationID>1,]

  if(nrow(cap_broods_dups_dup)>0){
    broods=unique(brood_data[,c("BroodID","LocationID","BreedingSeason")])
    broods=broods[which(broods$BroodID %in% cap_broods_dups_dup$BroodID ),]#these are the correct ones (same in both)

    df=unique(rr_data_ring[which(rr_data_ring$BroodID%in%cap_broods_dups_dup$BroodID),c("BroodID","LocationID","BreedingSeason")])
    df=df[-which(df$LocationID %in% broods$LocationID),]#these are the incorrect ones

    broodsAll=unique(brood_data[,c("BroodID","LocationID","BreedingSeason")])
    broodsAll=broodsAll[broodsAll$LocationID %in% df$LocationID,]#these are the correct broodIDS

     for ( i in 1:nrow(broodsAll)){
    rr_data_ring[which(rr_data_ring$LocationID %in%broodsAll$LocationID[i] ),"BroodID"]=broodsAll$BroodID[i]
    }
  }

  ## Get recapture information from primary data
  rr_data_recap <- dplyr::tbl(connection, "RECUPERACIONES") %>%
    tibble::as_tibble() %>%
    dplyr::rename_with(~gsub(pattern = "~|'|\\?",
                             replacement = "",
                             iconv(.,
                                   from = "UTF-8",
                                   to = 'ASCII//TRANSLIT')),
                       .cols = tidyselect::everything()) %>%
    janitor::clean_names(case = "upper_camel") %>%

    ## Process relevant columns
    dplyr::transmute(table = "recap",
                     PopID = "SAG",
                     Species = species_codes[species_codes$SpeciesID == 14640,]$Species,
                     IndvID = toupper(dplyr::case_when(stringr::str_detect(.data$Anilla,  "^[:alnum:]{6,8}$") ~ .data$Anilla,
                                                       TRUE ~ NA_character_)),
                     Plot= NA_character_,
                     BroodID = .data$CodigoNido,
                     Age_observed = as.integer(.data$EdadEuring),
                     CaptureDate = format(.data$FechaCaptura, "%Y-%m-%d"),
                     BreedingSeason = as.numeric(format(.data$FechaCaptura, "%Y")),##Some years are 1 century off...
                     LocationID = paste(toupper(.data$CodigoCaja),BreedingSeason,sep="_"),
                     Mass = round(as.numeric(.data$Peso),1),
                     WingLength = round(as.numeric(.data$LongitudAla),1),
                     Tarsus = round(as.numeric(.data$LongitudTarso),2),
                     ExperimentID = dplyr::case_when(!is.na(.data$Transponder) ~ "SURVIVAL",
                                                     TRUE ~ NA_character_))

  ## Combine capture records
  rr_data <- dplyr::bind_rows(rr_data_ring,
                              rr_data_recap) %>%
    dplyr::arrange(.data$BreedingSeason, .data$IndvID, .data$CaptureDate) %>%

    ## Replace 0 with NA in mass, wing length, and tarsus
    dplyr::mutate(dplyr::across(c(.data$Mass,
                                  .data$WingLength,
                                  .data$Tarsus), ~dplyr::na_if(., 0))) %>%

    ## Sex is not always entered, but information is sometimes present from earlier years
    ## If it is not conflicted, fill in missing values
    dplyr::group_by(.data$IndvID) %>%
    dplyr::mutate(Sex_calculated = purrr::map_chr(.x = list(unique(stats::na.omit(.data$Sex_observed))),
                                                  .f = ~{
                                                    if(length(..1) == 0){
                                                      return(NA_character_)
                                                    } else if(length(..1) == 1){
                                                      return(..1)
                                                    } else {
                                                      return(NA_character_)
                                                    }
                                                  }),
                  CaptureDate = as.Date(.data$CaptureDate),
                  CaptureMonth=as.numeric(format(.data$CaptureDate, "%m")))%>%
    dplyr::filter(CaptureMonth>3) %>%
    dplyr::filter(!is.na(BroodID)) %>%
    dplyr::filter(!is.na(IndvID)) %>%

    tibble::as_tibble()
 #some nest box numbers are missing:fix it
  boxes_to_find=unique(rr_data[grep("NA",rr_data$LocationID),c("LocationID","BroodID")])

  for ( i in 1:nrow(boxes_to_find)){
    rr_data[which(rr_data$BroodID ==boxes_to_find$BroodID[i]),"LocationID"]=
    brood_data[which(brood_data$BroodID ==boxes_to_find$BroodID[i]),"LocationID"]
  }


  # BROOD DATA

  message("Compiling brood information...")

  Brood_data_temp <- create_brood_SAG(brood_data, rr_data)


  # CAPTURE DATA

  message("Compiling capture information...")

  Capture_data_temp <- create_capture_SAG(Brood_data_temp, rr_data)


  # INDIVIDUAL DATA

  message("Compiling individual information...")

  Individual_data_temp <- create_individual_SAG(Capture_data_temp)


  # LOCATION DATA

  message("Compiling location information...")

  Location_data_temp <- create_location_SAG(Brood_data_temp, nest_coord_data)

  #Disconnect from database
  DBI::dbDisconnect(connection)

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
    dplyr::ungroup()

  # ## Check column classes
  # purrr::map_df(location_data_template, class) == purrr::map_df(Location_data, class)

  # EXPORT DATA

  time <- difftime(Sys.time(), start_time, units = "sec")

  message(paste0("All tables generated in ", round(time, 2), " seconds"))


  if (output_type == 'csv') {

    message("Saving .csv files...")

    utils::write.csv(x = Brood_data, file = paste0(path, "\\Brood_data_SAG.csv"), row.names = F)

    utils::write.csv(x = Capture_data, file = paste0(path, "\\Capture_data_SAG.csv"), row.names = F)

    utils::write.csv(x = Individual_data, file = paste0(path, "\\Individual_data_SAG.csv"), row.names = F)

    utils::write.csv(x = Location_data, file = paste0(path, "\\Location_data_SAG.csv"), row.names = F)

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

  #' Create brood data table for Sagunto, Spain.
  #'
  #' @param brood_data Brood data compiled from primary data from Sagunto, Spain.
  #'
  #' @param rr_data Ringing data compiled from primary data from Sagunto, Spain.
  #'
  #' @return A data frame.

  create_brood_SAG   <- function(brood_data, rr_data) {
    ## Create brood data
    Brood_data_temp <- brood_data %>%

      ## Summarize chick data form ringing records
      ## TODO: Some BroodIDs are clearly associated with multiple broods (e.g. 12015), these need to be fixed
      ## TODO: Check about how birds are sexed when age == 1
      dplyr::left_join(rr_data %>%

                         ## Get parents from each brood
                         dplyr::filter(.data$table == "ringing",
                                       (!is.na(.data$Sex_calculated) & .data$Age_observed != 1),
                                       !is.na(.data$BroodID)) %>%
                         dplyr::select(PopID,
                                       Species,
                                       BreedingSeason,
                                       Plot,
                                       LocationID,
                                       Sex_calculated,
                                       IndvID,
                                       BroodID) %>%
                         dplyr::group_by(.data$BroodID, .data$Sex_calculated) %>%

                         ## Remove broods where more than one parent has the same sex
                         dplyr::mutate(count = n()) %>%
                         dplyr::filter(count < 2) %>%
                         tidyr::pivot_wider(id_cols = .data$BroodID,
                                            names_from = .data$Sex_calculated,
                                            values_from = .data$IndvID,
                                            values_fill = NA) %>%
                         dplyr::rename(FemaleID = .data$F,
                                       MaleID = .data$M),
                       by = "BroodID") %>%

      ## Get chick summary stats

    dplyr::left_join(rr_data %>%

                         ## Keeping chicks from ringing data
                         dplyr::filter(.data$Age_observed == 1,
                                       .data$table == "ringing") %>%

                         dplyr::select(.data$BroodID,
                                       .data$IndvID,
                                       .data$Mass,
                                       .data$Tarsus) %>%

                         ## For each brood, get summary stats
                         dplyr::group_by(.data$BroodID) %>%
                         dplyr::summarise(AvgChickMass = round(mean(Mass, na.rm = T), 1),
                                          NumberChicksMass = sum(!is.na(.data$Mass)),
                                          AvgTarsus = round(mean(Tarsus, na.rm = T), 2),
                                          NumberChicksTarsus = sum(!is.na(.data$Tarsus))) %>%

                         ## Replace NaNs and 0 with NA
                         dplyr::mutate(dplyr::across(c(.data$AvgChickMass,.data$AvgTarsus), ~dplyr::na_if(., NaN))) %>%

                         ## Drop NAs in BroodID
                         dplyr::filter(!is.na(.data$BroodID)),
                       by = "BroodID")

    tibble::as_tibble()

    return(Brood_data_temp)

  }

  #' Create capture data table in standard format for data from Sagunto, Spain.
  #'
  #' @param rr_data Data frame. Primary data from ringing records.
  #'
  #' @return A data frame.

  create_capture_SAG <- function(rr_data) {

    ## Captures from ringing data
    Capture_data_temp <- rr_data %>%

      ## Rename variables
      dplyr::rename(CapturePopID = .data$PopID) %>%

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
      dplyr::mutate(CaptureID = paste(.data$IndvID, dplyr::row_number(), sep = "_"))


    return(Capture_data_temp)

  }


  #' Create individual data table in standard format for data from Sagunto, Spain.
  #'
  #' @param Capture_data_temp Data frame. Output from \code{\link{create_capture_SAG}}.
  #'
  #' @return A data frame.

  create_individual_SAG <- function(Capture_data_temp) {

    ## Create individual data from capture data
    Individual_data_temp <- Capture_data_temp %>%

      ## Arrange
      dplyr::arrange(.data$IndvID, .data$CaptureDate) %>%

      #### Format and create new data columns
      dplyr::group_by(.data$IndvID, .data$CapturePopID) %>%
      dplyr::mutate(PopID = .data$CapturePopID) %>%
      dplyr::group_by(.data$IndvID) %>%
      dplyr::mutate(Species = purrr::map_chr(.x = list(unique(stats::na.omit(.data$Species))),
                                             .f = ~{
                                               if(length(..1) == 0){
                                                 return(NA_character_)
                                               } else if(length(..1) == 1){
                                                 return(..1)
                                               } else {
                                                 return("CCCCCC")
                                               }
                                             }),
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
                    BroodIDLaid = purrr::map_chr(.x = list(unique(stats::na.omit(.data$BroodID))),
                                                 .f = ~{
                                                   if(length(..1) != 1){
                                                     return(NA_character_)
                                                   } else if(length(..1) == 1){
                                                     return(..1)
                                                   }
                                                 }),

                    ## No cross-fostering, so BroodIDFledged is always BroodIDLaid
                    BroodIDFledged = .data$BroodIDLaid) %>%

      ## BroodIDs should be NA for any individuals ringed as adults
      dplyr::mutate(across(c(BroodIDLaid,
                             BroodIDFledged),
                           ~dplyr::case_when(.data$RingAge == "adult" ~ NA_character_,
                                             .data$RingAge == "chick" ~ .))) %>%

      ## Keep distinct records by PopID and InvdID
      dplyr::distinct(.data$PopID, .data$IndvID, .keep_all = TRUE) %>%
      dplyr::ungroup() %>%

      ## Reorder columns
      dplyr::select(dplyr::any_of(names(individual_data_template)), dplyr::everything())

    return(Individual_data_temp)

  }


  #' Create location data table in standard format for data from Sagunto, Spain.
  #'
  #' @param Brood_data_temp Data frame. Output from \code{\link{create_brood_SAG}}.
  #'
  #' @param nest_coord_data Data frame. Primary data on nest coordinates.
  #'
  #' @return A data frame.

  create_location_SAG <- function(Brood_data_temp, nest_coord_data) {

    ## Create location data from brood data and nest coordinates

    Location_data_temp <- Brood_data_temp %>%
      dplyr::select(.data$Plot,
                    .data$LocationID,
                    .data$BreedingSeason) %>%
      dplyr::mutate(PopID = "SAG",
                    NestboxID = .data$LocationID,
                    StartSeason = NA_character_,#NA for now because we nest boxes don't have unique coordinates
                    EndSeason = NA_character_,#NA for now
                    Latitude = NA_character_,#NA for now
                    Longitude = NA_character_,#NA for now
                    HabitatType = "deciduous",
                    LocationType = "NB") %>%


    return(Location_data_temp)

  }


