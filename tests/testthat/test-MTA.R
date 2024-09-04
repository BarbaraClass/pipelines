#first step: get format_MTA in the environment (either run full script or import function using dget)
pipeline_output <- format_MTA(db=choose_directory())#warnings:  Use of .data in tidyselect expressions was deprecated in tidyselect 1.2.0.

test_that("SZE outputs all files...", {

  expect_true(all("SZE" %in% pipeline_output$Brood_data$PopID))
  expect_true(all("SZE" %in% pipeline_output$Capture_data$CapturePopID))
  expect_true(all("SZE" %in% pipeline_output$Individual_data$PopID))
  expect_true(all("SZE" %in% pipeline_output$Location_data$PopID))

})

test_that("VES outputs all files...", {

  expect_true(all("VES" %in% pipeline_output$Brood_data$PopID))
  expect_true(all("VES" %in% pipeline_output$Capture_data$CapturePopID))
  expect_true(all("VES" %in% pipeline_output$Individual_data$PopID))
  expect_true(all("VES" %in% pipeline_output$Location_data$PopID))

})


### Test Individual data for VES

test_that("Individual data returns an expected outcome...", {

  #Open VES data
  MTA_data <- dplyr::filter(pipeline_output$Individual_data, PopID %in% "VES")

  ## Great tit male ringed as adult
  expect_equal(subset(MTA_data, IndvID == "N167082")$Sex_calculated, "M")
  expect_equal(subset(MTA_data, IndvID == "N167082")$Species, "PARMAJ")
  expect_equal(subset(MTA_data, IndvID == "N167082")$RingAge, "adult")
  expect_equal(subset(MTA_data, IndvID == "N167082")$Sex_genetic, NA_character_)
  expect_equal(subset(MTA_data, IndvID == "N167082")$BroodIDLaid, NA_character_)#not correct.fix it
  expect_equal(subset(MTA_data, IndvID == "N167082")$RingSeason, 2013)

  # Great tit male ringed as adult
  expect_equal(subset(MTA_data, IndvID == "N203843")$Sex_calculated, "F")
  expect_equal(subset(MTA_data, IndvID == "N203843")$Species, "PARMAJ")
  expect_equal(subset(MTA_data, IndvID == "N203843")$RingAge, "adult")
  expect_equal(subset(MTA_data, IndvID == "N203843")$Sex_genetic, NA_character_)
  expect_equal(subset(MTA_data, IndvID == "N203843")$BroodIDLaid, NA_character_)
  expect_equal(subset(MTA_data, IndvID == "N203843")$RingSeason, 2014)

  #Great tit female caught as chick
  expect_equal(subset(MTA_data, IndvID == "N203930")$Sex_calculated, "F")#should be NA
  expect_equal(subset(MTA_data, IndvID == "N203930")$Sex_genetic, NA_character_)
  expect_equal(subset(MTA_data, IndvID == "N203930")$Species, "PARMAJ")
  expect_equal(subset(MTA_data, IndvID == "N203930")$BroodIDLaid, "523")
  expect_equal(subset(MTA_data, IndvID == "N203930")$BroodIDFledged, "523")
  expect_equal(subset(MTA_data, IndvID == "N203930")$RingSeason, 2014)
  expect_equal(subset(MTA_data, IndvID == "N203930")$RingAge, "chick")

})

### Test Individual data for SZE

test_that("Individual data returns an expected outcome...", {

  #Open VES data
  MTA_data <- dplyr::filter(pipeline_output$Individual_data, PopID %in% "SZE")

  ## Great tit female ringed as adult
  expect_equal(subset(MTA_data, IndvID == "N373738")$Sex_calculated, "F")
  expect_equal(subset(MTA_data, IndvID == "N373738")$Species, "PARMAJ")
  expect_equal(subset(MTA_data, IndvID == "N373738")$RingAge, "adult")
  expect_equal(subset(MTA_data, IndvID == "N373738")$Sex_genetic, NA_character_)
  expect_equal(subset(MTA_data, IndvID == "N373738")$BroodIDLaid, NA_character_)
  expect_equal(subset(MTA_data, IndvID == "N373738")$RingSeason, 2020)

  # Great tit male ringed as adult
  expect_equal(subset(MTA_data, IndvID == "N172746")$Sex_calculated, "M")
  expect_equal(subset(MTA_data, IndvID == "N172746")$Species, "PARMAJ")
  expect_equal(subset(MTA_data, IndvID == "N172746")$RingAge, "adult")
  expect_equal(subset(MTA_data, IndvID == "N172746")$Sex_genetic, NA_character_)
  expect_equal(subset(MTA_data, IndvID == "N172746")$BroodIDLaid, NA_character_)
  expect_equal(subset(MTA_data, IndvID == "N172746")$RingSeason, 2013)

  #Great tit male caught as chick
  expect_equal(subset(MTA_data, IndvID == "N254188")$Sex_calculated, "M")
  expect_equal(subset(MTA_data, IndvID == "N254188")$Sex_genetic, NA_character_)
  expect_equal(subset(MTA_data, IndvID == "N254188")$Species, "PARMAJ")
  expect_equal(subset(MTA_data, IndvID == "N254188")$BroodIDLaid, "698")
  expect_equal(subset(MTA_data, IndvID == "N254188")$BroodIDFledged, "698")
  expect_equal(subset(MTA_data, IndvID == "N254188")$RingSeason, 2015)
  expect_equal(subset(MTA_data, IndvID == "N254188")$RingAge, "chick")

})



test_that("Brood_data returns an expected outcome...", {

  #Open VES data
  MTA_data <- dplyr::filter(pipeline_output$Brood_data, PopID %in% "VES")

  ## PARMAJ nest 250 in 2013
  expect_equal(subset(MTA_data,
                      BroodID == "250" &
                        BreedingSeason == 2013)$Species, "PARMAJ")
  expect_equal(subset(MTA_data,
                      BroodID == "250" &
                        BreedingSeason == 2013)$FemaleID, "N172882")
  expect_equal(subset(MTA_data,
                      BroodID == "250" &
                        BreedingSeason == 2013)$MaleID, "N172695")
  expect_equal(subset(MTA_data,
                      BroodID == "250" &
                        BreedingSeason == 2013)$LayDate_observed, as.Date("2013-04-17"))
  expect_equal(subset(MTA_data,
                      BroodID == "250" &
                        BreedingSeason == 2013)$ClutchSize_observed, NA_integer_)#should update clutch size min based on nr fledged?
  expect_equal(subset(MTA_data,
                      BroodID == "250" &
                        BreedingSeason == 2013)$NumberFledged_observed, 9)

  ## PARMAJ nest 2665 in 2021
  expect_equal(subset(MTA_data,
                      BroodID == "2665" &
                        BreedingSeason == 2021)$Species, "PARMAJ")
  expect_equal(subset(MTA_data,
                      BroodID == "2665" &
                        BreedingSeason == 2021)$FemaleID, "N373960")
  expect_equal(subset(MTA_data,
                      BroodID == "2665" &
                        BreedingSeason == 2021)$MaleID, NA_character_)
  expect_equal(subset(MTA_data,
                      BroodID == "2665" &
                        BreedingSeason == 2021)$LayDate_observed, as.Date("2021-04-13"))
  expect_equal(subset(MTA_data,
                      BroodID == "2665" &
                        BreedingSeason == 2021)$ClutchSize_observed, 8)
  expect_equal(subset(MTA_data,
                      BroodID == "2665" &
                        BreedingSeason == 2021)$NumberFledged_observed, 7)

})

test_that("Brood_data returns an expected outcome...", {

  #Open SZE data
  MTA_data <- dplyr::filter(pipeline_output$Brood_data, PopID %in% "SZE")

  ## PARMAJ nest 2829 in 2021
  expect_equal(subset(MTA_data,
                      BroodID == "2829" &
                        BreedingSeason == 2021)$Species, "PARMAJ")
  expect_equal(subset(MTA_data,
                      BroodID == "2829" &
                        BreedingSeason == 2021)$FemaleID, "N331823")
  expect_equal(subset(MTA_data,
                      BroodID == "2829" &
                        BreedingSeason == 2021)$MaleID, "N373232")
  expect_equal(subset(MTA_data,
                      BroodID == "2829" &
                        BreedingSeason == 2021)$LayDate_observed, as.Date("2021-04-20"))
  expect_equal(subset(MTA_data,
                      BroodID == "2829" &
                        BreedingSeason == 2021)$ClutchSize_observed, NA_integer_)
  expect_equal(subset(MTA_data,
                      BroodID == "2829" &
                        BreedingSeason == 2021)$NumberFledged_observed, 8)
})



test_that("Capture_data returns an expected outcome...", {#to update

  #Open MTA data
  MTA_data <- dplyr::filter(pipeline_output$Capture_data, CapturePopID %in% "MTA")

  ##great tit 1KA34008 ringed as chick
  expect_equal(subset(MTA_data,
                      IndvID == "1KA34008")$BreedingSeason, 2015)
  expect_equal(subset(MTA_data,
                      IndvID == "1KA34008")$CaptureDate, as.Date("2015-05-14"))
  expect_equal(subset(MTA_data,
                      IndvID == "1KA34008")$ChickAge, 14)
  expect_equal(subset(MTA_data,
                      IndvID == "1KA34008")$Mass, 15.1)
  expect_equal(subset(MTA_data,
                      IndvID == "1KA34008")$Tarsus, 19.3)
  expect_equal(subset(MTA_data,
                      IndvID == "1KA34008")$LocationID, "84")


  ## female 2601411
  expect_equal(unique(subset(MTA_data,
                             IndvID == "2601411")$BreedingSeason), c(2000,2001,2002,2003,2004))
  expect_equal(unique(subset(MTA_data,
                             IndvID == "2601411")$Sex_observed), "F")
  expect_equal(unique(subset(MTA_data,
                             IndvID == "2601411")$LocationID), c(NA,"5","51","53","154"))
  expect_equal(unique(subset(MTA_data,
                             IndvID == "2601411")$Tarsus), c(NA,19.4,19.6,19.0,19.3))
  expect_equal(unique(subset(MTA_data,
                             IndvID == "2601411")$WingLength), c(NA,72.0,73.0,75.5,75.0))

})

test_that("Location_data returns an expected outcome...", {#to update

  #Open MTA data
  MTA_data <- dplyr::filter(pipeline_output$Location_data, PopID %in% "MTA")

  expect_equal(unique(subset(MTA_data,
                             LocationID == "52")$Latitude),41.467601003 )
  expect_equal(unique(subset(MTA_data,
                             LocationID == "52")$Longitude), 2.147169517)
  expect_equal(unique(subset(MTA_data,
                             LocationID == "52")$StartSeason), 1998)

  expect_equal(unique(subset(MTA_data,
                             LocationID == "175")$Latitude),41.468254349 )
  expect_equal(unique(subset(MTA_data,
                             LocationID == "175")$Longitude),2.146151032 )
  expect_equal(unique(subset(MTA_data,
                             LocationID == "175")$StartSeason), 2008)

  expect_equal(unique(subset(MTA_data,
                             LocationID == "185")$Latitude), 41.462856906)
  expect_equal(unique(subset(MTA_data,
                             LocationID == "185")$Longitude), 2.14061869)
  expect_equal(unique(subset(MTA_data,
                             LocationID == "185")$StartSeason), 2013)

})


### General tests (for pipelines formatted to standard protocol version 1.1.0): ALL passed

test_that("Expected columns are present", {

  ## Will fail if not all the expected columns are present

  ## Brood data: Test that all columns are present
  test_col_present(pipeline_output, "Brood")

  ## Capture data: Test that all columns are present
  test_col_present(pipeline_output, "Capture")

  ## Individual data: Test that all columns are present
  test_col_present(pipeline_output, "Individual")

  ## Location data: Test that all columns are present
  test_col_present(pipeline_output, "Location")

})

test_that("Column classes are as expected", {

  ## Will fail if columns that are shared by the output and the templates have different classes.

  # ## Brood data: Test that all column classes are expected
  test_col_classes(pipeline_output, "Brood")

  ## Capture data: Test that all column classes are expected
  test_col_classes(pipeline_output, "Capture")

  ## Individual data: Test that all column classes are expected
  test_col_classes(pipeline_output, "Individual")

  ## Location data: Test that all column classes are expected
  test_col_classes(pipeline_output, "Location")

})


test_that("ID columns match the expected format for the pipeline", {#fails

  # ## FemaleID format is as expected
  test_ID_format(pipeline_output, ID_col = "FemaleID", ID_format = "^[:alnum:]{6,8}$")

  # ## MaleID format is as expected
  test_ID_format(pipeline_output, ID_col = "MaleID", ID_format = "^[:alnum:]{6,8}$")

  # ## IndvID format in Capture data  is as expected
  test_ID_format(pipeline_output, ID_col = "C-IndvID", ID_format = "^[:alnum:]{6,8}$")

  ## IndvID format in Individual data is as expected
  test_ID_format(pipeline_output, ID_col = "I-IndvID", ID_format = "^[:alnum:]{6,8}$")

})


test_that("Key columns only contain unique values", {

  # ## BroodID has only unique values
  test_unique_values(pipeline_output, "BroodID")

  ## CaptureID has only unique values
  test_unique_values(pipeline_output, "CaptureID")

  ## PopID-IndvID has only unique values
  test_unique_values(pipeline_output, "PopID-IndvID")

})


test_that("Key columns in each table do not have NAs", {

  ## Brood
  test_NA_columns(pipeline_output, "Brood")

  ## Capture
  test_NA_columns(pipeline_output, "Capture")

  ## Individual
  test_NA_columns(pipeline_output, "Individual")

  ## Location
  test_NA_columns(pipeline_output, "Location")

})

test_that("Categorical columns do not have unexpected values", {#fails

  ## Brood
  test_category_columns(pipeline_output, "Brood")

  ## Capture
  test_category_columns(pipeline_output, "Capture")

  ## Individual
  test_category_columns(pipeline_output, "Individual")

  ## Location
  test_category_columns(pipeline_output, "Location")

})

