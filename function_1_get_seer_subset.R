# LOAD FUNCTION FOR ONE STATE AND YEAR

# RACE CODES:
# 1 = NH WHITE
# 2 = NH BLACK
# 3 = NH AMERICAN INDIAN/ALASKA NATIVE
# 4 = NH ASIAN OR NATIVE HAWAIIAN/PACIFIC ISLANDER
# 5 = HISPANIC (ALL RACES)

# SEX CODES:
# 1 = MALE
# 2 = FEMALE

# AGE CODES:
# 00 = 0 years
# 01 = 1-4 years
# 02 = 5-9 years
# 03 = 10-14 years
# 04 = 15-19 years
# …
# 17 = 80-84 years
# 18 = 85+ years
library(tidyverse)
library(arrow)
################################################
# FUNCTION TO PULL IN ONE STATE x YEAR

get_seer_subset <- function(this_year = NULL, this_state_fips = NULL){
  if(length(this_year) %in% 0){
    stop(paste0("Invalid this_year value. ",
                "Must be 4-digit numeric year between 2006 and 2020. ",
                "[Error code 1]"))
  }else if(is.null(this_year)){
    stop(paste0("Invalid this_year value. ",
                "Must be 4-digit numeric year between 2006 and 2020. ",
                "[Error code 2]"))
  }else{
    this_year <- unique(sort(this_year))
    if(length(this_year) %in% 0){
      stop(paste0("Invalid this_year value. ",
                  "Must be 4-digit numeric year between 2006 and 2020. ",
                  "[Error code 3]"))
    }else if(is.character(this_year)){
      stop(paste0("Invalid this_year value. ",
                  "Must be 4-digit numeric year between 2006 and 2020. ",
                  "[Error code 4]"))
    }else if(is.numeric(this_year)){
      if(!(FALSE %in% (this_year %in% 2006:2020))) {
        n_years <- length(this_year)
        print(paste0("Year(s): ", paste(this_year, collapse = ", ")))
      }else{
        stop(paste0("Invalid this_year value. ",
                    "Must be 4-digit numeric year between 2006 and 2020. ",
                    "[Error code 5]"))
      }
    }else{
      stop(paste0("Invalid this_year value. ",
                  "Must be 4-digit numeric year between 2006 and 2020. ",
                  "[Error code 6]"))
    }
  }


  if(length(this_state_fips) %in% 0){
    stop(paste0("No state provided, please select a state FIPS for ",
                "this_state_fips [Error code 7]"))
  }else if(is.null(this_state_fips)){
    stop(paste0("No state provided, please select a state FIPS for ",
                "this_state_fips [Error code 8]"))
  }else{
    this_state_fips <- unique(sort(this_state_fips))
    if(length(this_state_fips) %in% 0){
      stop(paste0("No state provided, please select a state FIPS for ",
                  "this_state_fips [Error code 9]"))
    }else if(length(this_state_fips) > 1){
      stop(paste0("Length of this_state_fips greater than one (1). ",
                  "Provide only 1. [Error code 10]"))
    }else if(is.character(this_state_fips)){
      if(nchar(this_state_fips) %in% 1:2){
        if(this_state_fips %in% c(as.character(c(1:2, 4:6, 8:13, 15:42,
                                                 44:51, 53:56)),
                                  sprintf("%02d", c(1:2, 4:6, 8:9)))){
          print(paste0("State FIPS provided: ", this_state_fips))
          this_state_fips <- ifelse(nchar(this_state_fips) %in% 1,
                                    paste0("0", this_state_fips),
                                    this_state_fips)
        }else{
          stop(paste0("Invalid value for state FIPS code, this_state_fips. ",
                      "Value should be a 2-digit number, numeric or character,",
                      " with or without the leading zero. [Error code 11]"))
        }
      }else{
        stop(paste0("Invalid value for state FIPS code, this_state_fips. ",
                    "Value should be a 2-digit number, numeric or character,",
                    " with or without the leading zero. [Error code 12]"))
      }
    }else if(is.numeric(this_state_fips)){
      if(this_state_fips %in% c(1:2, 4:6, 8:13, 15:42,
                                44:51, 53:56)){
        print(paste0("State FIPS provided: ", this_state_fips))
        this_state_fips <- sprintf("%02d", this_state_fips)
      }else{
        stop(paste0("Invalid value for state FIPS code, this_state_fips. ",
                    "Value should be a 2-digit number, numeric or character,",
                    " with or without the leading zero. [Error code 13]"))
      }
    }else{
      stop(paste0("Invalid value for state FIPS code, this_state_fips. ",
                  "Value should be a 2-digit number, numeric or character,",
                  " with or without the leading zero. [Error code 14]"))
    }
  }
  if(n_years %in% 1){
    print(paste0("Attempting to pull SEER data for ",this_year,
                 " in state FIPS ", this_state_fips, "."))
  }else if(n_years %in% 2:5){
    print(paste0("Multiple years selected: ", paste(this_year, collapse = ", "),
                 " for state FIPS ", this_state_fips, ". Will attempt."))
  }else{
    stop(paste0("Large number of years, ",
                "please provide five or fewer years for this_year to",
                " avoid memory issues. [Error code 15]"))
  }
  parquet_file <- "us_2006_2020_tract_level_pops.parquet"
  db <- arrow::open_dataset(parquet_file)
  temp_data <- db %>%
    filter(year %in% this_year,
           state_fips %in% this_state_fips) %>%
    collect() %>%
    dplyr::mutate(tract_fips=paste0(state_fips, county_fips, tract_fips),
                  # THERE ARE 4 IMPLIED DECIMAL PLACES IN THE POPULATION
                  pop=as.numeric(pop)/10000,
                  age=case_when(
                    age=="00" ~ 0,
                    age=="01" ~ 1,
                    T ~ (as.numeric(age)-1)*5
                  ),
                  female = sex - 1,
                  race = dplyr::case_when(race %in% 1 ~ "NH White",
                                          race %in% 2 ~ "NH Black",
                                          race %in% 3 ~ "NH AIAN",
                                          race %in% 4 ~ "NH API",
                                          race %in% 5 ~ "Hispanic")) %>%
    dplyr::select(year, tract_fips, female, race, age, pop) %>%
    # COMPLETE THE DATA WITH MISSING COMBINATIONS OF DATA
    tidyr::complete(year, tract_fips, female, race, age,
                    fill = list(pop = 0))
}

if(F){
  # TESTS - MOST OF THESE WILL RETURN AN ERROR
  dta_test <- get_seer_subset(this_year = 2006, this_state_fips = 42)
  dta_test <- get_seer_subset(this_year = 200, this_state_fips = 42)
  dta_test <- get_seer_subset(this_year = "a", this_state_fips = 42)
  dta_test <- get_seer_subset( this_state_fips = 42)
  dta_test <- get_seer_subset(this_year = 2006:2007, this_state_fips = 42)
  dta_test <- get_seer_subset(this_year = 2006:2020, this_state_fips = 42)
  dta_test <- get_seer_subset(this_year = c(2005:2007), this_state_fips = 42)
  dta_test <- get_seer_subset(this_year = NA, this_state_fips = 42)
  dta_test <- get_seer_subset(this_year = c(NA, 2020), this_state_fips = 42)
  dta_test <- get_seer_subset(this_year = 2010, this_state_fips = 3)
  dta_test <- get_seer_subset(this_year = 2010, this_state_fips = c(1,2))
  dta_test <- get_seer_subset(this_year = 2010, this_state_fips = c("1","2"))
  dta_test <- get_seer_subset(this_year = 2010, this_state_fips = c("1","2"))
  dta_test <- get_seer_subset(this_year = 2010, this_state_fips = c("ab"))
  dta_test <- get_seer_subset(this_year = 2010, this_state_fips = c(77))
  dta_test <- get_seer_subset(this_year = T, this_state_fips = 42)
  dta_test <- get_seer_subset(this_year = 2006, this_state_fips = F)
}
