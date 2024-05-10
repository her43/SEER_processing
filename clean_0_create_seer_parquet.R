# BASIC CLEANING - POPULATIONS FROM SEER DATA
# LOAD IN, BASIC MANIPULATION, SAVE AS PARQUET FOR LATER USE

# DATA SOURCE:
# https://seer.cancer.gov/censustract-pops/

rm(list=ls())
library(tidyverse)
gc()

seer_pop_tract <-
  readr::read_fwf("us.2006_2020.tract.level.pops.txt",
                  col_positions =
                    fwf_widths(c(4, 2, 2, 3, 6, 1, 1, 2, 9),
                               col_names = c("year", "state", "state_fips",
                                             "county_fips", "tract_fips",
                                             "race", "sex", "age", "pop")))
gc()

##### SAVE AS PARQUET
arrow::write_parquet(x = seer_pop_tract,
                     sink = "us_2006_2020_tract_level_pops.parquet")

gc()

### CHECK:
if(F){
  parquet_file <- "us_2006_2020_tract_level_pops.parquet"
  db <- arrow::open_dataset(parquet_file)
  temp_data_1 <- db %>%
    mutate(nchar_state_fips = nchar(state_fips)) %>%
    group_by(nchar_state_fips) %>%
    count() %>%
    collect()
  temp_data_2 <- db %>%
    group_by(state_fips) %>%
    count() %>%
    collect()
  temp_data_3 <- db %>%
    filter(state_fips %in% 42) %>%
    group_by(state_fips,county_fips,tract_fips, year) %>%
    count() %>%
    collect()
  table(temp_data_3$county_fips, temp_data_3$year)
  temp_data_4 <- db %>%
    filter(state_fips %in% "08", county_fips %in% "014") %>%
    group_by(state_fips,county_fips,tract_fips, year) %>%
    count() %>%
    collect()
  table(temp_data_3$county_fips, temp_data_3$year)
}


