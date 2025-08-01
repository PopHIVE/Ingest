library(dplyr)
library(vroom)
####################################################################
# prepare population size estimate
####################################################################

# state level popsize estimate (latest available: 2023) obtained from CDC wonder
pop <- read.csv(
  "./resources/Single-Race Population Estimates 2020-2023 by State and Single-Year Age.csv"
) %>%
  dplyr::rename(
    char = Notes.States.States.Code.Single.Year.Ages.Single.Year.Ages.Code.Population
  ) %>%
  mutate(
    geography = str_extract(char, "^[^0-9]+") %>% str_trim(),
    age = str_extract(char, "\\d{1,2} (year|years)"),
    popsize = as.numeric(str_extract(char, "\\d+$"))
  ) %>%
  mutate(
    age = as.numeric(str_replace(
      ifelse(
        grepl("<", char),
        0, # age = 0 for < 1
        ifelse(grepl("85\\+", char), 85, age)
      ),
      "year",
      ""
    ))
  ) %>% # age = 85 for 85+
  filter(!is.na(age)) %>%
  dplyr::select(-char)


# pop of Puerto Rico (2023) obtained from https://www.census.gov/data/tables/time-series/demo/popest/2020s-detail-puerto-rico.html
pop_PR <- data.frame(
  geography = "Puerto Rico",
  age_level = c(
    "Total",
    "<1 Years",
    "1-4 Years",
    "5-17 Years",
    "18-49 Years",
    "65+ Years"
  ),
  popsize = c(3205691, 18682, 78297, 401700, 1295060, 770855)
)

# pop of Virgin islands obtained from: https://www.census.gov/data/tables/2020/dec/2020-us-virgin-islands.html
# (only 2020 data can be found on census.gov, and only data unstratified by age can be found)
pop_VI <- data.frame(
  geography = "Virgin Islands",
  age_level = "Total",
  popsize = 87146
)

# pop of Guam obtained from: https://www.census.gov/newsroom/press-releases/2023/2020-dhc-summary-file-guam.html
# (only 2020 data can be found on census.gov, and only data unstratified by age can be found)
pop_GU <- data.frame(geography = "Guam", age_level = "Total", popsize = 153836)


# population size by state (unstratified by age)
pop_unstra <- pop %>%
  group_by(geography) %>%
  summarize(popsize = sum(popsize)) %>%
  mutate(age_level = "Total") %>%
  rbind(pop_PR %>% filter(age_level == "Total"), pop_VI, pop_GU) %>%
  arrange(geography)

pop_unstra <- pop_unstra %>%
  rbind(data.frame(
    geography = "United States",
    popsize = sum(pop_unstra$popsize),
    age_level = "Total"
  ))

# population size by state (stratified by age)
pop_stra <- pop %>%
  mutate(
    age_level = case_when(
      age == 0 ~ "<1 Years",
      age >= 1 & age <= 4 ~ "1-4 Years",
      age >= 5 & age <= 17 ~ "5-17 Years",
      age >= 18 & age <= 49 ~ "18-49 Years",
      age >= 50 & age <= 64 ~ "50-64 Years",
      age >= 65 ~ "65+ Years"
    )
  ) %>%
  group_by(geography, age_level) %>%
  summarize(popsize = sum(popsize)) %>%
  rbind(pop_PR %>% filter(age_level != "Total")) %>%
  arrange(geography)


# get population size by Health Region
pop_unstra_hhs <- pop_unstra %>%
  mutate(
    hhs = case_when(
      geography %in%
        c(
          "Connecticut",
          "Maine",
          "Massachusetts",
          "New Hampshire",
          "Rhode Island",
          "Vermont"
        ) ~
        "Region 1",
      geography %in% c("Alaska", "Idaho", "Oregon", "Washington") ~ "Region 10",
      geography %in% c("New Jersey", "New York") ~ "Region 2",
      geography %in%
        c("Delaware", "Maryland", "Pennsylvania", "Virginia", "West Virginia") ~
        "Region 3",
      geography %in%
        c(
          "Alabama",
          "Florida",
          "Georgia",
          "Kentucky",
          "Mississippi",
          "North Carolina",
          "South Carolina",
          "Tennessee"
        ) ~
        "Region 4",
      geography %in%
        c("Illinois", "Indiana", "Michigan", "Minnesota", "Ohio", "Wisconsin") ~
        "Region 5",
      geography %in%
        c("Arkansas", "Louisiana", "New Mexico", "Oklahoma", "Texas") ~
        "Region 6",
      geography %in% c("Iowa", "Kansas", "Missouri", "Nebraska") ~ "Region 7",
      geography %in%
        c(
          "Colorado",
          "Montana",
          "North Dakota",
          "South Dakota",
          "Utah",
          "Wyoming"
        ) ~
        "Region 8",
      geography %in% c("Arizona", "California", "Hawaii", "Nevada") ~ "Region 9"
    )
  ) %>%
  group_by(hhs) %>%
  summarize(popsize = sum(popsize)) %>%
  filter(!is.na(hhs))

vroom::vroom_write(pop_unstra_hhs, "resources/pop_hhs.csv.gz", ",")
vroom::vroom_write(pop_stra, "resources/pop_state_age.csv.gz", ",")
vroom::vroom_write(pop_unstra, "resources/pop_state.csv.gz", ",")
