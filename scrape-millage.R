# Simple Millage Scraper for Allegheny County
# Scrapes municipal and school district millage rates for specified years
# Sources:
#   Municipal: https://apps.alleghenycounty.us/website/MillMuni.asp
#   School: https://apps.alleghenycounty.us/website/millsd.asp

library(rvest)
library(dplyr)
library(purrr)
library(stringr)
library(readr)
library(here)

#' Scrape municipal millage rates for a specific year
#' @param year Year to scrape
#' @return Data frame with municipality names and millage rates
scrape_muni_millage <- function(year) {
  url <- paste0("https://apps.alleghenycounty.us/website/MillMuni.asp?Year=", year)

  cat("Scraping municipal millage for", year, "...\n")
  Sys.sleep(1)

  tryCatch({
    page <- read_html(url)
    table_rows <- html_nodes(page, "table tr")
    data_rows <- table_rows[-c(1, 2)]  # Skip first two rows (malformed header rows)

    millage_data <- map_df(data_rows, function(row) {
      cells <- html_nodes(row, "td")

      if (length(cells) >= 4) {
        municipality <- html_text(cells[1], trim = TRUE)
        millage <- html_text(cells[4], trim = TRUE)
        millage_clean <- gsub("[^0-9.]", "", millage)
        millage_numeric <- ifelse(millage_clean == "", NA, as.numeric(millage_clean))

        # Check for land millage (column 5) - only applicable to some municipalities
        land_millage_numeric <- NA
        if (length(cells) >= 5) {
          land_millage <- html_text(cells[5], trim = TRUE)
          land_millage_clean <- gsub("[^0-9.]", "", land_millage)
          land_millage_numeric <- ifelse(land_millage_clean == "", NA, as.numeric(land_millage_clean))
        }

        tibble(
          municipality = municipality,
          tax_year = year,
          millage = millage_numeric,
          land_millage = land_millage_numeric
        )
      } else {
        NULL
      }
    })

    # Filter out empty rows and separate county from municipalities
    millage_data <- millage_data |>
      filter(!is.na(municipality), municipality != "", !is.na(millage))

    cat("  Found", nrow(millage_data), "entries\n")
    return(millage_data)

  }, error = function(e) {
    cat("Error scraping municipal millage for year", year, ":", e$message, "\n")
    return(NULL)
  })
}

#' Scrape school district millage rates for a specific year
#' @param year Year to scrape
#' @return Data frame with school district names and millage rates
scrape_school_millage <- function(year) {
  url <- paste0("https://apps.alleghenycounty.us/website/millsd.asp?Year=", year)

  cat("Scraping school district millage for", year, "...\n")
  Sys.sleep(1)

  tryCatch({
    page <- read_html(url)
    table_rows <- html_nodes(page, "table tr")
    data_rows <- table_rows[-c(1, 2)]  # Skip first two rows (malformed header rows)

    millage_data <- map_df(data_rows, function(row) {
      cells <- html_nodes(row, "td")

      if (length(cells) >= 3) {
        school_district <- html_text(cells[1], trim = TRUE)
        millage <- html_text(cells[3], trim = TRUE)
        millage_clean <- gsub("[^0-9.]", "", millage)
        millage_numeric <- ifelse(millage_clean == "", NA, as.numeric(millage_clean))

        # Check for land millage (column 4) - only applicable to some school districts
        land_millage_numeric <- NA
        if (length(cells) >= 4) {
          land_millage <- html_text(cells[4], trim = TRUE)
          land_millage_clean <- gsub("[^0-9.]", "", land_millage)
          land_millage_numeric <- ifelse(land_millage_clean == "", NA, as.numeric(land_millage_clean))
        }

        tibble(
          school = school_district,
          tax_year = year,
          millage = millage_numeric,
          land_millage = land_millage_numeric
        )
      } else {
        NULL
      }
    })

    # Filter out empty rows
    millage_data <- millage_data |>
      filter(!is.na(school), school != "", !is.na(millage)) |>
      # Clean up school district names - remove special characters and extra whitespace
      mutate(school = str_squish(str_to_upper(gsub("[º\u0095]", "", school)))) |>
      # Remove duplicates (same school district appears only once per year)
      distinct()

    cat("  Found", nrow(millage_data), "school districts\n")
    return(millage_data)

  }, error = function(e) {
    cat("Error scraping school millage for year", year, ":", e$message, "\n")
    return(NULL)
  })
}

#' Main function to scrape all millage data
#' @param years Vector of years to scrape (default 2018:2025)
scrape_all_millage <- function(years = 2018:2025) {
  cat("=== ALLEGHENY COUNTY MILLAGE SCRAPER ===\n\n")

  # Scrape municipal millage
  cat("STEP 1: Scraping municipal millage rates...\n")
  muni_data <- map_df(years, scrape_muni_millage)

  # Separate county from municipalities
  county_data <- muni_data |>
    filter(municipality == "Allegheny County") |>
    mutate(county = "Allegheny County") |>
    select(county, tax_year, millage)

  muni_data <- muni_data |>
    filter(municipality != "Allegheny County") |>
    # Clean up municipality names - remove footnote numbers
    mutate(municipality = gsub("\\s+\\d+$", "", municipality))

  # Scrape school district millage
  cat("\nSTEP 2: Scraping school district millage rates...\n")
  school_data <- map_df(years, scrape_school_millage)

  # Add municipality and school district codes
  cat("\nSTEP 3: Adding codes...\n")

  # Municipality codes mapping
  muni_data <- muni_data |>
    mutate(
      muni_code = case_when(
        municipality == "Aleppo Township" ~ "901",
        municipality == "Aspinwall Borough" ~ "801",
        municipality == "Avalon Borough" ~ "802",
        municipality == "Baldwin Borough" ~ "877",
        municipality == "Baldwin Township" ~ "902",
        municipality == "Bell Acres Borough" ~ "883",
        municipality == "Bellevue Borough" ~ "803",
        municipality == "Ben Avon Borough" ~ "804",
        municipality == "Ben Avon Heights Borough" ~ "805",
        municipality == "Bethel Park" ~ "876",
        municipality == "Blawnox Borough" ~ "806",
        municipality == "Brackenridge Borough" ~ "807",
        municipality == "Braddock Borough" ~ "808",
        municipality == "Braddock Hills Borough" ~ "872",
        municipality == "Bradford Woods Borough" ~ "809",
        municipality == "Brentwood Borough" ~ "810",
        municipality == "Bridgeville Borough" ~ "811",
        municipality == "Carnegie Borough" ~ "812",
        municipality == "Castle Shannon Borough" ~ "813",
        municipality == "Chalfant Borough" ~ "814",
        municipality == "Cheswick Borough" ~ "815",
        municipality == "Churchill Borough" ~ "816",
        municipality == "City of Clairton" ~ "200",
        municipality == "Collier Township" ~ "905",
        municipality == "Coraopolis Borough" ~ "817",
        municipality == "Crafton Borough" ~ "818",
        municipality == "Crescent Township" ~ "906",
        municipality == "Dormont Borough" ~ "819",
        municipality == "Dravosburg Borough" ~ "820",
        municipality == "City of Duquesne" ~ "300",
        municipality == "East Deer Township" ~ "907",
        municipality == "East McKeesport Borough" ~ "821",
        municipality == "East Pittsburgh Borough" ~ "822",
        municipality == "Edgewood Borough" ~ "823",
        municipality == "Edgeworth Borough" ~ "824",
        municipality == "Elizabeth Borough" ~ "825",
        municipality == "Elizabeth Township" ~ "908",
        municipality == "Emsworth Borough" ~ "826",
        municipality == "Etna Borough" ~ "827",
        municipality == "Fawn Township" ~ "909",
        municipality == "Findlay Township" ~ "910",
        municipality == "Forest Hills Borough" ~ "828",
        municipality == "Forward Township" ~ "911",
        municipality == "Fox Chapel Borough" ~ "868",
        municipality == "Franklin Park Borough" ~ "884",
        municipality == "Frazer Township" ~ "913",
        municipality == "Glassport Borough" ~ "829",
        municipality == "Glenfield Borough" ~ "830",
        municipality == "Green Tree Borough" ~ "831",
        municipality == "Hampton Township" ~ "914",
        municipality == "Harmar Township" ~ "915",
        municipality == "Harrison Township" ~ "916",
        municipality == "Haysville Borough" ~ "832",
        municipality == "Heidelberg Borough" ~ "833",
        municipality == "Homestead Borough" ~ "834",
        municipality == "Indiana Township" ~ "917",
        municipality == "Ingram Borough" ~ "835",
        municipality == "Jefferson Hills Borough" ~ "878",
        municipality == "Kennedy Township" ~ "919",
        municipality == "Kilbuck Township" ~ "920",
        municipality == "Leet Township" ~ "921",
        municipality == "Leetsdale Borough" ~ "836",
        municipality == "Liberty Borough" ~ "837",
        municipality == "Lincoln Borough" ~ "881",
        municipality == "Marshall Township" ~ "923",
        municipality == "McCandless Township" ~ "927",
        municipality == "McDonald Borough" ~ "841",
        municipality == "City of McKeesport" ~ "400",
        municipality == "McKees Rocks Borough" ~ "842",
        municipality == "Millvale Borough" ~ "838",
        municipality == "Monroeville Municipality" ~ "879",
        municipality == "Moon Township" ~ "925",
        municipality == "Mount Lebanon" ~ "926",
        municipality == "Mount Oliver Borough" ~ "839",
        municipality == "Munhall Borough" ~ "840",
        municipality == "Neville Township" ~ "928",
        municipality == "North Braddock Borough" ~ "843",
        municipality == "North Fayette Township" ~ "929",
        municipality == "North Versailles Township" ~ "930",
        municipality == "Oakdale Borough" ~ "844",
        municipality == "Oakmont Borough" ~ "845",
        municipality == "O'Hara Township" ~ "931",
        municipality == "Ohio Township" ~ "932",
        municipality == "Glen Osborne Borough" ~ "846",
        municipality == "Penn Hills Township" ~ "934",
        municipality == "Pennsbury Village" ~ "871",
        municipality == "Pine Township" ~ "935",
        municipality == "Pitcairn Borough" ~ "847",
        municipality == "City of Pittsburgh" ~ "100",
        municipality == "Pleasant Hills Borough" ~ "873",
        municipality == "Plum Borough" ~ "880",
        municipality == "Port Vue Borough" ~ "848",
        municipality == "Rankin Borough" ~ "849",
        municipality == "Reserve Township" ~ "937",
        municipality == "Richland Township" ~ "938",
        municipality == "Robinson Township" ~ "939",
        municipality == "Ross Township" ~ "940",
        municipality == "Rosslyn Farms Borough" ~ "850",
        municipality == "Scott Township" ~ "941",
        municipality == "Sewickley Borough" ~ "851",
        municipality == "Sewickley Heights Borough" ~ "869",
        municipality == "Sewickley Hills Borough" ~ "882",
        municipality == "Shaler Township" ~ "944",
        municipality == "Sharpsburg Borough" ~ "852",
        municipality == "South Fayette Township" ~ "946",
        municipality == "South Park Township" ~ "945",
        municipality == "South Versailles Township" ~ "947",
        municipality == "Springdale Borough" ~ "853",
        municipality == "Springdale Township" ~ "948",
        municipality == "Stowe Township" ~ "949",
        municipality == "Swissvale Borough" ~ "854",
        municipality == "Tarentum Borough" ~ "855",
        municipality == "Thornburg Borough" ~ "856",
        municipality == "Trafford Borough" ~ "857",
        municipality == "Turtle Creek Borough" ~ "858",
        municipality == "Upper St. Clair Township" ~ "950",
        municipality == "Verona Borough" ~ "859",
        municipality == "Versailles Borough" ~ "860",
        municipality == "Wall Borough" ~ "861",
        municipality == "West Deer Township" ~ "952",
        municipality == "West Elizabeth Borough" ~ "862",
        municipality == "West Homestead Borough" ~ "863",
        municipality == "West Mifflin Borough" ~ "870",
        municipality == "West View Borough" ~ "864",
        municipality == "Whitaker Borough" ~ "865",
        municipality == "White Oak Borough" ~ "875",
        municipality == "Whitehall Borough" ~ "874",
        municipality == "Wilkins Township" ~ "953",
        municipality == "Wilkinsburg Borough" ~ "866",
        municipality == "Wilmerding Borough" ~ "867"
      )
    ) |>
    select(municipality, muni_code, tax_year, millage, land_millage) |>
    arrange(municipality, tax_year)

  # School district codes mapping
  school_data <- school_data |>
    mutate(
      school_code = case_when(
        school == "ALLEGHENY VALLEY" ~ "1",
        school == "AVONWORTH" ~ "2",
        school == "BALDWIN-WHITEHALL" ~ "4",
        school == "BETHEL PARK" ~ "5",
        school == "BRENTWOOD" ~ "6",
        school == "CARLYNTON" ~ "7",
        school == "CHARTIERS VALLEY" ~ "8",
        school == "CLAIRTON" ~ "10",
        school == "CORNELL" ~ "11",
        school == "DEER LAKES" ~ "12",
        school == "DUQUESNE AREA" ~ "13",
        school == "EAST ALLEGHENY" ~ "14",
        school == "ELIZABETH-FORWARD" ~ "16",
        school == "FORT CHERRY" ~ "48",
        school == "FOX CHAPEL AREA" ~ "17",
        school == "GATEWAY" ~ "18",
        school == "HAMPTON" ~ "20",
        school == "HIGHLANDS" ~ "21",
        school == "KEYSTONE OAKS" ~ "22",
        school == "MCKEESPORT AREA" ~ "23",
        school == "MONTOUR" ~ "24",
        school == "MOON AREA" ~ "25",
        school == "MT. LEBANON" ~ "26",
        school == "NORTH ALLEGHENY" ~ "27",
        school == "NORTH HILLS" ~ "28",
        school == "NORTHGATE" ~ "29",
        school == "PENN HILLS" ~ "30",
        school == "PENN-TRAFFORD" ~ "49",
        school == "PINE-RICHLAND" ~ "3",
        school == "PITTSBURGH" ~ "47",
        school == "PLUM" ~ "31",
        school == "QUAKER VALLEY" ~ "32",
        school == "RIVERVIEW" ~ "33",
        school == "SHALER AREA" ~ "34",
        school == "SOUTH ALLEGHENY" ~ "35",
        school == "SOUTH FAYETTE" ~ "36",
        school == "SOUTH PARK" ~ "37",
        school == "STEEL VALLEY" ~ "38",
        school == "STO-ROX" ~ "39",
        school == "UPPER ST. CLAIR" ~ "42",
        school == "WEST ALLEGHENY" ~ "43",
        school == "WEST JEFFERSON" ~ "44",
        school == "WEST MIFFLIN AREA" ~ "45",
        school == "WILKINSBURG" ~ "46",
        school == "WOODLAND HILLS" ~ "9"
      )
    ) |>
    select(school, school_code, tax_year, millage, land_millage) |>
    arrange(school, tax_year)

  # Save files - preserve historical data by merging with existing files
  cat("\nSTEP 4: Saving files...\n")

  # Load existing data if files exist, otherwise create empty dataframes
  muni_file <- here("data", "millage-muni.csv")
  school_file <- here("data", "millage-school.csv")
  county_file <- here("data", "millage-county.csv")

  # Municipal millage - preserve existing data
  if (file.exists(muni_file)) {
    existing_muni <- read_csv(muni_file, show_col_types = FALSE) |>
      mutate(
        muni_code = as.character(muni_code),
        # Clean footnotes from existing data too
        municipality = gsub("\\s+\\d+$", "", municipality)
      )
    muni_data <- bind_rows(existing_muni, muni_data) |>
      # Remove rows with NA muni_code (these are the old footnoted entries)
      filter(!is.na(muni_code)) |>
      distinct(municipality, muni_code, tax_year, .keep_all = TRUE) |>
      arrange(municipality, tax_year)
    cat("  Merged with existing municipal data\n")
  }

  # School millage - preserve existing data
  if (file.exists(school_file)) {
    existing_school <- read_csv(school_file, show_col_types = FALSE) |>
      mutate(school_code = as.character(school_code))  # Ensure character type
    school_data <- bind_rows(existing_school, school_data) |>
      # Remove rows with NA school_code (these are old entries before Penn-Trafford was assigned a code)
      filter(!is.na(school_code)) |>
      distinct(school, school_code, tax_year, .keep_all = TRUE) |>
      arrange(school, tax_year)
    cat("  Merged with existing school data\n")
  }

  # County millage - preserve existing data
  if (file.exists(county_file)) {
    existing_county <- read_csv(county_file, show_col_types = FALSE)
    county_data <- bind_rows(existing_county, county_data) |>
      distinct(county, tax_year, .keep_all = TRUE) |>
      arrange(tax_year)
    cat("  Merged with existing county data\n")
  }

  # Ensure codes are character type before saving
  muni_data <- muni_data |> mutate(muni_code = as.character(muni_code))
  school_data <- school_data |> mutate(school_code = as.character(school_code))

  write_csv(muni_data, muni_file)
  write_csv(school_data, school_file)
  write_csv(county_data, county_file)

  cat("\n=== SCRAPING COMPLETE ===\n")
  cat("Output files:\n")
  cat("  - data/millage-muni.csv\n")
  cat("  - data/millage-school.csv\n")
  cat("  - data/millage-county.csv\n")
  cat("\nYears included:\n")
  cat("  - Municipal:", paste(sort(unique(muni_data$tax_year)), collapse = ", "), "\n")
  cat("  - School:", paste(sort(unique(school_data$tax_year)), collapse = ", "), "\n")
  cat("  - County:", paste(sort(unique(county_data$tax_year)), collapse = ", "), "\n")
  cat("\nRecord counts:\n")
  cat("  - Municipal:", nrow(muni_data), "records\n")
  cat("  - School:", nrow(school_data), "records\n")
  cat("  - County:", nrow(county_data), "records\n")

  return(list(
    muni = muni_data,
    school = school_data,
    county = county_data
  ))
}

# Main execution - always run when sourced
result <- scrape_all_millage()
