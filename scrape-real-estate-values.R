# Allegheny County Real Estate Values Time Series Scraper
# This script scrapes certified real estate values from municipal profiles
# and tracks changes over time by appending to a historical dataset
# Data source: https://apps.alleghenycounty.us/website/MuniProfile.asp

# Required libraries
library(rvest)
library(dplyr)
library(purrr)
library(stringr)
library(httr)
library(tibble)
library(here)
library(readr)
library(lubridate)

#' Safely extract text from HTML nodes using XPath
#' @param doc HTML document
#' @param xpath XPath selector string
#' @param default Default value if extraction fails (default: NA)
#' @return Extracted text or default value
safe_extract <- function(doc, xpath, default = NA) {
  tryCatch({
    nodes <- html_nodes(doc, xpath = xpath)
    if (length(nodes) > 0) {
      text <- html_text(nodes[1], trim = TRUE)
      if (text == "" || is.null(text)) return(default)
      return(text)
    } else {
      return(default)
    }
  }, error = function(e) {
    return(default)
  })
}

#' Extract the "Value As Of" date from the page
#' @param page HTML document
#' @return Date string in ISO format (e.g., "2026-01-08") or NA
extract_value_as_of_date <- function(page) {
  # Look for text like "Value As Of 1/8/2026:"
  tryCatch({
    # Get the second row of the table which contains the "Value As Of" values
    row_text <- safe_extract(page, "//*[@id='no-more-tables']/table[1]/tbody/tr[2]/td[1]")
    if (!is.na(row_text)) {
      # Extract date - format is "Value As Of 1/8/2026:"
      date_match <- str_extract(row_text, "[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}")
      if (!is.na(date_match)) {
        # Convert from M/D/YYYY to YYYY-MM-DD format
        parsed_date <- mdy(date_match)
        if (!is.na(parsed_date)) {
          return(format(parsed_date, "%Y-%m-%d"))
        }
      }
    }
    return(NA)
  }, error = function(e) {
    return(NA)
  })
}

#' Scrape real estate values for a single municipality
#' @param muni_id Municipality ID (1-130)
#' @param value_as_of_date Date string extracted from the page (to avoid re-extracting)
#' @return List containing municipality data or NULL if scraping fails
scrape_real_estate_values <- function(muni_id, value_as_of_date = NULL) {
  url <- paste0("https://apps.alleghenycounty.us/website/MuniProfile.asp?muni=", muni_id)

  cat("Scraping municipality", muni_id, "...\n")

  # Respectful delay between requests
  Sys.sleep(1)

  tryCatch({
    # Read the page
    page <- read_html(url)

    # Extract the "Value As Of" date if not provided
    if (is.null(value_as_of_date)) {
      value_as_of_date <- extract_value_as_of_date(page)
    }

    # Get municipality name
    municipality_name <- NA
    if (muni_id <= length(municipality_names)) {
      municipality_name <- municipality_names[muni_id]
    }

    # Extract "Value As Of" values from the second row using exact XPath selectors
    taxable_value <- as.numeric(gsub("[^0-9.]", "",
      safe_extract(page, "//*[@id='no-more-tables']/table[1]/tbody/tr[2]/td[2]", "0")))

    exempt_value <- as.numeric(gsub("[^0-9.]", "",
      safe_extract(page, "//*[@id='no-more-tables']/table[1]/tbody/tr[2]/td[3]", "0")))

    purta_value <- as.numeric(gsub("[^0-9.]", "",
      safe_extract(page, "//*[@id='no-more-tables']/table[1]/tbody/tr[2]/td[4]", "0")))

    all_real_estate <- as.numeric(gsub("[^0-9.]", "",
      safe_extract(page, "//*[@id='no-more-tables']/table[1]/tbody/tr[2]/td[5]", "0")))

    # Extract median residential value
    median_text <- safe_extract(page, "//*[contains(text(), 'Taxable Residential Median Value')]")
    median_value <- NA
    if (!is.na(median_text)) {
      # Extract dollar amount from text like "Taxable Residential Median Value as of 1/8/2026: $137,800"
      median_match <- str_extract(median_text, "\\$[0-9,]+")
      if (!is.na(median_match)) {
        median_value <- as.numeric(gsub("[^0-9]", "", median_match))
      }
    }

    # Get muni_code based on municipality name
    muni_code <- get_muni_code(municipality_name)

    # Return result with scrape metadata
    return(list(
      municipality = municipality_name,
      muni_code = muni_code,
      value_as_of_date = value_as_of_date,
      scraped_at = Sys.time(),
      taxable_value = taxable_value,
      exempt_value = exempt_value,
      purta_value = purta_value,
      all_real_estate = all_real_estate,
      median_residential_value = median_value
    ))

  }, error = function(e) {
    cat("Error scraping municipality", muni_id, ":", e$message, "\n")
    return(NULL)
  })
}

# Municipality names mapping (same as in scrape-profiles.R)
municipality_names <- c(
  "Aleppo Township", "Borough of Aspinwall", "Borough of Avalon",
  "Borough of Baldwin", "Baldwin Township", "Borough of Bell Acres",
  "Borough of Bellevue", "Borough of Ben Avon", "Borough of Ben Avon Hts.",
  "Municipality of Bethel Park", "Borough of Blawnox", "Borough of Brackenridge",
  "Borough of Braddock", "Borough of Braddock Hills", "Borough of Bradford Woods",
  "Borough of Brentwood", "Borough of Bridgeville", "Borough of Carnegie",
  "Borough of Castle Shannon", "Borough of Chalfant", "Borough of Cheswick",
  "Borough of Churchill", "City of Clairton", "Collier Township",
  "Borough of Coraopolis", "Borough of Crafton", "Crescent Township",
  "Borough of Dormont", "Borough of Dravosburg", "City of Duquesne",
  "East Deer Township", "Borough of East McKeesport", "Borough of East Pittsburgh",
  "Borough of Edgewood", "Borough of Edgeworth", "Borough of Elizabeth",
  "Elizabeth Township", "Borough of Emsworth", "Borough of Etna",
  "Fawn Township", "Findlay Township", "Borough of Forest Hills",
  "Forward Township", "Borough of Fox Chapel", "Borough of Franklin Park",
  "Frazer Township", "Borough of Glassport", "Borough of Glenfield",
  "Borough of Green Tree", "Hampton Township", "Harmar Township",
  "Harrison Township", "Borough of Haysville", "Borough of Heidelberg",
  "Borough of Homestead", "Indiana Township", "Borough of Ingram",
  "Borough of Jefferson Hills", "Kennedy Township", "Kilbuck Township",
  "Leet Township", "Borough of Leetsdale", "Borough of Liberty",
  "Borough of Lincoln", "Marshall Township", "Town of McCandless",
  "Borough of McDonald", "City of McKeesport", "Borough of McKees Rocks",
  "Borough of Millvale", "Municipality of Monroeville", "Moon Township",
  "Municipality of Mt. Lebanon", "Borough of Mt. Oliver", "Borough of Munhall",
  "Neville Township", "North Braddock Borough", "North Fayette Township",
  "North Versailles Township", "Borough of Oakdale", "Borough of Oakmont",
  "O'Hara Township", "Ohio Township", "Borough of Glen Osborne",
  "Municipality of Penn Hills", "Pennsbury Village", "Pine Township",
  "Borough of Pitcairn", "City of Pittsburgh", "Borough of Pleasant Hills",
  "Borough of Plum", "Borough of Port Vue", "Borough of Rankin",
  "Reserve Township", "Richland Township", "Robinson Township",
  "Ross Township", "Borough of Rosslyn Farms", "Scott Township",
  "Borough of Sewickley", "Borough of Sewickley Hts.", "Borough of Sewickley Hills",
  "Shaler Township", "Borough of Sharpsburg", "South Fayette Township",
  "South Park Township", "South Versailles Township", "Borough of Springdale",
  "Springdale Township", "Stowe Township", "Borough of Swissvale",
  "Borough of Tarentum", "Borough of Thornburg", "Borough of Trafford",
  "Borough of Turtle Creek", "Upper St. Clair Township", "Borough of Verona",
  "Borough of Versailles", "Borough of Wall", "West Deer Township",
  "Borough of West Elizabeth", "Borough of West Homestead", "Borough of West Mifflin",
  "Borough of West View", "Borough of Whitaker", "Borough of White Oak",
  "Borough of Whitehall", "Wilkins Township", "Borough of Wilkinsburg",
  "Borough of Wilmerding"
)

#' Get municipality code based on municipality name
#' @param municipality_name Name of the municipality
#' @return Municipality code or NA
get_muni_code <- function(municipality_name) {
  if (is.na(municipality_name)) return(NA)

  code <- case_when(
    municipality_name == "Aleppo Township" ~ "901",
    municipality_name == "Borough of Aspinwall" ~ "801",
    municipality_name == "Borough of Avalon" ~ "802",
    municipality_name == "Borough of Baldwin" ~ "877",
    municipality_name == "Baldwin Township" ~ "902",
    municipality_name == "Borough of Bell Acres" ~ "883",
    municipality_name == "Borough of Bellevue" ~ "803",
    municipality_name == "Borough of Ben Avon" ~ "804",
    municipality_name == "Borough of Ben Avon Hts." ~ "805",
    municipality_name == "Municipality of Bethel Park" ~ "876",
    municipality_name == "Borough of Blawnox" ~ "806",
    municipality_name == "Borough of Brackenridge" ~ "807",
    municipality_name == "Borough of Braddock" ~ "808",
    municipality_name == "Borough of Braddock Hills" ~ "872",
    municipality_name == "Borough of Bradford Woods" ~ "809",
    municipality_name == "Borough of Brentwood" ~ "810",
    municipality_name == "Borough of Bridgeville" ~ "811",
    municipality_name == "Borough of Carnegie" ~ "812",
    municipality_name == "Borough of Castle Shannon" ~ "813",
    municipality_name == "Borough of Chalfant" ~ "814",
    municipality_name == "Borough of Cheswick" ~ "815",
    municipality_name == "Borough of Churchill" ~ "816",
    municipality_name == "City of Clairton" ~ "200",
    municipality_name == "Collier Township" ~ "905",
    municipality_name == "Borough of Coraopolis" ~ "817",
    municipality_name == "Borough of Crafton" ~ "818",
    municipality_name == "Crescent Township" ~ "906",
    municipality_name == "Borough of Dormont" ~ "819",
    municipality_name == "Borough of Dravosburg" ~ "820",
    municipality_name == "City of Duquesne" ~ "300",
    municipality_name == "East Deer Township" ~ "907",
    municipality_name == "Borough of East McKeesport" ~ "821",
    municipality_name == "Borough of East Pittsburgh" ~ "822",
    municipality_name == "Borough of Edgewood" ~ "823",
    municipality_name == "Borough of Edgeworth" ~ "824",
    municipality_name == "Borough of Elizabeth" ~ "825",
    municipality_name == "Elizabeth Township" ~ "908",
    municipality_name == "Borough of Emsworth" ~ "826",
    municipality_name == "Borough of Etna" ~ "827",
    municipality_name == "Fawn Township" ~ "909",
    municipality_name == "Findlay Township" ~ "910",
    municipality_name == "Borough of Forest Hills" ~ "828",
    municipality_name == "Forward Township" ~ "911",
    municipality_name == "Borough of Fox Chapel" ~ "868",
    municipality_name == "Borough of Franklin Park" ~ "884",
    municipality_name == "Frazer Township" ~ "913",
    municipality_name == "Borough of Glassport" ~ "829",
    municipality_name == "Borough of Glenfield" ~ "830",
    municipality_name == "Borough of Green Tree" ~ "831",
    municipality_name == "Hampton Township" ~ "914",
    municipality_name == "Harmar Township" ~ "915",
    municipality_name == "Harrison Township" ~ "916",
    municipality_name == "Borough of Haysville" ~ "832",
    municipality_name == "Borough of Heidelberg" ~ "833",
    municipality_name == "Borough of Homestead" ~ "834",
    municipality_name == "Indiana Township" ~ "917",
    municipality_name == "Borough of Ingram" ~ "835",
    municipality_name == "Borough of Jefferson Hills" ~ "878",
    municipality_name == "Kennedy Township" ~ "919",
    municipality_name == "Kilbuck Township" ~ "920",
    municipality_name == "Leet Township" ~ "921",
    municipality_name == "Borough of Leetsdale" ~ "836",
    municipality_name == "Borough of Liberty" ~ "837",
    municipality_name == "Borough of Lincoln" ~ "881",
    municipality_name == "Marshall Township" ~ "923",
    municipality_name == "Town of McCandless" ~ "927",
    municipality_name == "Borough of McDonald" ~ "841",
    municipality_name == "City of McKeesport" ~ "400",
    municipality_name == "Borough of McKees Rocks" ~ "842",
    municipality_name == "Borough of Millvale" ~ "838",
    municipality_name == "Municipality of Monroeville" ~ "879",
    municipality_name == "Moon Township" ~ "925",
    municipality_name == "Municipality of Mt. Lebanon" ~ "926",
    municipality_name == "Borough of Mt. Oliver" ~ "839",
    municipality_name == "Borough of Munhall" ~ "840",
    municipality_name == "Neville Township" ~ "928",
    municipality_name == "North Braddock Borough" ~ "843",
    municipality_name == "North Fayette Township" ~ "929",
    municipality_name == "North Versailles Township" ~ "930",
    municipality_name == "Borough of Oakdale" ~ "844",
    municipality_name == "Borough of Oakmont" ~ "845",
    municipality_name == "O'Hara Township" ~ "931",
    municipality_name == "Ohio Township" ~ "932",
    municipality_name == "Borough of Glen Osborne" ~ "846",
    municipality_name == "Municipality of Penn Hills" ~ "934",
    municipality_name == "Pennsbury Village" ~ "871",
    municipality_name == "Pine Township" ~ "935",
    municipality_name == "Borough of Pitcairn" ~ "847",
    municipality_name == "City of Pittsburgh" ~ "100",
    municipality_name == "Borough of Pleasant Hills" ~ "873",
    municipality_name == "Borough of Plum" ~ "880",
    municipality_name == "Borough of Port Vue" ~ "848",
    municipality_name == "Borough of Rankin" ~ "849",
    municipality_name == "Reserve Township" ~ "937",
    municipality_name == "Richland Township" ~ "938",
    municipality_name == "Robinson Township" ~ "939",
    municipality_name == "Ross Township" ~ "940",
    municipality_name == "Borough of Rosslyn Farms" ~ "850",
    municipality_name == "Scott Township" ~ "941",
    municipality_name == "Borough of Sewickley" ~ "851",
    municipality_name == "Borough of Sewickley Hts." ~ "869",
    municipality_name == "Borough of Sewickley Hills" ~ "882",
    municipality_name == "Shaler Township" ~ "944",
    municipality_name == "Borough of Sharpsburg" ~ "852",
    municipality_name == "South Fayette Township" ~ "946",
    municipality_name == "South Park Township" ~ "945",
    municipality_name == "South Versailles Township" ~ "947",
    municipality_name == "Borough of Springdale" ~ "853",
    municipality_name == "Springdale Township" ~ "948",
    municipality_name == "Stowe Township" ~ "949",
    municipality_name == "Borough of Swissvale" ~ "854",
    municipality_name == "Borough of Tarentum" ~ "855",
    municipality_name == "Borough of Thornburg" ~ "856",
    municipality_name == "Borough of Trafford" ~ "857",
    municipality_name == "Borough of Turtle Creek" ~ "858",
    municipality_name == "Upper St. Clair Township" ~ "950",
    municipality_name == "Borough of Verona" ~ "859",
    municipality_name == "Borough of Versailles" ~ "860",
    municipality_name == "Borough of Wall" ~ "861",
    municipality_name == "West Deer Township" ~ "952",
    municipality_name == "Borough of West Elizabeth" ~ "862",
    municipality_name == "Borough of West Homestead" ~ "863",
    municipality_name == "Borough of West Mifflin" ~ "870",
    municipality_name == "Borough of West View" ~ "864",
    municipality_name == "Borough of Whitaker" ~ "865",
    municipality_name == "Borough of White Oak" ~ "875",
    municipality_name == "Borough of Whitehall" ~ "874",
    municipality_name == "Wilkins Township" ~ "953",
    municipality_name == "Borough of Wilkinsburg" ~ "866",
    municipality_name == "Borough of Wilmerding" ~ "867",
    TRUE ~ NA_character_
  )

  return(code)
}

#' Scrape real estate values for all municipalities
#' @return Data frame containing real estate values for all municipalities
scrape_all_real_estate_values <- function() {
  cat("Starting to scrape real estate values for all municipalities...\n")

  # Get municipality IDs (1-130)
  municipality_ids <- 1:130

  # First, scrape one municipality to get the "Value As Of" date
  cat("Extracting 'Value As Of' date from first municipality...\n")
  first_page <- read_html(paste0("https://apps.alleghenycounty.us/website/MuniProfile.asp?muni=1"))
  value_as_of_date <- extract_value_as_of_date(first_page)

  if (is.na(value_as_of_date)) {
    cat("Warning: Could not extract 'Value As Of' date. Proceeding anyway...\n")
  } else {
    cat("'Value As Of' date found:", value_as_of_date, "\n")
  }

  # Initialize results list
  all_results <- list()

  # Scrape each municipality
  for (i in municipality_ids) {
    result <- scrape_real_estate_values(i, value_as_of_date)
    if (!is.null(result)) {
      all_results[[length(all_results) + 1]] <- result
    }

    # Progress indicator
    if (i %% 10 == 0) {
      cat("Completed", i, "of", length(municipality_ids), "municipalities\n")
    }
  }

  # Convert to data frame
  df <- bind_rows(all_results)

  return(df)
}

#' Get the Friday date for a given date (for weekly grouping)
#' Vectorized function that works with single dates or vectors
#' @param date A date object or vector of dates
#' @return The Friday of the week containing each date
get_week_friday <- function(date) {
  # wday: 1=Sunday, 2=Monday, ..., 6=Friday, 7=Saturday
  # For each date, find the Friday of that week
  # If the date is Saturday (7) or Sunday (1), use the previous Friday
  weekday <- wday(date)
  days_to_friday <- case_when(
    weekday == 1 ~ -2,  # Sunday -> previous Friday
    weekday == 7 ~ -1,  # Saturday -> previous Friday
    TRUE ~ 6 - weekday  # Mon-Fri -> this week's Friday
  )
  return(as.Date(date) + days_to_friday)
}

#' Calculate week-over-week and year-to-date changes for the time series
#' @param data Data frame with time series data
#' @return Data frame with change columns added
calculate_changes <- function(data) {
  data %>%
    arrange(municipality, scrape_week) %>%
    group_by(municipality) %>%
    mutate(
      # Week-over-week change in taxable value
      taxable_value_wow_change = taxable_value - lag(taxable_value),
      taxable_value_wow_pct = round((taxable_value - lag(taxable_value)) / lag(taxable_value) * 100, 4),

      # Year-to-date cumulative change (from first observation of the year)
      year = year(scrape_week),
      first_value_of_year = first(taxable_value[year == year(scrape_week)]),
      taxable_value_ytd_change = taxable_value - first_value_of_year,
      taxable_value_ytd_pct = round((taxable_value - first_value_of_year) / first_value_of_year * 100, 4)
    ) %>%
    ungroup() %>%
    # Clean up: remove helper column, replace NaN/Inf with NA
    select(-first_value_of_year) %>%
    mutate(
      across(c(taxable_value_wow_change, taxable_value_wow_pct,
               taxable_value_ytd_change, taxable_value_ytd_pct),
             ~ifelse(is.nan(.) | is.infinite(.), NA, .))
    )
}

#' Update the real estate values time series file
#' This function scrapes current values and appends them to the historical dataset
#' Weekly snapshots are always recorded for consistent time series tracking
#' @param output_file Path to the output CSV file
#' @return Data frame with updated time series
update_real_estate_time_series <- function(output_file = here("data", "tax-base.csv")) {
  cat("=== Real Estate Values Time Series Update ===\n")
  cat("Starting scrape at:", as.character(Sys.time()), "\n\n")

  # Check if the county has new data before doing the full scrape
  cat("Checking county website for new data...\n")
  check_page <- read_html("https://apps.alleghenycounty.us/website/MuniProfile.asp?muni=1")
  current_value_date <- extract_value_as_of_date(check_page)
  cat("County 'Value As Of' date:", current_value_date, "\n")

  if (!is.na(current_value_date) && file.exists(output_file)) {
    existing_data <- read_csv(output_file, show_col_types = FALSE)
    if (current_value_date %in% existing_data$value_as_of_date) {
      cat("No new data. value_as_of_date", current_value_date, "already captured. Skipping.\n")
      return(invisible(NULL))
    }
  }

  cat("New data found! Proceeding with full scrape...\n\n")

  # Scrape current values
  new_data <- scrape_all_real_estate_values()

  # Convert scraped_at to character for consistent type handling
  new_data$scraped_at <- as.character(new_data$scraped_at)

  # Add scrape_week column (the Friday of the current week)
  current_date <- as.Date(Sys.time())
  scrape_week <- get_week_friday(current_date)
  new_data$scrape_week <- scrape_week
  cat("Scrape week (Friday):", as.character(scrape_week), "\n")

  # Check if historical file exists
  if (file.exists(output_file)) {
    cat("\nReading existing historical data...\n")
    historical_data <- read_csv(output_file, show_col_types = FALSE)

    # Ensure column types match the new data for bind_rows compatibility
    if ("muni_code" %in% names(historical_data)) {
      historical_data$muni_code <- as.character(historical_data$muni_code)
    }
    if ("value_as_of_date" %in% names(historical_data)) {
      historical_data$value_as_of_date <- as.character(historical_data$value_as_of_date)
    }
    if ("scraped_at" %in% names(historical_data)) {
      historical_data$scraped_at <- as.character(historical_data$scraped_at)
    }

    # Ensure scrape_week column exists in historical data
    if (!"scrape_week" %in% names(historical_data)) {
      cat("Adding scrape_week column to historical data...\n")
      # For legacy data, use the scraped_at date to determine the week
      historical_data <- historical_data %>%
        mutate(scrape_week = as.Date(get_week_friday(as.Date(scraped_at))))
    }

    # Check if we already have data for this value_as_of_date
    current_value_as_of <- unique(new_data$value_as_of_date)
    current_value_as_of <- current_value_as_of[!is.na(current_value_as_of)]
    if (length(current_value_as_of) > 0 && current_value_as_of[1] %in% historical_data$value_as_of_date) {
      cat("Data for value_as_of_date", current_value_as_of[1], "already exists.\n")
      cat("Replacing with fresh data...\n")
      historical_data <- historical_data %>%
        filter(!(value_as_of_date %in% current_value_as_of))
    }

    # Append new data to historical data
    combined_data <- bind_rows(historical_data, new_data)
    cat("Added", nrow(new_data), "new records.\n")
    cat("Total historical records:", nrow(historical_data), "\n")
  } else {
    cat("\nNo historical file found. Creating new file...\n")
    combined_data <- new_data
  }

  # Sort by municipality and week
  combined_data <- combined_data %>%
    arrange(municipality, scrape_week)

  # Calculate week-over-week and YTD changes
  cat("Calculating week-over-week and year-to-date changes...\n")
  combined_data <- calculate_changes(combined_data)

  # Write to file
  write_csv(combined_data, output_file)
  cat("\nData written to:", output_file, "\n")
  cat("Total records in file:", nrow(combined_data), "\n")
  cat("Unique weeks in file:", n_distinct(combined_data$scrape_week), "\n")
  cat("Scrape completed at:", as.character(Sys.time()), "\n")

  return(combined_data)
}

# Main execution when script is run directly (not sourced)
if (!interactive() && sys.nframe() == 0) {
  cat("=== Allegheny County Real Estate Values Time Series Scraper ===\n")
  cat("This script tracks certified real estate values over time.\n\n")
  cat("Starting update...\n\n")

  # Run the update
  update_real_estate_time_series()
} else {
  # Just show usage instructions when sourced
  cat("=== Allegheny County Real Estate Values Time Series Scraper ===\n")
  cat("This script tracks certified real estate values over time.\n\n")
  cat("To scrape current values and update the time series:\n")
  cat("  data <- update_real_estate_time_series()\n\n")
  cat("The script will:\n")
  cat("  1. Scrape current 'Value As Of' values from all 130 municipalities\n")
  cat("  2. Extract the 'Value As Of' date from each profile\n")
  cat("  3. Append new data to the historical dataset (avoiding duplicates)\n")
  cat("  4. Save to data/tax-base.csv\n\n")
  cat("Note: The script includes a 1-second delay between requests.\n")
}
