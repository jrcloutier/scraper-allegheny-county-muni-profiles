# Allegheny County Municipal Profile Scraper
# This script scrapes detailed information from Allegheny County's municipal profiles
# Data source: https://apps.alleghenycounty.us/website/MuniProfile.asp

# Required libraries for web scraping and data manipulation
library(rvest)      # Web scraping
library(dplyr)      # Data manipulation
library(purrr)      # Functional programming tools
library(stringr)    # String manipulation
library(httr)       # HTTP requests
library(tibble)     # Modern data frames
library(here)
library(readr)

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

#' Scrape a single municipality profile
#' @param muni_id Municipality ID (1-130)
#' @return List containing municipality data or NULL if scraping fails
scrape_municipality <- function(muni_id) {
  url <- paste0("https://apps.alleghenycounty.us/website/MuniProfile.asp?muni=", muni_id)

  cat("Scraping municipality", muni_id, "...\n")

  # Respectful delay between requests
  Sys.sleep(1)

  # Initialize result list with all expected fields
  result <- list(
    municipality = NA,                  # Municipality name
    county_council_district = NA,       # County council district number
    council_representative = NA,        # County council representative
    senatorial_district = NA,           # State senate district
    legislative_district = NA,          # State house district
    congressional_district = NA,        # US congressional district
    council_of_government = NA,         # Council of governments membership
    school_district = NA,               # School district name
    square_miles = NA                   # Geographic area
  )

  tryCatch({
    # Read the page
    page <- read_html(url)

    # Set municipality name from our mapping and remove any footnotes
    if (muni_id <= length(municipality_names)) {
      # Remove trailing footnote numbers (e.g., " 3", " 5", " 1")
      result$municipality <- gsub("\\s+\\d+$", "", municipality_names[muni_id])
    }

    # Extract school district using XPath and remove footnotes
    school_district <- safe_extract(page, "//td[contains(., 'School District:')]/following-sibling::td[1]")
    if (!is.na(school_district)) {
      # Remove special characters like º and trailing footnote numbers
      result$school_district <- gsub("[º\u0095]|\\s+\\d+$", "", trimws(school_district))
    }

    # Extract additional fields using table-based XPath with exact b tag matches
    council_district <- safe_extract(page, "//td[b[text()='County Council District:']]/following-sibling::td")
    if (!is.na(council_district)) {
      result$county_council_district <- trimws(council_district)
    }

    # Extract additional fields with corrected XPath selectors
    senatorial <- safe_extract(page, "//td[b[contains(text(), 'Senatorial District:')]]/following-sibling::td")
    if (!is.na(senatorial)) {
      result$senatorial_district <- trimws(senatorial)
    }

    legislative <- safe_extract(page, "//td[b[contains(text(), 'Legislative District:')]]/following-sibling::td")
    if (!is.na(legislative)) {
      result$legislative_district <- trimws(legislative)
    }

    congressional <- safe_extract(page, "//td[b[contains(text(), 'Congressional District:')]]/following-sibling::td")
    if (!is.na(congressional)) {
      result$congressional_district <- trimws(congressional)
    }

    cog <- safe_extract(page, "//td[b[contains(text(), 'Council  of Government:')]]/following-sibling::td")
    if (!is.na(cog)) {
      result$council_of_government <- trimws(gsub("\\s+", " ", cog))
    }

    square_miles <- safe_extract(page, "//td[b[text()='Square Miles:']]/following-sibling::td")
    if (!is.na(square_miles)) {
      result$square_miles <- as.numeric(gsub("[^0-9.]", "", square_miles))
    }

    # Clean up any remaining pipe characters and whitespace
    result <- map(result, function(x) {
      if (is.character(x)) {
        x <- str_trim(x)
        x <- str_replace_all(x, "\\|", "")
        x <- str_trim(x)
        if (is.na(x) || x == "" || x == " ") {
          return(NA)
        }
      }
      return(x)
    })

    return(result)

  }, error = function(e) {
    cat("Error scraping municipality", muni_id, ":", e$message, "\n")
    return(NULL)
  })
}

# Create municipality name mapping (based on the list page)
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

#' Scrape data for all Allegheny County municipalities
#' @return Data frame containing all municipal data
scrape_all_municipalities <- function() {
  cat("Starting to scrape all municipalities...\n")

  # Get municipality numbers (1-130)
  municipality_ids <- 1:130

  # Initialize results list
  all_results <- list()

  # Scrape each municipality
  for (i in municipality_ids) {
    result <- scrape_municipality(i)
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

  # Add municipality code
  df <- df |>
  mutate(
    muni_code = case_when(
      municipality == "Aleppo Township" ~ "901",
      municipality == "Borough of Aspinwall" ~ "801",
      municipality == "Borough of Avalon" ~ "802",
      municipality == "Borough of Baldwin" ~ "877",
      municipality == "Baldwin Township" ~ "902",
      municipality == "Borough of Bell Acres" ~ "883",
      municipality == "Borough of Bellevue" ~ "803",
      municipality == "Borough of Ben Avon" ~ "804",
      municipality == "Borough of Ben Avon Hts." ~ "805",
      municipality == "Municipality of Bethel Park" ~ "876",
      municipality == "Borough of Blawnox" ~ "806",
      municipality == "Borough of Brackenridge" ~ "807",
      municipality == "Borough of Braddock" ~ "808",
      municipality == "Borough of Braddock Hills" ~ "872",
      municipality == "Borough of Bradford Woods" ~ "809",
      municipality == "Borough of Brentwood" ~ "810",
      municipality == "Borough of Bridgeville" ~ "811",
      municipality == "Borough of Carnegie" ~ "812",
      municipality == "Borough of Castle Shannon" ~ "813",
      municipality == "Borough of Chalfant" ~ "814",
      municipality == "Borough of Cheswick" ~ "815",
      municipality == "Borough of Churchill" ~ "816",
      municipality == "City of Clairton" ~ "200",
      municipality == "Collier Township" ~ "905",
      municipality == "Borough of Coraopolis" ~ "817",
      municipality == "Borough of Crafton" ~ "818",
      municipality == "Crescent Township" ~ "906",
      municipality == "Borough of Dormont" ~ "819",
      municipality == "Borough of Dravosburg" ~ "820",
      municipality == "City of Duquesne" ~ "300",
      municipality == "East Deer Township" ~ "907",
      municipality == "Borough of East McKeesport" ~ "821",
      municipality == "Borough of East Pittsburgh" ~ "822",
      municipality == "Borough of Edgewood" ~ "823",
      municipality == "Borough of Edgeworth" ~ "824",
      municipality == "Borough of Elizabeth" ~ "825",
      municipality == "Elizabeth Township" ~ "908",
      municipality == "Borough of Emsworth" ~ "826",
      municipality == "Borough of Etna" ~ "827",
      municipality == "Fawn Township" ~ "909",
      municipality == "Findlay Township" ~ "910",
      municipality == "Borough of Forest Hills" ~ "828",
      municipality == "Forward Township" ~ "911",
      municipality == "Borough of Fox Chapel" ~ "868",
      municipality == "Borough of Franklin Park" ~ "884",
      municipality == "Frazer Township" ~ "913",
      municipality == "Borough of Glassport" ~ "829",
      municipality == "Borough of Glenfield" ~ "830",
      municipality == "Borough of Green Tree" ~ "831",
      municipality == "Hampton Township" ~ "914",
      municipality == "Harmar Township" ~ "915",
      municipality == "Harrison Township" ~ "916",
      municipality == "Borough of Haysville" ~ "832",
      municipality == "Borough of Heidelberg" ~ "833",
      municipality == "Borough of Homestead" ~ "834",
      municipality == "Indiana Township" ~ "917",
      municipality == "Borough of Ingram" ~ "835",
      municipality == "Borough of Jefferson Hills" ~ "878",
      municipality == "Kennedy Township" ~ "919",
      municipality == "Kilbuck Township" ~ "920",
      municipality == "Leet Township" ~ "921",
      municipality == "Borough of Leetsdale" ~ "836",
      municipality == "Borough of Liberty" ~ "837",
      municipality == "Borough of Lincoln" ~ "881",
      municipality == "Marshall Township" ~ "923",
      municipality == "Town of McCandless" ~ "927",
      municipality == "Borough of McDonald" ~ "841",
      municipality == "City of McKeesport" ~ "400",
      municipality == "Borough of McKees Rocks" ~ "842",
      municipality == "Borough of Millvale" ~ "838",
      municipality == "Municipality of Monroeville" ~ "879",
      municipality == "Moon Township" ~ "925",
      municipality == "Municipality of Mt. Lebanon" ~ "926",
      municipality == "Borough of Mt. Oliver" ~ "839",
      municipality == "Borough of Munhall" ~ "840",
      municipality == "Neville Township" ~ "928",
      municipality == "North Braddock Borough" ~ "843",
      municipality == "North Fayette Township" ~ "929",
      municipality == "North Versailles Township" ~ "930",
      municipality == "Borough of Oakdale" ~ "844",
      municipality == "Borough of Oakmont" ~ "845",
      municipality == "O'Hara Township" ~ "931",
      municipality == "Ohio Township" ~ "932",
      municipality == "Borough of Glen Osborne" ~ "846",
      municipality == "Municipality of Penn Hills" ~ "934",
      municipality == "Pennsbury Village" ~ "871",
      municipality == "Pine Township" ~ "935",
      municipality == "Borough of Pitcairn" ~ "847",
      municipality == "City of Pittsburgh" ~ "100",
      municipality == "Borough of Pleasant Hills" ~ "873",
      municipality == "Borough of Plum" ~ "880",
      municipality == "Borough of Port Vue" ~ "848",
      municipality == "Borough of Rankin" ~ "849",
      municipality == "Reserve Township" ~ "937",
      municipality == "Richland Township" ~ "938",
      municipality == "Robinson Township" ~ "939",
      municipality == "Ross Township" ~ "940",
      municipality == "Borough of Rosslyn Farms" ~ "850",
      municipality == "Scott Township" ~ "941",
      municipality == "Borough of Sewickley" ~ "851",
      municipality == "Borough of Sewickley Hts." ~ "869",
      municipality == "Borough of Sewickley Hills" ~ "882",
      municipality == "Shaler Township" ~ "944",
      municipality == "Borough of Sharpsburg" ~ "852",
      municipality == "South Fayette Township" ~ "946",
      municipality == "South Park Township" ~ "945",
      municipality == "South Versailles Township" ~ "947",
      municipality == "Borough of Springdale" ~ "853",
      municipality == "Springdale Township" ~ "948",
      municipality == "Stowe Township" ~ "949",
      municipality == "Borough of Swissvale" ~ "854",
      municipality == "Borough of Tarentum" ~ "855",
      municipality == "Borough of Thornburg" ~ "856",
      municipality == "Borough of Trafford" ~ "857",
      municipality == "Borough of Turtle Creek" ~ "858",
      municipality == "Upper St. Clair Township" ~ "950",
      municipality == "Borough of Verona" ~ "859",
      municipality == "Borough of Versailles" ~ "860",
      municipality == "Borough of Wall" ~ "861",
      municipality == "West Deer Township" ~ "952",
      municipality == "Borough of West Elizabeth" ~ "862",
      municipality == "Borough of West Homestead" ~ "863",
      municipality == "Borough of West Mifflin" ~ "870",
      municipality == "Borough of West View" ~ "864",
      municipality == "Borough of Whitaker" ~ "865",
      municipality == "Borough of White Oak" ~ "875",
      municipality == "Borough of Whitehall" ~ "874",
      municipality == "Wilkins Township" ~ "953",
      municipality == "Borough of Wilkinsburg" ~ "866",
      municipality == "Borough of Wilmerding" ~ "867"
    )
  )

  df <- df |>
  mutate(
    school_district = str_squish(str_to_upper(school_district)),
    school_code = case_when(
      school_district == "ALLEGHENY VALLEY" ~ "1",
      school_district == "AVONWORTH" ~ "2",
      school_district =="BALDWIN-WHITEHALL" ~ "4",
      school_district == "BETHEL PARK" ~ "5",
      school_district == "BRENTWOOD" ~ "6",
      school_district == "CARLYNTON" ~ "7",
      school_district == "CHARTIERS VALLEY" ~ "8",
      school_district == "CLAIRTON" ~ "10",
      school_district == "CORNELL" ~ "11",
      school_district == "DEER LAKES" ~ "12",
      school_district == "DUQUESNE AREA" ~ "13",
      school_district == "EAST ALLEGHENY" ~ "14",
      school_district == "ELIZABETH-FORWARD" ~ "16",
      school_district == "FORT CHERRY º" ~ "48",
      school_district == "FOX CHAPEL AREA" ~ "17",
      school_district == "GATEWAY" ~ "18",
      school_district == "HAMPTON" ~ "20",
      school_district == "HIGHLANDS" ~ "21",
      school_district == "KEYSTONE OAKS" ~ "22",
      school_district == "MCKEESPORT AREA" ~ "23",
      school_district == "MONTOUR" ~ "24",
      school_district == "MOON AREA" ~ "25",
      school_district == "MT. LEBANON" ~ "26",
      school_district == "NORTH ALLEGHENY" ~ "27",
      school_district == "NORTH HILLS" ~ "28",
      school_district == "NORTHGATE" ~ "29",
      school_district == "PENN HILLS" ~ "30",
      school_district == "PENN-TRAFFORD º" ~ "49",
      school_district == "PINE-RICHLAND" ~ "3",
      school_district == "PITTSBURGH" ~ "47",
      school_district == "PLUM" ~ "31",
      school_district == "QUAKER VALLEY" ~ "32",
      school_district == "RIVERVIEW" ~ "33",
      school_district == "SHALER AREA" ~ "34",
      school_district == "SOUTH ALLEGHENY" ~ "35",
      school_district == "SOUTH FAYETTE" ~ "36",
      school_district == "SOUTH PARK" ~ "37",
      school_district == "STEEL VALLEY" ~ "38",
      school_district == "STO-ROX" ~ "39",
      school_district == "UPPER ST. CLAIR" ~ "42",
      school_district == "WEST ALLEGHENY" ~ "43",
      school_district == "WEST JEFFERSON" ~ "44",
      school_district == "WEST MIFFLIN AREA" ~ "45",
      school_district == "WILKINSBURG" ~ "46",
      school_district == "WOODLAND HILLS" ~ "9"
    )
  )

  return(df)
}

dat <- scrape_all_municipalities()

lookup <- dat |>
  select(
    muni_code,
    municipality,
    school_district,
    school_code,
    county_council_district,
    senatorial_district,
    legislative_district,
    congressional_district,
    council_of_government,
    square_miles
  )

write_csv(lookup, here("data", "muni-lookup.csv"))
