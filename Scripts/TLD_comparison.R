#### Title
# PURPOSE: Country Profile Sandbox
# AUTHOR: alerichardson | sch
# LICENSE: MIT
# DATE: 2024-02-07
# NOTES: 

#### LOCALS & SETUP ============================================================================

# Libraries
require(tidyverse)
require(gagglr)
require(here)
require(googledrive)
#si_setup()

#### FUNCTIONS ============================================================================  

artmisDL <- function(down_path = here::here("Data"), redownload = T, perf_filename = "", rtk_filename = ""){
  
  if(redownload == T){
    
    # Download the Artmis dataset
    perf_path <- "1h9SXDID1H2FSgWJffsfJbgi1Pyv1Em0L"
    file <- googledrive::drive_ls(googledrive::as_id(perf_path))
    perf_filename <- file %>%
      dplyr::filter(stringr::str_detect(name, pattern = "xlsx")) %>%
      dplyr::pull(name)
    glamr::import_drivefile(
      drive_folder = perf_path,
      filename = perf_filename,
      folderpath = down_path,
      zip = FALSE
    )
    
    # Download RTKs
    rtk_path <- "1GNl2b046QBPBxw0o4z1YpKz54uCcYl4w"
    file <- googledrive::drive_ls(googledrive::as_id(rtk_path))
    rtk_filename <- file %>%
      dplyr::filter(stringr::str_detect(name, pattern = "xlsx")) %>%
      dplyr::pull(name)
    glamr::import_drivefile(
      drive_folder = rtk_path,
      filename = rtk_filename[1],
      folderpath = down_path,
      zip = FALSE
    )
    rtk_filename = rtk_filename[1]
  }
  
  # Read in Artmis
  perf_raw <- readxl::read_xlsx(here::here(down_path, perf_filename))
  
  df_artmis <- perf_raw %>%
    janitor::clean_names() %>%
    mutate(country = case_when(country == "Côte d'Ivoire" ~ "Cote d'Ivoire",
                               country == "Congo DRC" ~ "Democratic Republic of the Congo",
                               country == "DRC" ~ "Democratic Republic of the Congo",
                               country == "Eswatini" ~ "eSwatini",
                               TRUE ~ country)) %>%
    readr::write_csv(here::here(down_path, "df_artmis.csv"))
  
  return(df_artmis)
}

artmisWrangle <- function(rtk_filename = NA, RTKs = F, down_path = here::here("Data")){
  
  df_artmis = readr::read_csv(here::here(down_path, "df_artmis.csv"))
  
  df_artmis = df_artmis %>%
    filter(condom_adjusted_task_order == "TO1",
           order_type %in% c("Purchase Order", "Distribution Order"),
           d365_funding_source_detail == "PEPFAR-COP-USAID"
    ) %>%
    select(unit_price,
           ordered_quantity,
           line_total,
           fiscal_year_funding,
           d365_funding_source_detail,
           d365_health_element,
           product_category,
           item_tracer_category,
           country,
           product_name,
           task_order,
           order_type,
           base_unit_multiplier) %>%
    mutate(cop = (as.numeric(str_extract(fiscal_year_funding, "\\d{1,}"))-1)+2000) %>%
    mutate(item_tracer_category = case_when(
      item_tracer_category == "TB HIV" ~ "TB",
      TRUE ~ item_tracer_category
    ))
  
  if(RTKs == T){
    
    rtk_raw = readxl::read_xlsx(file.path(down_path, rtk_filename), sheet = "GHSCTransactionStatusallTransa") %>%
      filter(str_detect(COP, "COP")) %>%
      select(ordered_quantity = `Quantity (kits)`,
             cost = CPT,
             fiscal_year_funding = COP,
             Class,
             country = `Ship-To Country`,
             product_name = Description) %>%
      mutate(cop = as.numeric(str_extract(fiscal_year_funding, "\\d{1,}"))+2000,
             line_total = ordered_quantity*cost) %>%
      filter(Class == "Products",
             cop > 2010) %>%
      mutate(item_tracer_category = "RTK",
             d365_funding_source_detail = "PEPFAR-COP-USAID",
             d365_health_element = "HIV/AIDS",
             task_order  = "TO1",
             order_type = "Purchase Order")
    rtk_raw$country[rtk_raw$country=="Cote D'Ivoire"]<-"Cote d'Ivoire"
    rtk_raw$country[rtk_raw$country=="Congo, DRC"]<-"DRC"
    
    df_artmis = df_artmis %>%
      bind_rows(rtk_raw)
  }
  
  return(df_artmis)
  
}

##function to download commodities dataset
down_commod <- function(commod_path) {
  
  file <- googledrive::drive_ls(googledrive::as_id(commod_path))
  
  commod_filename <- file %>% 
    dplyr::filter(stringr::str_detect(name, pattern = ".txt")) %>%
    dplyr::pull(name)
  
  glamr::import_drivefile(drive_folder = commod_path,
                          filename = commod_filename,
                          folderpath = here("Data"),
                          zip = FALSE)
  return(commod_filename)
}

# Read in matched dataset

down_match <- function(match_path){
  
  file <- googledrive::drive_ls(googledrive::as_id(match_path))
  
  match_filename <- file %>% 
    dplyr::filter(stringr::str_detect(name, pattern = "planned v procured")) %>%
    dplyr::pull(name)
  
  glamr::import_drivefile(drive_folder = match_path,
                          filename = match_filename,
                          folderpath = here("Data"),
                          zip = FALSE)
  return(match_filename)
}

#### LOAD DATA ============================================================================  

artmisDF <- artmisDL()

artmisDF <- artmisWrangle(rtk_filename = "GHSC-RTK Order status report 2023-01-03.xlsx", RTKs = F)

artmisDF$country[artmisDF$country=="DRC"]<-"Democratic Republic of the Congo"


commod_path <- "1YqA0VutptWYs1_cvNeSacb9I7I2a3ovg"

commod_filename = down_commod(commod_path)
match_filename = down_match(match_path)

commod_raw <- readr::read_tsv(here("Data", commod_filename))
match_arv = readr::read_csv(here("Data", "ARVmatch.csv"))
match_arv[match_arv == "NA"]<-NA

#### Align Datasets ============================================================

artmisARV <- artmisDF %>%
  filter(str_detect(item_tracer_category, "ARV"))

# Creating df_commodity with the variables I care about
df_commodity = commod_raw %>%
  mutate(minor_category = case_when(
    minor_category == "ARVs for Adult Treatment" ~ "Adult ARV",
    minor_category == "ARVs for PrEP" ~ "Adult ARV",
    minor_category == "ARVs for Pediatric Treatment" ~ "Pediatric ARV",
    minor_category == "ARVs for Infant Prophylaxis" ~ "Pediatric ARV",
    minor_category %in% c("Other Health Commodities VMMC",
                          "Surgical Kit",
                          "VMMC Device") ~ "VMMC",
    minor_category %in% c("Self Testing",
                          "HIV Tests",
                          "Recency Testing",
                          "Non HIV And Combos") ~ "RTK",
    minor_category == "TB Pharma Prophylaxis" ~ "TB",
    TRUE ~ minor_category
  )) %>%
  select(country,
         fundingagency,
         major_category, 
         minor_category,
         planning_cycle,
         implementation_year,
         commodity_item,
         item_quantity,
         unit_price,
         mech_name) %>%
  mutate(total_cost = item_quantity*unit_price,
         cop = as.numeric(str_extract(planning_cycle, "\\d{1,}"))+2000)

commodityARV = df_commodity %>%
  filter(str_detect(minor_category, "ARV"))

commodityARV$commodity_item[commodityARV$commodity_item == 
                              "Dolutegravir/Lamivudine/Tenofovir DF 50/300/300 mg Tablet, 30 Tablets [OPTIMAL]"
                            ] <- "Dolutegravir/Lamivudine/Tenofovir DF 50/300/300 mg Tablet, 30 Tablets"
commodityARV$commodity_item[commodityARV$commodity_item == 
                              "Dolutegravir/Lamivudine/Tenofovir DF 50/300/300 mg Tablet, 90 Tablets [OPTIMAL]"
                            ] <- "Dolutegravir/Lamivudine/Tenofovir DF (TLD) 50/300/300 mg Tablet, 90 Tablets"
commodityARV$commodity_item[commodityARV$commodity_item == 
                              "Dolutegravir/Lamivudine/Tenofovir DF 50/300/300 mg Tablet, 180 Tablets [OPTIMAL]"
                            ] <- "Dolutegravir/Lamivudine/Tenofovir DF (TLD) 50/300/300 mg Tablet, 180 Tablets"
commodityARV$commodity_item[commodityARV$commodity_item == 
                              "Dolutegravir/Lamivudine/Tenofovir DF (TLD) 50/300/300 mg Tablet, 180 Tablets [OPTIMAL]"
                            ] <- "Dolutegravir/Lamivudine/Tenofovir DF (TLD) 50/300/300 mg Tablet, 180 Tablets"

artmisARV %>%
  filter(product_name %in% match_arv$product_name) %>% 
  group_by(fiscal_year_funding, country, product_name) %>%
  summarise(ordered_quantity = sum(ordered_quantity, na.rm = T)) %>%
  left_join(match_arv) %>%
  left_join(commodityARV %>%
              filter(commodity_item %in% match_arv$commodity_item) %>%
              group_by(planning_cycle, country, commodity_item) %>%
              summarize(item_quantity = sum(item_quantity, na.rm = T)) %>%
              mutate(fiscal_year_funding = paste0("FY", 
                                                  as.numeric(str_extract(planning_cycle, "\\d{2}"))+1)) %>%
              ungroup() %>%
              select(-planning_cycle), by = c("fiscal_year_funding", "country", "commodity_item")) %>%
  filter(!fiscal_year_funding %in% c("FY18", "FY19")) %>%
  rename(ArtmisName = product_name,
         ArtmisQuantity = ordered_quantity,
         PlannedName = commodity_item,
         PlannedQuantity = item_quantity) %>%
  mutate(ArtmisTablets = case_when(
    str_detect(ArtmisName, " 28 ") ~ 28,
    str_detect(ArtmisName, " 30 ") ~ 30,
    str_detect(ArtmisName, " 90 ") ~ 90,
    str_detect(ArtmisName, " 180 ") ~ 180
  ),
         PlannedTablets = case_when(
    str_detect(PlannedName, " 30 ") ~ 30,
    str_detect(PlannedName, " 90 ") ~ 90,
    str_detect(PlannedName, " 180 ") ~ 180
         )) %>%
  select(FiscalYear = fiscal_year_funding,
         Country = country,
         PlannedName,
         PlannedTablets,
         PlannedQuantity,
         ArtmisName,
         ArtmisTablets,
         ArtmisQuantity) %>%
  #write_csv(here("Dataout", "PvP_ARV.csv"))
  mutate(
    PlannedTablets = case_when(
      PlannedTablets == 28 ~ 1,
      PlannedTablets == 30 ~ 1,
      PlannedTablets == 90 ~ 3,
      PlannedTablets == 180 ~ 6
    ),
    ArtmisTablets = case_when(
      ArtmisTablets == 28 ~ 1,
      ArtmisTablets == 30 ~ 1,
      ArtmisTablets == 90 ~ 3,
      ArtmisTablets == 180 ~ 6
    )
  ) %>%
  mutate(PlannedMOT = PlannedTablets*PlannedQuantity,
         ArtmisMOT = ArtmisTablets*ArtmisQuantity) %>%
  group_by(FiscalYear, Country) %>%
  summarize(PlannedMOT = sum(PlannedMOT, na.rm = T),
            ArtmisMOT = sum(ArtmisMOT, na.rm = T)) %>%
  mutate(AbsDiff = abs(PlannedMOT - ArtmisMOT)) %>%
  arrange(Country, FiscalYear) %>%
  view()
