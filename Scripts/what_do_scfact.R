#### Title
# PURPOSE: What to do with SC_FACT
# AUTHOR: alerichardson | sch
# LICENSE: MIT
# DATE: 2024-05-24
# NOTES: 

#### LOCALS & SETUP ============================================================================

# Libraries
require(tidyverse)
require(gagglr)
require(here)
require(googledrive)
#si_setup()

#### LOAD DATA ============================================================================  

drive_auth()

mer <- getBigfoot::get_mer(path = here("C:/Users/arichardson/OneDrive - Credence Management Solutions LLC/Documents/Github/sch_misc/Data"))

sc_fact <- getBigfoot::get_scfact(download = T)

#### Number of Unmatched Sites ============================================================================  

mer_locs = mer %>%
  mutate(operatingunit = case_when(
    snu1 == "Ghana" ~ "Ghana",
    snu1 == "Mali" ~ "Mali",
    TRUE ~ operatingunit
  ),
  snu1 = case_when(
    snu1 == "Ghana" ~ psnu,
    snu1 == "Mali" ~ psnu,
    TRUE ~ snu1
  )) %>%
  filter(operatingunit != "West Africa Region") %>%
  group_by(orgunituid, operatingunit, snu1) %>%
  summarize()

sc_fact_locs = sc_fact %>%
  ungroup() %>%
  group_by(DatimCode) %>%
  summarize() %>%
  mutate(scfact = 1)

mer_matches = mer_locs %>%
  left_join(sc_fact_locs, by = c("orgunituid" = "DatimCode")) %>%
  ungroup() %>%
  group_by(operatingunit, snu1) %>%
  summarize(n = n(),
            matches = sum(scfact, na.rm = T)) %>%
  mutate(match_percentage = paste0(round((matches*100/n), digits = 1), "%"))

mer_matches %>%
  write_csv(here("Dataout", "mer_scfact_match_percents.csv"))

#### Number of Sites Without UIDs ============================================================================  

sc_fact_uids = sc_fact %>% mutate(uid = case_when(!is.na(DatimCode) ~ 1,
                                                  is.na(DatimCode) ~ 0)) %>%
  ungroup() %>%
  group_by(Country, SNL1) %>%
  summarise(n = n(),
            uid = sum(uid, na.rm = T)) %>%
  mutate(uid_percentage = paste0(round((uid * 100 / n), digits = 1), "%"))

sc_fact_uids %>%
  write_csv(here("Dataout", "sc_fact_uids.csv"))

#### Number of Sites Without Data ============================================================================  

sc_fact_duplicates = sc_fact %>%
  group_by(Country, Period, SNL1, SNL2, FacilityCD, Facility, DatimCode, ProductCategory,
           Product, SKU, `Pack Size`, SOH, AMI, MOS, Facility_mapped, Source) %>%
  summarize(n = n()) %>%
  filter(n > 1) %>%
  select(-n)

sc_fact_nd = sc_fact %>%
  anti_join(sc_fact_duplicates) %>%
  bind_rows(sc_fact_duplicates) %>%
  mutate(data = 1) %>%
  pivot_wider(id_cols = c("Country", 
                          "SNL1", 
                          "SNL2", 
                          "FacilityCD",
                          "Facility",
                          "DatimCode",
                          "ProductCategory",
                          "Product",
                          "SKU",
                          "Pack Size",
                          "SOH",
                          "AMI",
                          "MOS",
                          "Facility_mapped",
                          "Source"), 
              names_from = Period, 
              values_from = data,
              values_fill = 0)

bigger_than_zero = function(var){
  return(var>0)
}

sc_fact_nd %>% group_by(Country, Facility) %>% summarize(
  `2022-10` = sum(as.numeric(`2022-10`), na.rm = T),
  `2022-11` = sum(as.numeric(`2022-11`), na.rm = T),
  `2022-12` = sum(as.numeric(`2022-12`), na.rm = T),
  `2023-01` = sum(as.numeric(`2023-01`), na.rm = T),
  `2023-02` = sum(as.numeric(`2023-02`), na.rm = T),
  `2023-03` = sum(as.numeric(`2023-03`), na.rm = T),
  `2023-04` = sum(as.numeric(`2023-04`), na.rm = T),
  `2023-05` = sum(as.numeric(`2023-05`), na.rm = T),
  `2023-06` = sum(as.numeric(`2023-06`), na.rm = T),
  `2023-07` = sum(as.numeric(`2023-07`), na.rm = T),
  `2023-08` = sum(as.numeric(`2023-08`), na.rm = T),
  `2023-09` = sum(as.numeric(`2023-09`), na.rm = T),
  `2023-10` = sum(as.numeric(`2023-10`), na.rm = T),
  `2023-11` = sum(as.numeric(`2023-11`), na.rm = T),
  `2023-12` = sum(as.numeric(`2023-12`), na.rm = T),
  `2024-01` = sum(as.numeric(`2024-01`), na.rm = T)
) %>%
  mutate_if(is.numeric, bigger_than_zero) %>%
  mutate_if(is.logical, as.numeric) %>%
  summarize_if(is.numeric, sum) %>%
  write_csv(here("Dataout", "sc_fact_facilities_reporting.csv"))

sc_fact_nd_s = sc_fact_nd %>%
  mutate(data_3m = `2024-01` + `2023-12` + `2023-11`) %>%
  group_by(Country, SNL1, SNL2, Facility) %>%
  summarize(data_3m = sum(data_3m, na.rm = T)) %>%
  mutate(no_data_3m = case_when(
    data_3m == 0 ~ 1,
    TRUE ~ 0
  )) 

sc_fact_nd_s %>%
  filter(no_data_3m == 1) %>%
  mutate(no_data_3m = TRUE) %>%
  write_csv(here("Dataout", "sc_fact_nd_facilities.csv"))


sc_fact_nd_s %>%
  ungroup() %>%
  group_by(Country) %>%
  summarize(no_data_3m = sum(no_data_3m, na.rm = T),
            n = n()) %>%
  mutate(no_data_percent = paste0(round(no_data_3m*100/n, digits = 1), "%")) %>%
  write_csv(here("Dataout", "sc_fact_nd_facilities_countrysums.csv"))



#### QAT-TX CURR Exploration =======================================================

mer_sums = mer %>% 
  filter(operatingunit %in% c("Zambia", "Mozambique", "Nigeria")) %>%
  ungroup() %>%
  group_by(operatingunit, period) %>%
  summarize(`Expected MoT` = sum(value, na.rm = T)*3)

qat_sums = qat %>%
  filter(Country %in% c("ZMB", "MOZ", "NGA"),
         str_detect(Product, "Dolutegravir/Lamivudine/Tenofovir")) %>%
  mutate(multiplier = case_when(
    str_detect(Product, "30 Tablets") ~ 1,
    str_detect(Product, "90 Tablets") ~ 3,
    str_detect(Product, "180 Tablets") ~ 6,
    TRUE ~ 0
  ),
  MoT = multiplier*Amount,
  Country = case_when(
    Country == "ZMB" ~ "Zambia",
    Country == "MOZ" ~ "Mozambique",
    Country == "NGA" ~ "Nigeria"
  ),
  smp = lubridate::quarter(
    x = `Receive Date`,
    with_year = TRUE,
    fiscal_start = 10
  ),
  mer_pd = paste0("FY", substr(smp, 3, 4), "Q", substr(smp, 6, 6))
  ) %>%
  group_by(Country, mer_pd) %>%
  summarize(MoT = sum(MoT, na.rm = T))

mer_qat_sums = mer_sums %>%
  left_join(qat_sums, by = c("operatingunit" = "Country", "period" = "mer_pd"))


mer_qat_sums %>%
  mutate(abs_diffs = abs(`Expected MoT`-MoT)) %>%
  view()

mer_qat_sums %>%
  mutate(abs_diffs = abs(`Expected MoT`-MoT)) %>%
  ggplot() +
  geom_line(aes(x = period, y = abs_diffs, group = operatingunit, color = operatingunit))
