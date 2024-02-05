{{ config(
    unique_key="recordid",
    
      cluster_by= ["county" ,"metrostatus"],
    
    materialized="view") }}

with vaccinedata as 
(
        select *
        from {{ source("staging", "NY") }}
        where fips  != "UNK"
)

select

    -- date
    date as date,
    {{dbt_utils.surrogate_key(['date', 'fips','recip_state'])}} as recordid,

    -- Demographics
    cast(fips as integer) as fips,
    cast(recip_county as string) as county,
    cast(recip_state as string) as state,
    cast(metro_status as string) as metrostatus,

    -- First Dosage
    cast(administered_dose1_recip as integer) as onedosecomplete,
    cast(administered_dose1_pop_pct as numeric) as onedosecompletep,
    cast(administered_dose1_recip_18plus as integer) as onedosecomplete18p,
    
    cast(administered_dose1_recip_65plus as integer) as onedosecomplete65p,
    
    -- Booster Dosage
    cast(booster_doses as integer) as boosterdone,

    cast(series_complete_pop_pct as numeric) as completedosagepercentage,
    cast(series_complete_pop_pct_svi as numeric) as completedosagewboosterpercentage,

    cast(census2019 as integer) as population2019,
    cast(census2019_18pluspop as integer) as population18p2019,
    cast(census2019_65pluspop as integer) as population65p2019

from vaccinedata

