{{ config(
    unique_key="recordid",
    
   
    materialized="table") }}

with 
ny_data as (
    select * from {{ ref('stg_NY2021_vaccine') }}
    
),

nc_data as (
    select * from {{ ref('stg_NC2021_vaccine') }}
    
),

data_unioned as (
    select * from ny_data 
    UNION ALL
    select * from nc_data
)

select * from data_unioned order by date

