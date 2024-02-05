{{ config(materialized="view") }}

with raw_data as (

    select date,
    
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    EXTRACT(DAY FROM date) AS day
    
    from {{ ref('fact_vaccine')}}

)

select * from raw_data