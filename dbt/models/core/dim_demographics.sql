{{ config(materialized="table")}}

WITH raw_data 
AS
(select fips, metrostatus
from {{ ref('fact_vaccine')}}

)

select fips,
    metrostatus,
    {{ case_when(metrostatus == 'Metro', 1, 0) }} AS  is_metro,
FROM raw_data
