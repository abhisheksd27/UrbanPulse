-- ==============================================================================
-- Model: dim_date
-- Layer: Core
-- Materialization: Table (diststyle all)
-- Schema: core
--
-- WHAT THIS MODEL DOES:
-- Generates an exhaustive calendar date dimension from 2022-01-01 through 2025-12-31.
-- Provides calendar attributes, day-of-week, quarter, weekend, and US holiday flags.
-- ==============================================================================

{{
    config(
        materialized='table',
        dist='all',
        sort=['full_date']
    )
}}

with recursive date_series(curr_date) as (

    select cast('{{ var("start_date", "2022-01-01") }}' as date)

    union all

    select dateadd(day, 1, curr_date)
    from date_series
    where curr_date < cast('{{ var("end_date", "2025-12-31") }}' as date)

),

date_dimension as (

    select
        -- Surrogate integer key: YYYYMMDD format (e.g. 20240115)
        cast(to_char(curr_date, 'YYYYMMDD') as integer)       as date_key,
        curr_date                                              as full_date,
        extract(year from curr_date)                           as year,
        extract(quarter from curr_date)                        as quarter,
        extract(month from curr_date)                          as month,
        to_char(curr_date, 'Month')                            as month_name,
        extract(week from curr_date)                           as week_of_year,
        extract(day from curr_date)                            as day_of_month,
        extract(dow from curr_date)                            as day_of_week,
        to_char(curr_date, 'Day')                              as day_name,

        -- Weekend Flag (0 = Sunday, 6 = Saturday in Redshift)
        case
            when extract(dow from curr_date) in (0, 6) then true
            else false
        end                                                    as is_weekend,

        -- Major US Federal Holidays (Static definitions for SF municipal context)
        case
            when extract(month from curr_date) = 1 and extract(day from curr_date) = 1 then true   -- New Year's Day
            when extract(month from curr_date) = 7 and extract(day from curr_date) = 4 then true   -- Independence Day
            when extract(month from curr_date) = 11 and extract(day from curr_date) = 11 then true -- Veterans Day
            when extract(month from curr_date) = 12 and extract(day from curr_date) = 25 then true -- Christmas Day
            else false
        end                                                    as is_holiday,

        case
            when extract(month from curr_date) = 1 and extract(day from curr_date) = 1 then 'New Year\'s Day'
            when extract(month from curr_date) = 7 and extract(day from curr_date) = 4 then 'Independence Day'
            when extract(month from curr_date) = 11 and extract(day from curr_date) = 11 then 'Veterans Day'
            when extract(month from curr_date) = 12 and extract(day from curr_date) = 25 then 'Christmas Day'
            else null
        end                                                    as holiday_name

    from date_series

)

select * from date_dimension
