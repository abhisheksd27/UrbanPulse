-- ==============================================================================
-- Model: mart_revenue
-- Layer: Marts
-- Materialization: Table
-- Schema: marts
--
-- WHAT THIS MODEL DOES:
-- Monthly fleet revenue analysis segmented by payment method and taxi company.
-- Computes tip percentages, unit fares, and proportional market share.
-- ==============================================================================

{{
    config(
        materialized='table',
        sort=['report_month', 'total_revenue']
    )
}}

with monthly_segments as (

    select
        date_trunc('month', ft.pickup_at)                      as report_month,
        ft.payment_type,
        ft.company,
        count(ft.trip_key)                                     as total_trips,
        sum(ft.fare_amount)                                    as total_base_fare,
        sum(ft.tip_amount)                                     as total_tips,
        sum(ft.total_amount)                                   as total_revenue,
        avg(ft.fare_amount)                                    as avg_fare,
        avg(ft.tip_amount)                                     as avg_tip

    from {{ ref('fact_trips') }} ft
    group by 1, 2, 3

),

with_shares as (

    select
        report_month,
        payment_type,
        company,
        total_trips,
        round(total_base_fare, 2)                              as total_base_fare,
        round(total_tips, 2)                                   as total_tips,
        round(total_revenue, 2)                                as total_revenue,
        round(avg_fare, 2)                                     as avg_fare,
        round(avg_tip, 2)                                      as avg_tip,

        {{ safe_divide('(total_tips * 100.0)', 'total_base_fare', 2, '0.0') }} as avg_tip_percentage,

        round(
            total_revenue / nullif(sum(total_revenue) over (partition by report_month), 0) * 100.0,
            2
        )                                                      as revenue_share_pct

    from monthly_segments

)

select * from with_shares
order by report_month desc, total_revenue desc
