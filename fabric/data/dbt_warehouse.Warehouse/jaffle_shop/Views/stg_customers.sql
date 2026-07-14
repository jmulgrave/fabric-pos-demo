-- Auto Generated (Do not modify) B46101CFA1710AE0C02C7B201108E3C7D3FB84622FA04D56CA39A6E5602DD0EC
create view [jaffle_shop].[stg_customers] as with source as (
    select * from [dbt_warehouse].[jaffle_shop].[raw_customers]

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from renamed;