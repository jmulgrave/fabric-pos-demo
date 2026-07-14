-- Auto Generated (Do not modify) 48CE257AC84D738ECBA4E97D797788CE458AC621CCBEA29B580A42B7F01C01F9
create view [jaffle_shop].[stg_payments] as with source as (
    select * from [dbt_warehouse].[jaffle_shop].[raw_payments]

),

renamed as (

    select
        id as payment_id,
        order_id,
        payment_method,

        -- `amount` is currently stored in cents, so we convert it to dollars
        amount / 100 as amount

    from source

)

select * from renamed;