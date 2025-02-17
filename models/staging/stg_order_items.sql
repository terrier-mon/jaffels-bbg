with

source as (

    select * from {{ source('ecom', 'raw_items') }}

),

renamed as (

    select

        ----------  ids
        id as order_item_id,
        orderid as order_id,
        sku as product_id

    from source

)

select * from renamed
