{{ config(
    materialized='table',
    schema='STAGING'
) }}

with source_data as (
    select * from {{ source('raw_fashion', 'PRODUCTS_RAW') }}
),

cleaned_data as (
    select
        id,
        upper(trim(gender)) as gender,
        upper(trim(master_category)) as master_category,
        upper(trim(sub_category)) as sub_category,
        upper(trim(article_type)) as article_type,
        upper(trim(base_colour)) as base_colour,
        upper(trim(season)) as season,
        year,
        upper(trim(usage)) as usage,
        trim(product_display_name) as product_display_name,
        
        -- Atributos de imagem passados do pipeline RAW
        mean_r,
        mean_g,
        mean_b,
        file_size_bytes,
        brightness,
        std_r,
        std_g,
        std_b,

        -- Lógica de negócio: classificação automática de estampas/multicolorido
        case 
            when (std_r + std_g + std_b) / 3 > 45 then true 
            else false 
        end as is_multicolored

    from source_data
)

select * from cleaned_data