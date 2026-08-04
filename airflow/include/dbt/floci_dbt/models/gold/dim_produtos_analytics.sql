{{ config(
    materialized='table',
    schema='CORE'
) }}

with staging_produtos as (
    select * from {{ ref('stg_products') }}
)

select
    id as produto_id,
    gender as genero,
    master_category as categoria_principal,
    sub_category as subcategoria,
    article_type as tipo_artigo,
    base_colour as cor_base,
    season as estacao,
    year as ano_lancamento,
    usage as ocasiao_uso,
    product_display_name as nome_produto,
    
    -- Características cromáticas extraídas
    mean_r as media_vermelho,
    mean_g as media_verde,
    mean_b as media_azul,
    
    -- Metadados adicionais da imagem
    std_r as desvio_vermelho,
    std_g as desvio_verde,
    std_b as desvio_azul,
    brightness as brilho,
    is_multicolored as multicolorido,
    
    -- Atributos avançados de textura e cor
    saturacao_media as saturacao_media,
    complexidade_visual as complexidade_visual,
    contraste_luminancia as contraste_luminancia

from staging_produtos
where id is not null