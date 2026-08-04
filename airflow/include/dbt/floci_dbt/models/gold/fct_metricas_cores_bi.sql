{{ config(
    materialized='table',
    schema='CORE'
) }}

with produtos as (
    select * from {{ ref('dim_produtos_analytics') }}
)

select
    categoria_principal,
    subcategoria,
    genero,
    estacao,
    count(produto_id) as total_produtos,
    
    -- Médias gerais de cor por agrupamento
    round(avg(media_vermelho), 2) as avg_vermelho,
    round(avg(media_verde), 2) as avg_verde,
    round(avg(media_azul), 2) as avg_azul,
    
    -- Métricas adicionais para análise de variabilidade de cores
    round(avg(desvio_vermelho), 2) as avg_desvio_vermelho,
    round(avg(desvio_verde), 2) as avg_desvio_verde,
    round(avg(desvio_azul), 2) as avg_desvio_azul,
    
    -- Proporção de produtos multicoloridos
    round(100.0 * sum(case when multicolorido = true then 1 else 0 end) / count(produto_id), 2) as pct_multicolorido,
    
    -- Brilho médio
    round(avg(brilho), 2) as avg_brilho,
    
    -- Atributos avançados de textura e cor
    round(avg(saturacao_media), 2) as avg_saturacao,
    round(avg(complexidade_visual), 2) as avg_complexidade_visual,
    round(avg(contraste_luminancia), 2) as avg_contraste_luminancia
from produtos
group by 1, 2, 3, 4