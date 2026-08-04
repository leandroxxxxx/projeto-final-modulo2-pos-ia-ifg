{{ config(
    materialized='table',
    schema='CORE'
) }}

with base as (
    select * from {{ ref('dim_produtos_analytics') }}
)

select
    produto_id,
    genero,
    subcategoria,
    tipo_artigo,
    cor_base,
    estacao,
    ocasiao_uso,
    
    -- Normalização das cores (escala de 0 a 1 em vez de 0 a 255)
    -- Isso auxilia na convergência de algoritmos de Machine Learning
    round(media_vermelho / 255.0, 4) as media_vermelho_norm,
    round(media_verde / 255.0, 4) as media_verde_norm,
    round(media_azul / 255.0, 4) as media_azul_norm,
    
    -- Atributos avançados normalizados (escala 0-100 para saturação, 
    -- e min-max aproximado para complexidade e contraste)
    round(saturacao_media / 255.0, 4) as saturacao_media_norm,
    round(contraste_luminancia / 255.0, 4) as contraste_luminancia_norm,
    round(complexidade_visual / 255.0, 4) as complexidade_visual_norm,
    
    -- Variável Alvo (Target) - Garantir que não existam nulos
    categoria_principal as target_categoria_principal
from base
where target_categoria_principal is not null
  -- Remover categorias raras que podem prejudicar o treino
  and target_categoria_principal in (
      select categoria_principal 
      from base 
      group by categoria_principal 
      having count(*) >= 10
  )