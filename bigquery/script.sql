CREATE OR REPLACE TABLE `project-d0c76d9f-d27a-4298-8e3.ds_zone_silver.tb_optimization_t`
AS
SELECT
  -- Tiempo
  fecha,

  -- Dimensiones
  COALESCE(TRIM(LOWER(sit_site_id)), 'unknown') AS sit_site_id,
  COALESCE(TRIM(LOWER(bu)), 'unknown') AS bu,
  COALESCE(TRIM(campaign_name), 'unknown') AS campaign_name,
  COALESCE(TRIM(campaign_name_2), 'unknown') AS campaign_name_2,
  COALESCE(TRIM(group_number), 'unknown') AS group_number,
  COALESCE(TRIM(LOWER(platform_name)), 'unknown') AS platform_name,
  COALESCE(TRIM(LOWER(network_name)), 'unknown') AS network,
  COALESCE(TRIM(LOWER(channel_format)), 'unknown') AS channel_format,

  -- Normalización success → boolean lógico
  CASE 
    WHEN LOWER(TRIM(success)) IN ('true','1','yes','ok','success') THEN 1
    ELSE 0
  END AS success_flag,

  -- Métricas
  COALESCE(SAFE_CAST(previous_value AS FLOAT64), 0) AS previous_value,
  COALESCE(SAFE_CAST(new_value AS FLOAT64), 0) AS new_value,

  -- Delta (clave para análisis)
  COALESCE(SAFE_CAST(new_value AS FLOAT64), 0) 
  - COALESCE(SAFE_CAST(previous_value AS FLOAT64), 0) AS delta_value,

  -- Logs / debugging
  COALESCE(TRIM(descripcion), '') AS descripcion,
  COALESCE(TRIM(extra_message), '') AS extra_message,
  COALESCE(TRIM(warning_message), '') AS warning_message,

  -- Auditoría
  CURRENT_TIMESTAMP() AS ingestion_ts

FROM `project-d0c76d9f-d27a-4298-8e3.ds_zone_bronze.tb_optimization_t`

-- Calidad mínima
WHERE campaign_name IS NOT NULL
  AND sit_site_id IS NOT NULL
  AND TRIM(campaign_name) != ''
  AND TRIM(sit_site_id) != ''
;

-------

CREATE OR REPLACE TABLE `ds_zone_gold.tb_dsh_report_meli` AS 
WITH 
-- 1. maxSemanaGlobal: Obtener la semana máxima de toda la tabla para el status dinámico
global_max AS (
  SELECT 
    MAX(SAFE_CAST(semana AS INT64)) AS max_semana_global 
  FROM `ds_zone_silver.tb_impressions_week`
),

-- 2. base_real_pesos: LA MAGIA DEL PRORRATEO PROPORCIONAL (Solución a Pregunta 1)
-- Calculamos cuánto pesa (en %) cada fila dentro del total de la campaña en esa misma semana.
base_real_pesos AS (
  SELECT 
    *,
    -- Calculamos los totales reales de la campaña para ESA semana específica
    SUM(investment_usd) OVER(PARTITION BY semana, UPPER(TRIM(pais)), UPPER(TRIM(campaign_name))) AS total_inv_camp_sem,
    SUM(iiee_weekly) OVER(PARTITION BY semana, UPPER(TRIM(pais)), UPPER(TRIM(campaign_name))) AS total_iiee_camp_sem,
    
    -- Calculamos el peso (Porcentaje de 0 a 1) que tiene esta fila sobre el total
    SAFE_DIVIDE(
      investment_usd, 
      SUM(investment_usd) OVER(PARTITION BY semana, UPPER(TRIM(pais)), UPPER(TRIM(campaign_name)))
    ) AS peso_inv,
    
    SAFE_DIVIDE(
      iiee_weekly, 
      SUM(iiee_weekly) OVER(PARTITION BY semana, UPPER(TRIM(pais)), UPPER(TRIM(campaign_name)))
    ) AS peso_iiee
  FROM `ds_zone_silver.tb_impressions_week`
),

-- 3. campStats: Resumen general de campañas para saber su fecha máxima e históricos
camp_stats AS (
  SELECT 
    UPPER(TRIM(pais)) AS pais, 
    UPPER(TRIM(brand)) AS brand, 
    UPPER(TRIM(level_1)) AS level1,
    UPPER(TRIM(network_name)) AS network, 
    UPPER(TRIM(campaign_name)) AS campana,
    MAX(SAFE_CAST(semana AS INT64)) AS max_semana,
    SUM(investment_usd) AS total_inv,
    SUM(iiee_weekly) AS total_iiee
  FROM `ds_zone_silver.tb_impressions_week`
  GROUP BY 1, 2, 3, 4, 5
),

-- 4. Benchmarks: Top 3 campañas más recientes por red (Solución a Pregunta 2)
bench_ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY pais, brand, level1, network ORDER BY max_semana DESC) AS rn
  FROM camp_stats
),
benchmarks AS (
  SELECT 
    pais, brand, level1, network,
    -- Evitamos promedios de promedios: Suma total Inv / Suma total IIEE
    SAFE_DIVIDE(SUM(total_inv), SUM(total_iiee)) * 1000 AS cpm_bench
  FROM bench_ranked
  WHERE rn <= 3 
  GROUP BY 1, 2, 3, 4
),

-- 5. mapOpti: Agrupación de la data del optimizador a nivel Formato
opti_agg AS (
  SELECT 
    UPPER(TRIM(sit_site_id)) AS pais,
    UPPER(TRIM(bu)) AS brand, 
    UPPER(TRIM(campaign_name_2)) AS campana, 
    -- UPPER(TRIM(network)) AS network,
    UPPER(TRIM(channel_format)) AS formato,
    SUM(previous_value) AS pre_opti,
    SUM(new_value) AS post_opti,
    SUM(new_value) - SUM(previous_value) as delta_opti
  FROM `ds_zone_silver.tb_optimization_t`
  GROUP BY 1, 2, 3, 4
),

-- 6. mapTargets: Extraemos el target "Semanal" base de la campaña
targets_agg AS (
 SELECT 
    UPPER(TRIM(site)) AS pais,
    UPPER(TRIM(bu)) AS brand,
    UPPER(TRIM(campana)) AS campana,
    -- Tomamos el costo semanal de la tabla target
    SUM(costo_semanal_usd_sin_directo) AS costo_target_semanal,
    -- Prorrateamos las impresiones totales entre la duración en semanas
    SAFE_DIVIDE(SUM(iiee_total_proy_curva_2025), MAX(duracion_semanas)) AS iiee_target_semanal
  FROM `ds_zone_silver.tb_targets_mk`
  GROUP BY 1, 2, 3
),

-- 7. BD FINAL: El gran cruce
bd_final AS (
  SELECT 
    r.semana,
    UPPER(TRIM(r.brand)) AS brand,
    UPPER(TRIM(r.pais)) AS pais,
    UPPER(TRIM(r.level_1)) AS level1,
    UPPER(TRIM(r.campaign_name)) AS campana,
    UPPER(TRIM(r.network_name)) AS network,
    UPPER(TRIM(r.channel_format)) AS formato,
    
    -- Estatus Dinámico
    CASE 
      WHEN SAFE_CAST(cs.max_semana AS INT64) >= gm.max_semana_global - 1 THEN 'Activa'
      ELSE 'Finalizada'
    END AS status,

    -- Métricas Reales
    r.investment_usd AS inv_real,
    r.iiee_weekly AS iiee_real,
    SAFE_DIVIDE(r.investment_usd, r.iiee_weekly) * 1000 AS cpm_real,

    -- Métricas Target Prorrateadas (Proporcionalmente al Peso)
    -- Multiplicamos el target de toda la semana por el % que gastó este formato específico
    t.iiee_target_semanal * r.peso_iiee AS iiee_target,
    t.costo_target_semanal * r.peso_inv AS inv_target,
    
    -- El CPM Target se calcula dividiendo sus nuevas partes proporcionales
    SAFE_DIVIDE(
        (t.costo_target_semanal * r.peso_inv), 
        (t.iiee_target_semanal * r.peso_iiee)
    ) * 1000 AS cpm_target,

    -- Benchmark y Optimizador
    COALESCE(b.cpm_bench, 0) AS cpm_bench,
    COALESCE(o.pre_opti, 0) AS pre_opti,
    COALESCE(o.post_opti, 0) AS post_opti,
    COALESCE(o.delta_opti, 0) AS delta_opti

  FROM base_real_pesos r  -- Usamos nuestra nueva tabla base con los porcentajes
  CROSS JOIN global_max gm

  -- Join Status
  LEFT JOIN camp_stats cs
    ON UPPER(TRIM(r.pais)) = cs.pais AND UPPER(TRIM(r.brand)) = cs.brand 
    AND UPPER(TRIM(r.level_1)) = cs.level1 AND UPPER(TRIM(r.network_name)) = cs.network 
    AND UPPER(TRIM(r.campaign_name)) = cs.campana

  -- Join Benchmark
  LEFT JOIN benchmarks b
    ON UPPER(TRIM(r.pais)) = b.pais AND UPPER(TRIM(r.brand)) = b.brand 
    AND UPPER(TRIM(r.level_1)) = b.level1 AND UPPER(TRIM(r.network_name)) = b.network

  -- Join Targets (Nota: Cruzamos directo sin necesidad de alloc_counts porque ya tenemos el peso en "r")
  LEFT JOIN targets_agg t
    ON UPPER(TRIM(r.pais)) = t.pais AND UPPER(TRIM(r.level_1)) = t.brand 
    AND UPPER(TRIM(r.campaign_name)) = t.campana

  -- Join Optimizador
  LEFT JOIN opti_agg o
    ON UPPER(TRIM(r.pais)) = o.pais AND 
    UPPER(TRIM(r.level_1)) = o.brand 
    AND UPPER(TRIM(r.campaign_name)) = o.campana 
    -- AND UPPER(TRIM(r.network_name)) = o.network 
    AND UPPER(TRIM(r.channel_format)) = o.formato
)

SELECT 
* 
FROM bd_final
WHERE 
1 = 1
-- AND cpm_target is not null AND campana = 'CAMP_013'
;