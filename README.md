# 📊 Media Performance Framework & Dashboard Regional 2026


https://lookerstudio.google.com/reporting/d4db3c78-9b95-4773-949d-087be2e1fb70


## 📝 Resumen del Proyecto
Este repositorio contiene la solución integral para el monitoreo y optimización de campañas de medios regionales. El desafío principal consistió en la unificación de tres fuentes de datos con granularidades divergentes para proporcionar una visión única de **Eficiencia (CPM)**, **Escala (IIEE)** e **Inversión**.

La solución no solo visualiza datos, sino que implementa una capa de **Inteligencia de Datos** en SQL para resolver problemas complejos de prorrateo y benchmarks históricos.

---

## 🛠️ Arquitectura de Datos (Stack Tecnológico)
* **Ingeniería de Datos:** Google BigQuery (SQL Standard).
* **Modelado:** Implementación de tablas maestras (One Big Table) con lógica de particionamiento.
* **Visualización:** Looker Studio (utilizando conectores nativos de BigQuery).
* **Documentación:** Markdown para documentación técnica y funcional.

---

## 🧠 Solución a Preguntas Analíticas (Criterios de Evaluación)

### 1. ETL & Arquitectura: Reto de Granularidad y Prorrateo
**Problema:** Los Targets se definen a nivel de Campaña (agrupado), mientras que la ejecución real es a nivel de Medio/Formato (detallado). Un JOIN directo causaría una duplicación masiva de los valores de target.

**Solución Técnica:** Se implementó un **Prorrateo Proporcional basado en el Peso de Ejecución (Weighting Allocation)**.
* **Lógica:** Mediante *Window Functions* (`OVER PARTITION BY`), se calculó qué porcentaje de la inversión real de la semana representó cada formato.
* **Fórmula:** `Target_Prorrateado = Target_Semanal_Campaña * (Inversión_Real_Formato / Inversión_Real_Total_Campaña)`.
* **Resultado:** Esto garantiza la integridad referencial. Si el usuario filtra por un formato, ve su parte proporcional del target; si ve la campaña completa, la suma es exacta al 100% del plan original.


### 2. Lógica de Negocio: Benchmark Histórico Dinámico
**Problema:** Comparar el rendimiento actual contra el promedio de las últimas 3 campañas finalizadas de la misma categoría.

**Solución Técnica:** Implementación de un modelo de **Ranking Temporal Particionado**.
* Se utilizó `ROW_NUMBER() OVER(PARTITION BY Pais, BU, Network ORDER BY Fecha_Fin DESC)` filtrando por estatus 'Finalizada'.
* **Precisión Matemática:** Se evitó el "promedio de promedios". El benchmark se calcula sumando la inversión total de esas 3 campañas y dividiéndola por la suma total de sus impresiones.
* **Dinamismo:** Al estar en la capa de SQL, el benchmark se actualiza automáticamente a medida que nuevas campañas cambian su estatus a "Finalizada".

### 3. Data Storytelling: Optimización de Inversión
**Análisis del Scatter Chart (Eficiencia vs. Escala):**
* **Insight:** Se identificaron redes y formatos en el **Cuadrante Superior Izquierdo** (Alto CPM y Bajo Volumen de Impresiones).
* **Acción Recomendada:** Pausa o renegociación inmediata. Estos puntos representan ineficiencias donde el costo por impacto es demasiado elevado para el volumen marginal que aportan.
* **Optimización:** El presupuesto liberado debe redistribuirse hacia los medios en el **Cuadrante Inferior Derecho**, que presentan economías de escala (bajo CPM con alto volumen), maximizando así el alcance total del presupuesto regional.



# 📊 Documentación Técnica – Pipeline de Datos y Dashboard de Performance

## 1. 🧩 Overview
Se diseñó e implementó un pipeline de datos en GCP (BigQuery) para consolidar, transformar y modelar información de campañas de medios, con el objetivo de habilitar un dashboard ejecutivo en Looker Studio.

---

## 2. 🏗️ Arquitectura de Datos

### 🥉 Bronze
- Fuente: Google Sheets conectados a BigQuery
- Datos crudos sin transformación

### 🥈 Silver
- Limpieza y normalización
- Tipado seguro con SAFE_CAST
- Manejo de nulos con COALESCE
- Creación de métricas derivadas

### 🥇 Gold
- Tabla final para BI: ds_zone_gold.tb_dsh_report_meli
- Integración de:
  - Data real
  - Targets
  - Optimizador
  - Benchmark

---

## 3. ⚙️ Lógica Clave

### Prorrateo de Targets
Distribución proporcional usando pesos:

    peso_inv = investment_usd / total_investment_campaign

    inv_target = costo_target * peso_inv

---

### Cálculo correcto de CPM

    CPM = SUM(inversión) / SUM(impresiones) * 1000

---

### Benchmark dinámico
- Top 3 campañas más recientes
- Cálculo agregado sin promedios incorrectos

---

### Status de campaña

    CASE 
        WHEN max_semana >= max_semana_global - 1 THEN 'Activa'
        ELSE 'Finalizada'
    END

---

### Optimizador
- pre_opti
- post_opti
- delta_opti

---

## 4. 📊 Dashboard

### KPIs
- Inversión
- Impresiones
- CPM

### Semáforo

    🟢 Eficiente
    🟠 En rango
    🔴 Ineficiente

### Scatter Plot
- X: Impresiones
- Y: CPM
- Tamaño: Inversión

### Filtros
- País
- BU
- Campaña
- Network
- Status

---

## 5. 🤖 Agente
- Conectado a BigQuery
- Permite consultas dinámicas

---

## 6. 🚀 Mejoras
- Particionamiento
- Clustering
- Orquestación
- Data Quality
- dbt

---

## 7. ✅ Conclusión
Pipeline robusto, escalable y alineado a negocio.
