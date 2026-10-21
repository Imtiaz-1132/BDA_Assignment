# EpiScope: Big Data Platform for Disease Surveillance and Resource Allocation in Bangladesh

## 1. Problem Statement

EpiScope aims to develop a reproducible big-data platform for disease surveillance, outbreak detection, short-term forecasting, and healthcare resource allocation across Bangladesh's 64 districts. The platform will combine public geographic boundaries, population and weather data with synthetic clinical encounters, facility information, and daily resource records for 2021–2025. It will address reporting delays, duplicate records, missing references, data quality, privacy, and uneven resource demand.

The system will use Apache Spark to process heterogeneous data through Bronze, Silver, and Gold layers. Its analytical outputs will support early warnings, disease forecasts, resource allocation, and an interactive D3.js dashboard. Synthetic data and explicit ground-truth labels will enable reproducible evaluation without exposing real patient records.

## 2. Stakeholders and Conflicting Requirements

Key stakeholders include Ministry of Health policymakers, surveillance epidemiologists, district health officers, hospital managers, vector-control units, patients, the public, and privacy regulators. Their needs create five conflicts:

- **C1—Sensitivity vs. specificity:** Lower alert thresholds may detect outbreaks earlier but increase false alarms. We will compare two detection methods and evaluate sensitivity, false alerts per district-year, and median detection delay to select a defensible threshold.
- **C2—Granularity vs. privacy:** Detailed geographic results improve local action but may expose small patient counts. We will suppress counts from 1–4, prevent recovery by subtraction, and publish only aggregated Gold-layer outputs.
- **C3—Efficiency vs. equity:** Maximizing the use of a fixed budget may disadvantage lower-risk districts. We will compare optimized allocations with a population-proportional baseline and evaluate efficiency–equity trade-offs across at least five budget levels.
- **C4—Freshness vs. cost and complexity:** Continuous processing can improve alert timeliness but increases operational cost. We plan daily incremental batch processing as the primary path, with late-arriving records updating affected weeks; optional streaming will be considered only if time and resources permit.
- **C5—Accuracy vs. interpretability:** Complex models may improve forecasts but reduce trust. We will compare forecast performance against a seasonal-naïve baseline and prefer transparent, explainable methods when performance is competitive.

## 3. Planned Architecture and Methods

Public boundaries, population, and historical weather will be combined with deterministic synthetic encounter, facility, and resource data. Apache Spark (PySpark) will handle large transformations. The lakehouse will use Bronze for source-preserving ingestion, Silver for validated and cleaned data, and Gold for aggregated analytical outputs, stored in partitioned Parquet files. Automated quality checks, quarantine reason codes, batch identifiers, and lineage will support reliability and reproducibility.

The initial disease focus will be **dengue (ICD-10 A90/A91)** and **diarrhoeal disease (A00/A09)**. We will use established outbreak-detection methods such as EARS and CUSUM, compare their results against synthetic ground truth, and guard against false alerts caused by reporting artefacts. Weekly forecasts for one to four weeks ahead will incorporate lagged weather variables and be evaluated using rolling-origin backtesting, MAE, and 80% prediction-interval coverage. Forecasts will inform estimates of beds, IV saline, and test-kit demand. A linear or integer optimization model will allocate a fixed budget, with comparison against a proportional baseline.

The dashboard will use D3.js v7 to provide a district incidence map, observed-versus-forecast time series, seasonal heatmap, resource-allocation view, and ranked early-warning table. Views will be linked through selections and filters. Only aggregated Gold outputs will reach the browser, with uncertainty, data freshness, and suppressed cells clearly indicated.

## 4. Team Roles

1. **Md Imtiaz Uddin — Data Engineer:** Synthetic data generation, ingestion, Bronze/Silver/Gold pipeline, quality rules, lineage, and incremental processing.
2. **Sabiha Taslim Jannath — Analytics Engineer:** Outbreak detection, forecasting, backtesting, demand estimation, and resource optimization.
3. **Pritam Nath — Visualisation Engineer:** Gold-to-serving outputs, privacy-aware aggregation, D3.js dashboard, and usability checks.
4. **Nusrat Jahan Ananna — Testing and Integration Engineer:** Automated tests, component integration, reproducibility and scalability checks, and requirement verification.

All members will contribute code and review the complete system, not only their assigned components.

## 5. Risk Register

| Risk | Mitigation |
|---|---|
| Synthetic data does not represent realistic outbreaks | Use deterministic seeds, seasonal patterns, injected outbreaks, and separate ground-truth labels. |
| Reporting delays or backfills create false alerts | Model reporting delays and test detection against an injected reporting artefact. |
| Invalid, duplicate, or orphan records reduce reliability | Apply automated quality rules, quarantine invalid rows, and test incremental idempotency. |
| Small counts reveal sensitive information | Enforce suppression of counts 1–4 and test that totals cannot reveal suppressed values. |
| Forecasts or allocations perform poorly | Use rolling-origin backtests, uncertainty intervals, and a population-proportional baseline. |
| Processing exceeds available time or memory | Begin with smaller scale factors, profile Spark jobs, and document optimization results. |
| Integration delays prevent a reproducible demo | Use pinned dependencies, automated tests, regular integration, and a documented run command. |

The team will prioritize a reproducible end-to-end pipeline, measurable evaluation, privacy protection, and a transparent dashboard within the assignment timeline.