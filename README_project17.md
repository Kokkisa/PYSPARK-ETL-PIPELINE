# PySpark ETL Pipeline — Orders Data Processing

## Overview
Complete ETL (Extract-Transform-Load) pipeline built with PySpark. Extracts from multiple sources (CSV orders, JSON products, CSV customers), transforms with cleaning, validation, enrichment, and dimension joins, and loads into partitioned Parquet following star schema pattern. Includes data quality checks, pipeline logging, and downstream validation.

**Built by:** Nithin Kumar Kokkisa — Senior Demand Planner with 12+ years at HPCL managing 180,000 MTPA facility.

---

## Business Problem
Raw operational data arrives from multiple systems in different formats with quality issues (missing values, duplicates, invalid entries). An automated pipeline is needed to clean, standardize, enrich, and organize this data for analytics and reporting. This ETL pipeline transforms messy multi-source data into a clean, query-optimized star schema.

## Pipeline Structure

```
EXTRACT                    TRANSFORM                         LOAD
--------                   ---------                         ----
raw_orders.csv    --->     Quality checks                    fact_orders/ (Parquet)
(500K rows, messy)         Remove duplicates                   partitioned by year/month
                           Filter invalid records
raw_products.json --->     Type casting                      dim_products/ (Parquet)
(500 products)             Business logic columns
                           Join dimensions                   dim_customers/ (Parquet)
raw_customers.csv --->     Profit calculations
(20K customers)            Post-transform validation          agg_daily_summary/
                                                             agg_category_summary/
                                                             pipeline_log.json
```

## Data Quality Handling

| Issue | Detection | Resolution |
|-------|-----------|------------|
| Duplicate orders | dropDuplicates(['order_id']) | Keep first occurrence |
| Missing customer_id | filter(isNotNull()) | Remove row |
| Negative prices | filter(unit_price > 0) | Remove row |
| Missing unit_price | Null count check | Remove row |
| Type mismatches | Schema enforcement | Cast to correct type |

## Output Schema (Star Schema)

- **Fact Table**: fact_orders — partitioned by order_year, order_month
- **Dimension Tables**: dim_products, dim_customers
- **Aggregated Tables**: daily summary, category summary
- **Metadata**: pipeline_log.json with row counts, quality metrics

## Run on Google Colab
```python
!pip install pyspark -q
# Then paste the project code cell by cell
```

## Tools & Technologies
- **PySpark** (Spark DataFrame API + Spark SQL)
- **Apache Parquet** (partitioned columnar storage)
- **Star Schema** (fact + dimension table design)

---

## About
Part of a **30-project data analytics portfolio**. See [GitHub profile](https://github.com/Kokkisa) for the full portfolio.
