# Gold Layer ETL

This directory contains ETL processes for the gold layer of the e-commerce lakehouse architecture.

## Overview

The gold layer is the third and final layer of the lakehouse architecture. It aggregates and transforms data from the silver layer to create business-ready Delta tables. The main goals of the gold layer are:

- Aggregate data for specific business domains
- Create denormalized, query-optimized tables
- Calculate business metrics and KPIs
- Support business intelligence and reporting
- Register tables in the Glue Data Catalog

## ETL Processes

The gold layer includes the following ETL processes:

- **Daily Sales ETL**: Aggregates order data by date
- **Product Performance ETL**: Analyzes product performance metrics
- **Department Analytics ETL**: Aggregates sales data by department
- **Customer Insights ETL**: Analyzes customer behavior

## Data Quality

The gold layer applies business-focused data quality checks:

- Aggregation validation
- Metric consistency checks
- Historical trend validation
- Cross-metric validation

## Partitioning and Optimization

The gold layer optimizes data as follows:

- Daily Sales: Partitioned by year and month
- Product Performance: Partitioned by department
- Department Analytics: Partitioned by department
- Customer Insights: Partitioned by customer segment

## Business Metrics

The gold layer calculates the following business metrics:

- Daily Sales:
  - `total_sales`: Total sales amount
  - `order_count`: Number of orders
  - `avg_order_value`: Average order value
  - `items_per_order`: Average items per order
  
- Product Performance:
  - `total_quantity`: Total quantity sold
  - `total_sales`: Total sales amount
  - `reorder_rate`: Rate at which products are reordered
  - `avg_days_between_orders`: Average days between orders
  
- Department Analytics:
  - `total_sales`: Total sales amount
  - `product_count`: Number of products
  - `customer_count`: Number of customers
  - `avg_order_value`: Average order value
  
- Customer Insights:
  - `total_spend`: Total customer spend
  - `order_count`: Number of orders
  - `avg_order_value`: Average order value
  - `days_since_last_order`: Days since last order
  - `customer_segment`: Customer segment based on RFM analysis

## Metadata

The gold layer adds the following metadata columns to all tables:

- `silver_source_tables`: The source tables in the silver layer
- `processing_timestamp`: The timestamp when the data was processed
- `layer`: The data layer (gold)

## File Processing Status

The gold layer tracks the status of files as they are processed:

- `silver/processing/`: Files currently being processed
- `silver/failed/`: Files that failed processing
- `silver/archive/`: Previous versions of data

## Usage

Each ETL process can be run individually or as part of the overall ETL pipeline. For example:

```bash
# Run the daily sales ETL
python -m etl.gold.daily_sales_etl

# Run the product performance ETL
python -m etl.gold.product_performance_etl

# Run the department analytics ETL
python -m etl.gold.department_analytics_etl

# Run the customer insights ETL
python -m etl.gold.customer_insights_etl
```
