# Silver Layer ETL

This directory contains ETL processes for the silver layer of the e-commerce lakehouse architecture.

## Overview

The silver layer is the second layer of the lakehouse architecture. It transforms and cleanses data from the bronze layer to create high-quality Delta tables. The main goals of the silver layer are:

- Clean and standardize data
- Remove duplicates
- Apply business rules and validations
- Ensure referential integrity
- Add derived columns
- Optimize storage and query performance
- Register tables in the Glue Data Catalog

## ETL Processes

The silver layer includes the following ETL processes:

- **Products ETL**: Cleanses and standardizes product data
- **Orders ETL**: Cleanses and standardizes order data
- **Order Items ETL**: Cleanses and standardizes order item data

## Data Quality

The silver layer applies comprehensive data quality checks:

- Schema validation
- Data type validation
- Business rule validation
- Referential integrity checks
- Duplicate detection and removal
- Range and constraint validation

## Partitioning and Optimization

The silver layer optimizes data as follows:

- Products: No partitioning, Z-ordered by product_id
- Orders: Partitioned by date, Z-ordered by order_id
- Order Items: Partitioned by date, Z-ordered by order_id and product_id

## Derived Columns

The silver layer adds the following derived columns:

- Products:
  - `is_active`: Whether the product is active
  - `department_name_standardized`: Standardized department name
  
- Orders:
  - `day_of_week`: The day of the week of the order
  - `hour_of_day`: The hour of the day of the order
  
- Order Items:
  - `days_since_prior_order_bucket`: Bucketed days since prior order

## Metadata

The silver layer adds the following metadata columns to all tables:

- `bronze_source_file`: The source file in the bronze layer
- `processing_timestamp`: The timestamp when the data was processed
- `layer`: The data layer (silver)

## File Processing Status

The silver layer tracks the status of files as they are processed:

- `bronze/processing/`: Files currently being processed
- `bronze/failed/`: Files that failed processing
- `bronze/archive/`: Previous versions of data

## Usage

Each ETL process can be run individually or as part of the overall ETL pipeline. For example:

```bash
# Run the products ETL
python -m etl.silver.products_etl

# Run the orders ETL
python -m etl.silver.orders_etl

# Run the order items ETL
python -m etl.silver.order_items_etl
```
