# E-Commerce Lakehouse Architecture

## Overall System Architecture

The e-commerce lakehouse architecture implements a multi-layer data processing pipeline that transforms raw data into business-ready insights through progressive refinement stages.

```mermaid
flowchart TD
    subgraph "Data Sources"
        A1[Products Data]
        A2[Orders Data]
        A3[Order Items Data]
    end

    subgraph "Raw Zone (S3)"
        B1[Raw Products]
        B2[Raw Orders]
        B3[Raw Order Items]
    end

    subgraph "Bronze Layer (Delta Lake)"
        C1[Bronze Products]
        C2[Bronze Orders]
        C3[Bronze Order Items]
    end

    subgraph "Silver Layer (Delta Lake)"
        D1[Silver Products]
        D2[Silver Orders]
        D3[Silver Order Items]
    end

    subgraph "Gold Layer (Delta Lake)"
        E1[Daily Sales]
        E2[Product Performance]
        E3[Department Analytics]
        E4[Customer Insights]
    end

    subgraph "Query Layer"
        F1[Athena]
        F2[BI Tools]
    end

    subgraph "Orchestration"
        G1[AWS Step Functions]
    end

    subgraph "CI/CD"
        H1[GitHub Actions]
    end

    A1 --> B1
    A2 --> B2
    A3 --> B3

    B1 --> C1
    B2 --> C2
    B3 --> C3

    C1 --> D1
    C2 --> D2
    C3 --> D3

    D1 --> E1
    D1 --> E2
    D1 --> E3
    D2 --> E1
    D2 --> E4
    D3 --> E2
    D3 --> E3
    D3 --> E4

    E1 --> F1
    E2 --> F1
    E3 --> F1
    E4 --> F1
    F1 --> F2

    G1 --> B1
    G1 --> B2
    G1 --> B3
    G1 --> C1
    G1 --> C2
    G1 --> C3
    G1 --> D1
    G1 --> D2
    G1 --> D3
    G1 --> E1
    G1 --> E2
    G1 --> E3
    G1 --> E4

    H1 --> G1
```

## Data Flow Diagram

The data flow diagram illustrates how data moves through the system, from ingestion to consumption.

```mermaid
flowchart LR
    subgraph "Data Ingestion"
        A1[S3 Event Notification]
        A2[Lambda Trigger]
    end

    subgraph "Data Processing"
        B1[Bronze ETL Jobs]
        B2[Silver ETL Jobs]
        B3[Gold ETL Jobs]
    end

    subgraph "Data Storage"
        C1[Raw Zone]
        C2[Bronze Zone]
        C3[Silver Zone]
        C4[Gold Zone]
    end

    subgraph "Metadata Management"
        D1[Glue Data Catalog]
        D2[Table Definitions]
        D3[Partition Management]
    end

    subgraph "Data Consumption"
        E1[Athena Queries]
        E2[BI Dashboards]
        E3[Ad-hoc Analysis]
    end

    A1 --> A2
    A2 --> B1
    B1 --> B2
    B2 --> B3

    C1 --> B1
    B1 --> C2
    B2 --> C3
    B3 --> C4

    B1 --> D1
    B2 --> D1
    B3 --> D1
    D1 --> D2
    D1 --> D3

    C2 --> E1
    C3 --> E1
    C4 --> E1
    E1 --> E2
    E1 --> E3

    D2 --> E1
    D3 --> E1
```

## AWS Step Functions Workflow

The Step Functions workflow orchestrates the entire ETL process, handling errors and ensuring data quality at each stage.

```mermaid
stateDiagram-v2
    [*] --> CheckForNewData
    
    CheckForNewData --> ValidateRawData: Data Available
    CheckForNewData --> WaitForNextRun: No Data
    
    ValidateRawData --> BronzeLayerProcessing: Valid
    ValidateRawData --> SendErrorNotification: Invalid
    
    BronzeLayerProcessing --> ValidateBronzeData
    
    ValidateBronzeData --> SilverLayerProcessing: Valid
    ValidateBronzeData --> RollbackAndNotify: Invalid
    
    SilverLayerProcessing --> ValidateSilverData
    
    ValidateSilverData --> GoldLayerProcessing: Valid
    ValidateSilverData --> RollbackAndNotify: Invalid
    
    GoldLayerProcessing --> ValidateGoldData
    
    ValidateGoldData --> UpdateGlueDataCatalog: Valid
    ValidateGoldData --> RollbackAndNotify: Invalid
    
    UpdateGlueDataCatalog --> ArchiveRawFiles
    ArchiveRawFiles --> SendSuccessNotification
    
    SendErrorNotification --> [*]
    RollbackAndNotify --> [*]
    SendSuccessNotification --> [*]
    WaitForNextRun --> [*]
```

## Delta Lake Table Structure

The Delta Lake tables are organized in a multi-layer architecture, with each layer serving a specific purpose in the data refinement process.

```mermaid
erDiagram
    RAW_ZONE ||--o{ BRONZE_LAYER : "ingests to"
    BRONZE_LAYER ||--o{ SILVER_LAYER : "refines to"
    SILVER_LAYER ||--o{ GOLD_LAYER : "aggregates to"
    
    RAW_ZONE {
        string bucket_name
        string prefix
        string file_format
        timestamp last_modified
    }
    
    BRONZE_LAYER {
        string table_name
        string schema
        string partition_by
        timestamp created_at
        timestamp updated_at
        string _delta_log
    }
    
    SILVER_LAYER {
        string table_name
        string schema
        string partition_by
        string z_order_by
        timestamp created_at
        timestamp updated_at
        string _delta_log
    }
    
    GOLD_LAYER {
        string table_name
        string schema
        string partition_by
        string z_order_by
        string aggregation_logic
        timestamp created_at
        timestamp updated_at
        string _delta_log
    }
```

## S3 Bucket Structure

The S3 bucket structure organizes data by processing stage and entity type.

```mermaid
graph TD
    S3[S3 Root]
    
    S3 --> RawZone[ecommerce-lakehouse-raw]
    S3 --> ProcessedZone[ecommerce-lakehouse-processed]
    S3 --> DeploymentZone[ecommerce-lakehouse-deployment]
    
    RawZone --> RawProducts[products/]
    RawZone --> RawOrders[orders/]
    RawZone --> RawOrderItems[order_items/]
    RawZone --> RawArchive[archive/]
    
    RawProducts --> RawProductsDate[YYYY-MM-DD/]
    RawOrders --> RawOrdersDate[YYYY-MM-DD/]
    RawOrderItems --> RawOrderItemsDate[YYYY-MM-DD/]
    
    ProcessedZone --> BronzeZone[bronze/]
    ProcessedZone --> SilverZone[silver/]
    ProcessedZone --> GoldZone[gold/]
    
    BronzeZone --> BronzeProducts[products/]
    BronzeZone --> BronzeOrders[orders/]
    BronzeZone --> BronzeOrderItems[order_items/]
    
    SilverZone --> SilverProducts[products/]
    SilverZone --> SilverOrders[orders/]
    SilverZone --> SilverOrderItems[order_items/]
    
    GoldZone --> GoldDailySales[daily_sales/]
    GoldZone --> GoldProductPerformance[product_performance/]
    GoldZone --> GoldDepartmentAnalytics[department_analytics/]
    GoldZone --> GoldCustomerInsights[customer_insights/]
    
    DeploymentZone --> ETLScripts[etl_scripts/]
    DeploymentZone --> InfrastructureTemplates[infrastructure/]
    DeploymentZone --> ConfigFiles[config/]
```

## AWS Service Integration

This diagram shows how the various AWS services integrate to form the complete lakehouse architecture.

```mermaid
flowchart TD
    S3[Amazon S3] --> Lambda[AWS Lambda]
    S3 --> Glue[AWS Glue]
    S3 --> Athena[Amazon Athena]
    
    Lambda --> StepFunctions[AWS Step Functions]
    
    StepFunctions --> Glue
    
    Glue --> GlueCatalog[AWS Glue Data Catalog]
    
    GlueCatalog --> Athena
    
    Glue --> CloudWatch[Amazon CloudWatch]
    StepFunctions --> CloudWatch
    
    CloudWatch --> SNS[Amazon SNS]
    
    SNS --> Email[Email Notifications]
    
    GitHub[GitHub Actions] --> S3
    GitHub --> StepFunctions
    GitHub --> Glue
    
    IAM[AWS IAM] --> S3
    IAM --> Lambda
    IAM --> Glue
    IAM --> StepFunctions
    IAM --> Athena
```
