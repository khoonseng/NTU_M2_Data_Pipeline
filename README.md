# NTU_M2_Data_Pipeline
NTU DSAI Module 2 Data Pipeline for London Bicycles


## 1. Project Overview
The London Bicycles Data Pipeline is a high-performance, end-to-end data engineering solution designed to transform raw, fragmented BigQuery datasets into centralized, actionable business intelligence. By automating the journey from source to schema, this pipeline provides a robust foundation for identifying operational efficiencies and strategic growth opportunities within the London bicycle-sharing network.

### Business Value Proposition
This architecture enables the CTO, CEO, and CMO to extract high-value insights — such as seasonal trip volume fluctuations, station popularity rankings, and fleet allocation — that were previously buried in raw data. By centralizing integrated historical data, the pipeline allows leadership to optimize station distributions, refine marketing spend based on usage patterns, and ensure full alignment between data infrastructure and business objectives.


## 2. System Architecture: Hybrid Cloud & Local Processing
We implement a [hybrid architecture](./docs/images/hybrid%20transformation.svg) that balances the agility of local development with the massive scale of the Google Cloud Platform.

**Local Prototyping (DuckDB):** We utilize DuckDB as a local development and prototyping environment. This allows engineers to test dbt models and SQL logic on data subsets without incurring BigQuery costs or latency.

**Production Warehouse (BigQuery):** Finalized models are deployed to BigQuery, leveraging its parallel processing power to handle the full London Bicycles dataset.

### The Modern ELT Ingestion Model
We have transitioned from traditional ETL to a modern ELT (Extract, Load, Transform) model to maximize the native processing capabilities of our cloud warehouse.
- **Extract:** Raw bicycle rental events and station data are pulled from source systems in batches.
- **Load**: Data is pushed directly into a staging area within BigQuery in its raw form, ensuring a low-latency ingestion path.
- **Transform**: Data Analysts and Analytics Engineers execute SQL-based transformations directly within BigQuery using dbt. This minimizes data movement and leverages BigQuery's compute for modular dimensional modeling.


## 3. Data Warehouse Design: The Star Schema
We have implemented a [Star Schema](./docs/images/star%20schema.svg) as the core architecture for our dimensional model. This design choice is functionally superior to a Snowflake Schema for our use case.

### Technical Justification
While a Snowflake Schema offers normalization, its "sub-dimensions" introduce complex relationships that require additional joins, significantly slowing down query performance. We utilize the Star Schema for:
- Read-Heavy Optimization: Specifically engineered for the complex, repetitive queries required by executive dashboards.
- Reduced Complexity: Minimal joins ensure that business users and BI tools can retrieve data with maximum efficiency.

### Schema Components
- **Fact Tables (e.g., Fact_Hire):** The center of the schema, containing quantitative metrics of the business process, such as hire duration and start hour of hire.
- **Dimension Tables (e.g., Dim_Bike, Dim_Station, Dim_Date):** These provide descriptive attributes (the "what, where, when") for slicing and dicing metrics.
- **Hierarchies:** Within Dim_Date, we implement Year > Quarter > Month > Day levels to enable seamless drill-down reporting.

### Cost Optimization Techniques
**Partitioning and Clustering**

To optimize cost and performance, Fact tables are partitioned by start_date_key. This allows BigQuery to scan only relevant data segments, significantly reducing query costs and improving retrieval speed for historical analysis. 
- Refer to [Database Optimizations](./docs/images/db%20optimizations.svg) for explanation of Partitioning and Clustering.
- Refer to [Query Improvements](./docs/images/query%20improvements.svg) for illustrations of optimizations.


## 4. Data Analysis & Executive Insights
Refer to [Analysis and Insights](Data%20Analysis%20and%20Insights.md) for more details.

We bridge the gap between the warehouse and the executive suite by connecting to the Star Schema via SQLAlchemy and Pandas.

**Primary Analytics Metrics**

The pipeline is engineered to surface the following bicycle-specific KPIs:
- Rental Volume Trends: Monthly and seasonal fluctuations in bicycle rentals.
- Station Popularity: Ranking stations by departure and arrival density to identify infrastructure needs.


## 5. Data Pipeline Walkthrough
Refer to the [Overall Architecture](./docs/images/overall%20architecture.svg) for the high level design of the data pipeline.

Refer to the [Hybrid Development Approach](./docs/images/hybrid%20development.svg) which illustrates the DRY (Don't Repeat Yourself) principle.
- Ingestion: How the Python script moves Parquet files from GCS to DuckDB.
- Transformation: How dbt creates the Star Schema (Dim/Fact tables) using Kimball methodology.
- Data Quality: Using dbt tests to check for nulls, duplicates, and referential integrity


## 6. dbt Configuration (Local and Cloud)


- Profiles Setup: Provide a template for profiles.yml showing both the dev (DuckDB) and prod (BigQuery) targets
- Execution Commands: List the specific commands to run the pipeline in different environments (e.g., dbt run --target local vs. dbt run --target cloud)



## 7. Documentation and Lineage
Refer to [DBT Documentation](https://khoonseng.github.io/NTU_M2_Data_Pipeline/) for both local and cloud dbt documents. This acts as a direct link where stakeholders and interested parties can view the interactive dbt documents and data lineage.



## 8. Quick Start and Environment Setup
Refer to [Setup Instructions](Setup%20Instructions.md) for more details.
