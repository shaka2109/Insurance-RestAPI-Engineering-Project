## Introduction

This project simulates a modern end-to-end data engineering solution for an insurance company using Microsoft Azure and Databricks technologies. The solution ingests operational insurance data from REST APIs and relational databases, processes it through a Medallion Architecture (Bronze, Silver, and Gold layers), and delivers analytical datasets optimized for reporting and business intelligence. The project demonstrates the implementation of scalable data pipelines, Delta Lake storage, data quality transformations, dimensional modeling, workflow orchestration, CI/CD automation, and Infrastructure as Code using Databricks Asset Bundles (DABs).

## Project Objectives

- Extract insurance operational data from REST APIs.
- Load and process large datasets using Databricks and Delta Lake.
- Implement a Medallion Architecture (Bronze, Silver, Gold).
- Apply data cleansing and standardization techniques.
- Build dimensional models (Fact and Dimension tables).
- Orchestrate workflows using Databricks Jobs.
- Implement CI/CD with GitHub Actions and Databricks Asset Bundles.
- Demonstrate industry-standard Data Engineering practices.

## Technologies

- Azure Databricks
- Delta Lake
- PySpark
- SQL
- REST APIs
- Databricks Asset Bundles (DABs)
- GitHub Actions
- GitHub
- SQL Server
- Azure Data Lake Storage

## Medallion Arquitecture

### Bronze Layer

The Bronze layer ingests raw data directly from external REST API endpoints.

Key activities:

- API extraction
- Raw data ingestion
- Schema preservation
- Incremental loading
- Delta Lake storage

### Silver Layer

The Silver layer performs data quality and transformation processes.

Key activities:

- Null handling
- Data standardization
- Type casting
- Hash columns
- Audit columns generation
- Load timestamps
- Data quality validations

### Gold Layer

The Gold layer provides business-ready datasets optimized for analytics and reporting.

Key activities:

- Fact tables creation
- Dimension tables creation
- Aggregations
- Common Table Expressions (CTEs)
- Business metrics calculations
- Analytical modeling

## Workflow Orchestration

The project uses Databricks Jobs to orchestrate the execution of the Medallion Architecture.

Pipeline execution flow:

1. Bronze ingestion jobs
2. Silver transformation jobs
3. Gold modeling jobs
4. Full pipeline execution

The workflow ensures dependencies between layers and guarantees data consistency throughout the process.

## CI/CD Implementation

Continuous Integration and Continuous Deployment (CI/CD) were implemented using GitHub Actions and Databricks Asset Bundles (DABs).

Deployment workflow:

- Code push to the develop branch triggers deployment to the development environment.
- Pull requests merged into main trigger deployment to the production environment.
- Bundle validation is executed before deployment.
- Databricks Jobs and resources are automatically updated.

This approach enables Infrastructure as Code (IaC), version control, and automated deployments.

## Repository Structure

```text
Insurance-RESTAPI-Engineering-Project/
│
├── databricks.yml
├── resources/
│   ├── bronze_job.yml
│   ├── silver_job.yml
│   ├── gold_job.yml
│   └── full_pipeline.yml
│
├── notebooks/
│   ├── bronze/
│   ├── bilver/
│   └── bold/
│   └── misc/
│
└── .github/
    └── workflows/
        └── databricks-cicd.yml
```


---

## Results

The project successfully demonstrates:

- End-to-end REST API ingestion.
- Scalable Medallion Architecture implementation.
- Automated workflow orchestration.
- CI/CD deployment automation.
- Dimensional modeling for analytical workloads.
- Production-ready Data Engineering practices using Databricks.

## Future Enhancements

- Incremental API ingestion using Change Data Capture (CDC)
- Data Quality monitoring
- Unit testing framework
- Terraform integration
- Multi-environment deployment strategy
- Monitoring and alerting

## Large Dataset Processing

The solution was tested with datasets exceeding 17 million records, demonstrating the scalability of Databricks and Delta Lake for large-scale analytical workloads.
