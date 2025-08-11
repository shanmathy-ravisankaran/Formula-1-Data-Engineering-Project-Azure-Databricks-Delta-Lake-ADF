🏎 Formula 1 Data Engineering – Azure Databricks & Delta Lake

    This project delivers a real-time inspired Formula 1 analytics pipeline built on Azure Databricks, Azure Data Factory (ADF), and Delta Lake. It automates data ingestion, transformation, and analysis using a Lakehouse approach, enabling insights into driver performance, team dominance, and race trends.

📌 Architecture Overview

Workflow Summary

    1.Source: Fetch Formula 1 race data from the Ergast API
    2.Raw Layer: Store original JSON/CSV files in Azure Data Lake Gen2 (Bronze zone)
    3.Ingestion Pipeline: Use Databricks PySpark notebooks to standardize schema, add audit fields, and store in Delta format (Silver zone)
    4.Transformation Pipeline: Join, aggregate, and create presentation-ready Delta tables (Gold zone)
    5.Analytics Layer: Query curated data for trend analysis
    6.Reporting: Visualize results in Power BI dashboards
  <img width="3551" height="1998" alt="solution architecture" src="https://github.com/user-attachments/assets/e60e4b27-b90e-41c7-becd-3524e48b35f6" />

📊 Insights & Dashboards

Dominant Drivers

    1.Tracks championship points and race counts by driver
    2.Highlights multi-season dominance patterns
    3.Compares performance trends across eras
   <img width="1619" height="832" alt="Screenshot 2025-08-09 145859" src="https://github.com/user-attachments/assets/2ee5e00c-854c-4672-8c69-c32a6fefdc70" />

Dominant Teams

     1.Analyzes constructor performance over decades
     2.Visualizes changes in team dominance
     3.Highlights shifts in competitive balance
  <img width="1386" height="686" alt="Screenshot 2025-08-09 150005" src="https://github.com/user-attachments/assets/27686a56-31ce-43a1-a5f8-6b70f60474f6" />


🚀 Key Features

     1.Automated Orchestration: Pipelines triggered & monitored via Azure Data Factory
     2.Lakehouse Storage: ACID-compliant Delta tables in ADLS Gen2
     3.Incremental Processing: Upserts via MERGE to handle new race data
     4.Multi-Layer Data Flow:
        -Bronze – Raw API data
        -Silver – Cleaned, structured datasets
        -Gold – Aggregated analytical tables
     5.Version Control: Time travel & rollback using Delta history
     6.Secure Access: Secrets managed in Azure Key Vault

🛠 Tools & Technologies

    1.Languages: PySpark, SQL
    2.Services: Azure Databricks, Azure Data Factory, Power BI
    3.Storage: Azure Data Lake Storage Gen2
    4.Frameworks: Delta Lake

⚙️ How to Run

1.Set Up Azure Resources
    
    -Create ADLS Gen2, Databricks workspace, and ADF instance
    -Configure service principal & mount storage in Databricks
    
2.Deploy Pipelines
   
    -Import ADF JSONs from adf/
    -Set linked service parameters to use Key Vault secrets
    
3.Run Order
   
    -Ingest Formula 1 Data
    -Transform Formula 1 Data
    -Process Presentation Layer
    
4.Visualize

    Connect Power BI to Gold layer tables

📂 Repository Structure

    adf/                # Azure Data Factory JSONs (datasets, pipelines, triggers)
    databricks/         # PySpark & SQL notebooks for ingestion, transformation, processing
    docs/               # Architecture diagrams, dashboard screenshots
    scripts/            # Optional utility scripts for setup/testing

📈 Results & Learnings

    - Built a fully automated ETL pipeline with Azure Databricks and ADF  
    - Achieved incremental loads with Delta Lake MERGE  
    - Created analytics-ready tables for driver & constructor standings  
    - Designed interactive Power BI dashboards for race trends

📜 License

  This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details
