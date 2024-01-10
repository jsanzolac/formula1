# Formula 1 data analysis
Analysis of Formula 1 data in a Databricks based Spark environment

# ELT Process
- Following a lakehouse design data is Extracted a Loaded into the system.
- Using Delta Lake all data is moved into a Bronze stage where transformation rules are applied to promote it to Silver and Gold layers.
- S3 is used as the object storage for data ingested into the Bronze, Silver and Gold layers.

# Data management and data quality considerations
- The dataset extracted from Kaggle is not correctly documented so tables are classified into master data tables and transactional tables.
- There's no associated data dictionary so there's no full clarity on the meaning of a considerable set of variables across tables.
- There's is a need to profile and assess the quality of the data.

## Actions regarding data management and quality:
- In the Bronze layer data is divided into Master and Transactional data.
- Results associated with data profiles is presented in formula1/data_profiling.
- Data quality procedures are explained in formula1/data_quality.

