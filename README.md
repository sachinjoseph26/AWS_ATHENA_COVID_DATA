# AWS Data Processing and Analysis Architecture for User Behavior Analysis Project

## Data Source: Reddit Covid 19 Dataset

### Description: This project analyzes user behavior on Reddit during the COVID-19 pandemic using AWS services. It leverages AWS Glue with PySpark for ETL, MongoDB for data storage, Amazon SageMaker for machine learning, and AWS QuickSight for data visualization.

### Project Architecture:
![alt text](image.png)

#### The architecture consists of the following components:

1. **Data Ingestion:** Amazon S3 stores raw COVID-19 Reddit data for centralized access.
2. **Data Processing (ETL):** AWS Glue with PySpark cleans and preprocesses data before storage.
3. **Transformed Data Storage:** Processed data is stored in S3, and user behavior data is stored in MongoDB Atlas.
4. **Querying Processed Data:** AWS Athena queries processed data in S3 using the Glue Data Catalog.
5. **Machine Learning for Behavior Modeling:** Amazon SageMaker performs feature engineering and trains ML models on user behavior data.
6. **Optional Sentiment Analysis:** AWS Comprehend analyzes sentiment in comments to gauge emotional trends.
7. **Data Visualization:** Amazon QuickSight creates interactive dashboards from Athena queries.
8. **Automation:** AWS Lambda automates ETL, query, and dashboard refresh tasks across the pipeline.
### Project Structure

```plaintext
├── data/                         # Data folder
├── notebooks/                    # Jupyter notebooks for data exploration and preprocessing
├── scripts/                      # Scripts for data processing, analysis, and ML models
└── README.md                     # Project documentation
