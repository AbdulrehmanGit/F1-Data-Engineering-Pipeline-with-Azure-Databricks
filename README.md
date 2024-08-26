# F1-Data-Engineering-Pipeline-with-Azure-Databricks
![F1-Data-Engineering-Pipeline-with-Azure-Databricks (1)](https://github.com/user-attachments/assets/bb92b7d5-9196-477e-9f07-4ea21642bfae)

# Introduction
The F1 Data Engineering Pipeline project showcases a sophisticated approach to processing and analyzing historical F1 race data. The primary goal of this project is to convert raw data into actionable insights, catering to the needs of data scientists, analysts, and machine learning engineers. Leveraging Databricks and the Azure ecosystem, the project orchestrates data transformations, secure storage, and automated workflows within a cloud-first architecture.

#  Project Structure
#  1. Data Acquisition
Source: The project pulls structured and semi-structured data from the Ergast API, which provides historical Formula 1 race data in various formats like CSV and JSON.
Storage: This raw data is securely stored in Azure Data Lake Storage Gen2, ensuring high availability and scalability.
![data acquisition](https://github.com/user-attachments/assets/ed61dffc-35e7-4d7f-bd6e-107e071c7671)

# 2. Ingestion Process
Integration: Using Databricks, the data stored in Azure Data Lake Storage is ingested and prepped for subsequent processing.
Automation: The entire ingestion process is automated through Azure Data Factory, enabling smooth and error-free data flow.
![Ingestoin](https://github.com/user-attachments/assets/4326fcd6-cb1b-4571-9197-75356cdf2005)

# 3. Data Quality and Validation
Validation Notebooks: Custom notebooks within Databricks were created to validate the ingested data, ensuring consistency and accuracy before proceeding to transformation stages.
![Data Validation](https://github.com/user-attachments/assets/cc82f17f-0d52-4247-92c4-1a76197a6711)
![Data Validation 2](https://github.com/user-attachments/assets/e58929c0-9067-481d-99ac-7e19a0610ede)


# 4. Data Transformation
Processing: PySpark is the backbone of the transformation process, where raw data is cleansed, structured, and transformed into a usable format.
Storage: The transformed data is saved back to Azure Data Lake Storage in Delta format, making it ready for downstream analytics.
![Data Transformatiomn](https://github.com/user-attachments/assets/a259e4ee-c6e3-4084-a095-4d667728674a)

# 5. Advanced Data Processing
Analysis: Leveraging PySpark SQL, the transformed data is processed to derive key insights and metrics relevant to F1 racing.
Optimization: Delta Lake technology ensures that the data remains consistent and high-quality throughout the processing stages.
![Advanced Data processing](https://github.com/user-attachments/assets/4de17339-f092-410f-9060-de3f0b603813)
![Advanced Data processing 2](https://github.com/user-attachments/assets/52572699-0294-4346-b31d-4656e921dcbd)

# 6. Structured Storage
Database Setup: A structured database is established within Azure Data Lake, organizing processed data into easily accessible tables.
Format: All data is stored in Delta format, ensuring optimal performance for querying and analytics.
![Data base Creation](https://github.com/user-attachments/assets/7ce94ad1-2772-469e-8bbc-718c790db08a)
![Data base Creation](https://github.com/user-attachments/assets/10e034f1-a5b7-49e0-ba53-6a25424450bb)

# 7. Data Presentation
SQL Layer: A robust SQL presentation layer is developed to facilitate data exploration and visualization.
Power BI Integration: Although the connection with Power BI is planned, the SQL layer sets the foundation for future data visualizations.
![presentation](https://github.com/user-attachments/assets/cf25d7cb-4abe-4684-a008-77fa635563fb)

# 8. Automation and Orchestration
Azure Data Factory: A pipeline is designed in Azure Data Factory which automates the entire data engineering process. From ingestion to final processing, every step is automated, triggering actions whenever new data is available.
![pipeline 1](https://github.com/user-attachments/assets/ace89e1b-a4d1-4848-aed4-fcd36a1b5a1b)
![pipeline 2](https://github.com/user-attachments/assets/a1a5dda8-61d5-455b-ba79-f736b37000c6)
![pipeline 3](https://github.com/user-attachments/assets/1c8ff815-0374-48cf-a68d-a40e481f1f49)

# Technology Stack
- Databricks: Core platform for data processing and transformation.
- Azure: Cloud services used to manage resources and secure data storage.
- Azure Data Factory: Powers the automation and orchestration of data pipelines.
- Azure Data Lake Storage Gen2: Ensures scalable and reliable storage for raw and processed data.
- PySpark: Handles the heavy lifting of data transformations.
- SQL: Provides the querying capabilities for structured data analysis.
- Delta Lake: Maintains data reliability and quality throughout the project lifecycle.

# Conclusion
This project demonstrates a comprehensive data engineering pipeline capable of transforming vast amounts of historical F1 data into valuable insights. By leveraging modern cloud-based technologies and automating key processes, the project is designed to be both scalable and adaptable to future requirements.
