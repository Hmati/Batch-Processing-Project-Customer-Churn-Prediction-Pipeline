# Batch-Processing-Project-Customer-Churn-Prediction-Pipeline
Examine consumer patterns and forecast attrition by utilizing past engagement data. Create a batch processing system with Apache Spark, 
showcasing data preparation, and save the modified data in Delta Lake. Display findings using a business intelligence platform such as Tableau or Power BI.


## **Overview**  
This project demonstrates how to build a batch processing pipeline for predicting customer churn using historical interaction data. The pipeline is implemented with **Apache Spark** and **Delta Lake** and deployed on **AWS S3** for scalable storage. Insights from the data are visualized using a BI tool to provide actionable recommendations.  

## **Objectives**  
1. Ingest, process, and store customer data efficiently using Apache Spark.  
2. Preprocess data by handling missing values, feature engineering, and encoding.  
3. Predict customer churn using machine learning with PySpark MLlib.  
4. Store transformed data in Delta Lake for efficient querying.  
5. Visualize churn insights to guide business strategies.  

---

## **Pipeline Architecture**  
The pipeline consists of the following stages:  
1. **Data Ingestion:** Raw data is uploaded to AWS S3 and loaded into Spark for processing.  
2. **Data Preprocessing:** Clean and transform the data, handle missing values, and perform feature engineering.  
3. **Feature Selection & Modeling:** Train a machine learning model to predict customer churn using PySpark MLlib.  
4. **Data Storage:** Save processed data and predictions in Delta Lake for future analysis.  
5. **Visualization:** Use a BI tool (e.g., Tableau, Power BI) to create churn-related dashboards.  

---

## **Tools and Technologies**  
- **Apache Spark**: For data processing and machine learning.  
- **PySpark**: Python API for Apache Spark.  
- **AWS S3**: Cloud storage for raw and processed data.  
- **Delta Lake**: For efficient storage and querying of transformed data.  
- **Jupyter Notebooks**: For project development and documentation.  
- **BI Tools**: Tableau or Power BI for visualizing insights.  

---

## **Dataset**  
- **Source:** [Telco Customer Churn Dataset](https://www.kaggle.com/blastchar/telco-customer-churn).  
- **Description:** Contains information about customer demographics, services, tenure, and churn status.  
- **Key Features:**  
  - Demographics: `gender`, `SeniorCitizen`, `Partner`, `Dependents`  
  - Services: `InternetService`, `Contract`, `PaymentMethod`  
  - Metrics: `tenure`, `MonthlyCharges`, `TotalCharges`  
  - Target: `Churn`  

---

## **Project Files**  
- `notebooks/`  
  - `01_data_ingestion.ipynb`: Load and validate raw data from AWS S3.  
  - `02_data_preprocessing.ipynb`: Clean data, handle missing values, and engineer features.  
  - `03_model_training.ipynb`: Train a churn prediction model using PySpark MLlib.  
  - `04_data_storage.ipynb`: Store transformed data in Delta Lake.  
- `README.md`: Project overview and instructions.  
- `results/`: Contains sample visualizations and dashboards.  
- `data/`: Placeholder for dataset (instructions for download provided).  

---

## **Setup Instructions**  

### **Step 1: Prerequisites**  
- Python (3.8 or later)  
- Apache Spark (3.0 or later)  
- AWS account with S3 bucket  
- Jupyter Notebook  

### **Step 2: Install Dependencies**  
Install required Python libraries:  

```bash
pip install pyspark boto3 delta-spark pandas
```

### **Step 3: Clone the Repository**  
```bash
git clone https://github.com/yourusername/customer-churn-pipeline.git
cd customer-churn-pipeline
```

### **Step 4: Configure AWS S3**  
Update your AWS credentials in the `notebooks/01_data_ingestion.ipynb` file:  

```python
spark = SparkSession.builder \
    .appName("Customer Churn Pipeline") \
    .config("spark.hadoop.fs.s3a.access.key", "<your-access-key>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<your-secret-key>") \
    .getOrCreate()
```

### **Step 5: Run the Notebooks**  
Follow the pipeline steps using the provided notebooks.  

---

## **Results**  
The following insights were generated:  
- **Churn Rate:** Percentage of customers likely to churn.  
- **Key Drivers of Churn:** Features like tenure, contract type, and payment method significantly influence churn.  
- **Recommendations:** Strategies to reduce churn based on model predictions.  

Sample visualization:  

![Churn Rate by Demographics](results/churn_rate_by_demographics.png)  

---

## **Future Work**  
- Automate the pipeline with Apache Airflow.  
- Add real-time processing capabilities with Apache Kafka.  

---

## **Contributing**  
Feel free to fork this repository, make updates, and submit a pull request.  

---

