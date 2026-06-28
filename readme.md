🔄 Data Ingestion Pipeline in Microsoft Fabric

This project demonstrates how to build an end-to-end data ingestion pipeline in Microsoft Fabric, combining pipeline orchestration with Apache Spark for scalable data transformation and loading.

🚀 Overview

In this lab, we implemented a full ELT pipeline that:

Extracts data from an external HTTP source
Loads it into a Fabric Lakehouse
Transforms the data using PySpark
Stores results in a Delta table
Automates the workflow using a Fabric pipeline



🧱 Architecture
```
External CSV (HTTP)
        ↓
   Pipeline (Copy Activity)
        ↓
   Lakehouse (Raw Files)
        ↓
   Spark Notebook (Transform)
        ↓
   Delta Table (Analytics Ready)
```


📂 Dataset
Source:
```<https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv>```
Format: CSV (Sales data)
⚙️ Steps Performed

1. Create Workspace
Created a Microsoft Fabric workspace
Enabled Fabric capacity (Trial / Premium / Fabric)

2. Create Lakehouse
Created a Lakehouse for data storage
Created a folder:
```
Files/new_data
```


3. Create Pipeline

Pipeline name: Ingest Sales Data

Activities:
Delete Data → Clears old files
Copy Data → Ingests CSV from HTTP
Notebook → Transforms & loads data


4. Configure Copy Activity
Source:
Type: HTTP
URL:
```
https://raw.githubusercontent.com/MicrosoftLearning/dp-data/main/sales.csv
```

Format: DelimitedText (CSV)
Header: Enabled

Destination:
    - Lakehouse → Files/new_data/sales.csv


5. Create Notebook

Notebook name: Load Sales

Parameter Cell:
```
table_name = "sales"
```

Transformation Logic:
```
from pyspark.sql.functions import *

df = spark.read.format("csv").option("header","true").load("Files/new_data/*.csv")

df = df.withColumn("Year", year(col("OrderDate"))) \
       .withColumn("Month", month(col("OrderDate")))

df = df.withColumn("FirstName", split(col("CustomerName"), " ").getItem(0)) \
       .withColumn("LastName", split(col("CustomerName"), " ").getItem(1))

df = df["SalesOrderNumber", "SalesOrderLineNumber", "OrderDate",
        "Year", "Month", "FirstName", "LastName",
        "EmailAddress", "Item", "Quantity", "UnitPrice", "TaxAmount"]

df.write.format("delta").mode("append").saveAsTable(table_name)
```

6. Modify Pipeline

Added orchestration logic:

🧹 Delete Data Activity
Removes existing .csv files before ingestion

📥 Copy Data Activity
Downloads fresh data into Lakehouse

📓 Notebook Activity
Executes transformation logic
Passes parameter:

```
table_name = "new_sales"
```


📊 Output
Raw data stored in:

```
Files/new_data/sales.csv
```

Transformed data stored as:
```
Tables/new_sales
```

Format: Delta Lake


📈 Key Learnings
Building ETL pipelines in Microsoft Fabric
Using Copy Data activity for ingestion
Orchestrating workflows with pipelines
Parameterizing notebooks
Transforming data with PySpark
Writing to Delta tables
Automating repeatable data workflows


🧹 Cleanup
To avoid unnecessary resource usage:

Stop any running Spark sessions
Delete the Fabric workspace after completion

📌 Requirements
Access to Microsoft Fabric
Basic knowledge of:
Python / PySpark
Data pipelines (ETL concepts)

💡 Notes
First Spark execution may take time to initialize
Parameterized notebooks improve reusability
Pipelines enable production-ready data workflows

📎 References
Microsoft Learn Lab: Ingest data with a pipeline in Microsoft Fabric
Apache Spark Documentation
Delta Lake Documentation
