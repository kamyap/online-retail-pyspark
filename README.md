Online Retail Analytics using PySpark

This project analyzes an online retail dataset using PySpark on Databricks. It covers data ingestion, cleaning, exploratory analysis, and business-focused aggregations to generate insights like customer value, product performance, and sales trends.

Tools & Setup

- Platform: Databricks
- Language: Python (PySpark)
- Data Source: UCI Online Retail Dataset (https://archive.ics.uci.edu/ml/datasets/Online+Retail)
- Format: Input as CSV, stored as Delta tables

To run locally:
pip install -r requirements.txt

Notebooks

- data_ingestion_cleaning.py – Load and clean data
- exploratory_data_analysis.py – Explore key metrics and patterns
- transformations_aggregations.py – Customer, product, and time-based aggregations

Insights Generated

- Revenue by country
- Top products by revenue
- Monthly sales trends

Notes

- Dataset is not included in the repo. Please upload it to your Databricks DBFS.
- Built for demonstration and learning purposes.
