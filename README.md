# Project Name
ETL Process and Data Visualization for RecyThing user activity and environmental data

# About Project
The RecyThing project focuses on optimizing and visualizing user activity and environmental data through an efficient ETL (Extract, Transform, Load) process. By extracting data from databases, transforming it into a consistent, analyzable format, and loading it into a centralized database, we ensure data accuracy and accessibility. Our goal is to leverage this processed data to create in-depth visualizations that highlight user behavior and environmental data that support sustainable practices and informed decision-making.

# Tech Stacks
Tools:

- Visual Studio Code
- Jupyter Notebook
- Github Desktop
- Github
- Google Cloud Storage
- Big Query
- Looker Studio
- Drawio
- Figma
- Apache Airflow

Frameworks:

- import os
- import pandas as pd
- from dotenv import load_dotenv
- import mysql.connector
- from google.cloud import storage
- from google.cloud import bigquery
- from google.cloud.exceptions import NotFound

# Architecture Diagram
![image](./etl-architecture.png)

# Schema Data Warehouse
![image](./multi-star-schema-de.png)
[Schema Data Warehouse](https://app.diagrams.net/#G1LylCoUEiQYpQpuj2GeisiBXmiXjsQXTz#%7B%22pageId%22%3A%226ofVpxtqm7AOZWSaK6SD%22%7D)

# Dashboard Visualization
[Dashboard Visualization Data Engineer](https://lookerstudio.google.com/reporting/69667802-88f6-47c2-a083-f2b7a5a6febd)

# Setup
- Clone repository proyek dari GitHub.
- Masuk ke dalam direktori proyek dan instal semua dependensi yang diperlukan.
- Ubah file konfigurasi (berupa file .env) untuk mencocokkan pengaturan lokal, termasuk konfigurasi database dan target load.
- Menjalankan proyek pipelines ETL
- Pantau proses ETL saat berjalan dan pastikan untuk menangani semua kesalahan atau masalah yang mungkin muncul.
- Pastikan bahwa data yang diekstrak, ditransformasi, dan dimuat (ETL) sesuai dengan yang diharapkan dengan menjalankan tes yang sesuai.
- Melakukan optimasi kinerja atau penyetelan lebih lanjut untuk meningkatkan kinerja atau keandalan (seperti otomasi dengan airflow).
