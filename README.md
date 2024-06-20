# Project Name
ETL Process and Data Visualization for RecyThing

# About Project
RecyThing is an innovative project aimed at enhancing waste management and recycling processes through advanced data analytics and visualization. By leveraging the power of BigQuery and data visualization tools, RecyThing provides insights into recycling patterns, efficiency of waste collection, and the overall impact on the environment.

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
- NAGA AI
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
![image](..//recything-de/etl-architecture.png)

# Schema Data Warehouse
![image](..//recything-de/multi-star-schema-de.png)

# Dashboard Visualization
[Dashboard Visualization Data Engineer](https://lookerstudio.google.com/reporting/69667802-88f6-47c2-a083-f2b7a5a6febd)

# Setup
- Clone repository proyek dari GitHub.
- Masuk ke dalam direktori proyek dan instal semua dependensi yang diperlukan.
- Ubah file konfigurasi (biasanya berupa file .env) untuk mencocokkan pengaturan lokal, termasuk konfigurasi database, sumber data, dan target data.
- Menjalankan proyek pipelines ETL
- Pantau proses ETL saat berjalan dan pastikan untuk menangani semua kesalahan atau masalah yang mungkin muncul.
- Pastikan bahwa data yang diekstrak, ditransformasi, dan dimuat (ETL) sesuai dengan yang diharapkan dengan menjalankan tes yang sesuai.
- Melakukan optimasi kinerja atau penyetelan lebih lanjut untuk meningkatkan kinerja atau keandalan.