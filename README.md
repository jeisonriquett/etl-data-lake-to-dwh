ETL Pipeline: Data Lake → Data Warehouse
Robust Python ETL for Customers, Products, Date & Sales Facts

This project implements a robust ETL pipeline in Python that extracts raw sales data from a CSV file, transforms it into dimensional tables, and loads everything into a MySQL Data Warehouse using a Star Schema.

The pipeline generates four core tables:

dim_clientes

dim_productos

dim_tiempo

fact_ventas

All ETL logic is handled inside a single script:
👉 etl_facturas.py

🚀 Features

Automatic CSV separator detection

Detection of malformed rows (on_bad_lines='warn')

Date normalization and validation

Automatic generation of the Date Dimension

Surrogate key lookups for customers & products

Full ETL flow: Extract → Transform → Load

Error handling and missing-key reporting

Loads data into a MySQL Data Warehouse

📁 Project Structure
etl-data-lake-to-dwh/
│
├── etl_facturas.py           # Main ETL script
│
├── data/
│   └── datafact_ventas.csv   # Raw data (Data Lake simulation)
│
├── images/
│   └── banner.png            # Optional banner (LinkedIn/GitHub)
│
└── README.md

🧱 Star Schema Overview
               dim_clientes
                    ▲
                    │
               fact_ventas ─────► dim_productos
                    ▲
                    │
               dim_tiempo

🛠️ Technologies Used

Python 3.x

Pandas

SQLAlchemy

MySQL + mysql-connector

Dimensional Modeling

ETL Pipelines

Data Warehouse Architecture

⚙️ Installation

Install dependencies:

pip install pandas sqlalchemy mysql-connector-python

🧩 How It Works
✔ 1. Extract

Reads the raw CSV file:

df = extract_facturas("datafact_ventas.csv")


The script:

Auto-detects separator

Warns for malformed lines

Prints first rows

Shows the detected columns

✔ 2. Transform

Generates:

🔹 dim_clientes

Unique customers

Cleans and converts registration dates

🔹 dim_productos

Unique products

Includes category & supplier information

🔹 dim_tiempo

Automatically created from fecha_venta:

fecha_key (YYYYMMDD)

Year, month, day

Weekday name

Quarter

Weekend flag

Full date (fecha_completa)

🔹 fact_ventas

Surrogate key lookups

Numeric cleaning

Missing-key detection

✔ 3. Load (MySQL Data Warehouse)

Each table is appended using:

df.to_sql(table_name, engine, if_exists='append', index=False)


The pipeline loads:

Dimensions first

Fact table after foreign keys are resolved

▶️ Running the ETL

Simply run:

python etl_facturas.py


Output includes:

Preview of raw file

Missing keys report

Rows loaded per table

Potential date issues

🧪 Example SQL Validation Query
SELECT  
    fv.fact_id,
    dc.nombre_cliente,
    dp.nombre_producto,
    dt.fecha_completa,
    fv.cantidad_vendida,
    fv.precio_unitario,
    fv.monto_total_venta
FROM fact_ventas fv
JOIN dim_clientes dc ON fv.cliente_key = dc.cliente_key
JOIN dim_productos dp ON fv.product_key = dp.product_key
JOIN dim_tiempo dt   ON fv.fecha_key = dt.fecha_key
LIMIT 20;

📸 Screenshots (Optional)

Place images inside /images and reference them:

![ETL Banner](images/banner.png)

📜 License

MIT License.

🎯 Author

Jeison Jose Riquett Patiño
