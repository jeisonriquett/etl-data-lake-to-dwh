ETL Pipeline: Data Lake → Data Warehouse
Robust Python ETL for Customers, Products, Date & Sales Facts

This repository contains a robust ETL pipeline built in Python to extract data from a raw CSV file (simulating a Data Lake), transform it into dimensional tables, and load it into a MySQL Data Warehouse following a star schema.

The pipeline automatically generates:

dim_clientes

dim_productos

dim_tiempo

fact_ventas

All transformation logic comes from the script etl_facturas.py, which includes validations, error handling, auto-separator CSV reading, and lookup-driven surrogate key assignment.
