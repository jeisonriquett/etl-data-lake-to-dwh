-- ============================================
--   DATABASE CREATION (optional)
-- ============================================
CREATE DATABASE IF NOT EXISTS bootcampretail_dwh;
USE bootcampretail_dwh;

-- ============================================
--   DIMENSION TABLE: CLIENTES
-- ============================================
DROP TABLE IF EXISTS dim_clientes;

CREATE TABLE dim_clientes (
    cliente_key INT AUTO_INCREMENT PRIMARY KEY,
    cliente_id_origen INT NOT NULL,
    nombre_cliente VARCHAR(150),
    ciudad VARCHAR(100),
    pais VARCHAR(100),
    segmento_cliente VARCHAR(100),
    fecha_registro DATE,

    UNIQUE (cliente_id_origen)
);

-- ============================================
--   DIMENSION TABLE: PRODUCTOS
-- ============================================
DROP TABLE IF EXISTS dim_productos;

CREATE TABLE dim_productos (
    producto_key INT AUTO_INCREMENT PRIMARY KEY,
    producto_id_origen INT NOT NULL,
    nombre_producto VARCHAR(200),
    categoria VARCHAR(100),
    subcategoria VARCHAR(100),
    proveedor VARCHAR(150),

    UNIQUE (producto_id_origen)
);

-- ============================================
--   DIMENSION TABLE: TIEMPO
-- ============================================
DROP TABLE IF EXISTS dim_tiempo;

CREATE TABLE dim_tiempo (
    fecha_key INT PRIMARY KEY,
    fecha_completa DATE,
    anio INT,
    mes INT,
    dia INT,
    dia_semana VARCHAR(20),
    trimestre INT,
    es_fin_de_semana BOOLEAN
);

-- ============================================
--   FACT TABLE: VENTAS
-- ============================================
DROP TABLE IF EXISTS fact_ventas;

CREATE TABLE fact_ventas (
    venta_id INT AUTO_INCREMENT PRIMARY KEY,

    fecha_key INT NOT NULL,
    cliente_key INT NOT NULL,
    producto_key INT NOT NULL,

    cantidad_vendida INT,
    precio_unitario DECIMAL(12,2),
    monto_total_venta DECIMAL(12,2),

    FOREIGN KEY (fecha_key) REFERENCES dim_tiempo(fecha_key),
    FOREIGN KEY (cliente_key) REFERENCES dim_clientes(cliente_key),
    FOREIGN KEY (producto_key) REFERENCES dim_productos(producto_key)
);
