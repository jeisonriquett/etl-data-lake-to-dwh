# etl_facturas.py (versión robusta)
import os
import pandas as pd
from sqlalchemy import create_engine, text
import datetime

# --- CONFIGURACIÓN CONEXIÓN ---
DB_USER = "root"
DB_PASSWORD = ""        # ajusta si tienes contraseña
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "bootcampretail_dwh"

engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}", echo=False)

# --- EXTRACT ---
def extract_facturas(filepath='datafact_ventas.csv'):
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Archivo no encontrado: {filepath} (asegura ruta y nombre)")

    # Intento robusto de lectura: detecta separador o fuerza sep=','
    try:
        # probar autoseparador (engine python permite sep=None)
        df = pd.read_csv(filepath, sep=None, engine='python', encoding='latin1', on_bad_lines='warn')
    except Exception:
        # fallback común
        df = pd.read_csv(filepath, sep=',', encoding='latin1', on_bad_lines='warn')

    print("=== Archivo leído correctamente ===")
    print("Columnas detectadas:", df.columns.tolist())
    print("Primeras 5 filas:")
    print(df.head(5).to_string(index=False))
    return df

# --- TRANSFORM ---
def generate_dim_tiempo(df):
    # Asegurar columna de fecha como datetime
    df['fecha_venta'] = pd.to_datetime(df['fecha_venta'], errors='coerce')

    # Mostrar filas con fecha inválida para que puedas corregir el CSV si prefieres
    invalid_dates = df[df['fecha_venta'].isna()]
    if not invalid_dates.empty:
        print(f"⚠️ Se detectaron {len(invalid_dates)} filas con fecha_venta inválida (NaT). Muestra:")
        print(invalid_dates.head(10).to_string(index=False))

    # Tomar solo fechas válidas únicas y ordenadas
    fechas = pd.to_datetime(df['fecha_venta'].dropna().unique())
    fechas = sorted(fechas)

    # Construir dim_tiempo usando nombre consistente fecha_completa
    rows = []
    for f in fechas:
        rows.append({
            'fecha_key': int(f.strftime('%Y%m%d')),
            'fecha_completa': f.date(),
            'anio': f.year,
            'mes': f.month,
            'dia': f.day,
            'dia_semana': f.strftime('%A'),
            'trimestre': (f.month - 1)//3 + 1,
            'es_fin_de_semana': f.weekday() >= 5
        })
    dim_tiempo = pd.DataFrame(rows)
    return dim_tiempo

def transform_dim_clientes(df):
    # Verificar que existan las columnas esperadas; si no, mostrar cuáles faltan
    expected = ['cliente_id_origen','nombre_cliente','ciudad','pais','segmento_cliente','fecha_registro']
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise KeyError(f"Faltan columnas en clientes: {missing}. Columnas disponibles: {df.columns.tolist()}")

    dim_clientes = df[expected].drop_duplicates(subset=['cliente_id_origen']).copy()
    # Convertir fecha_registro a datetime (opcional)
    if 'fecha_registro' in dim_clientes.columns:
        dim_clientes['fecha_registro'] = pd.to_datetime(dim_clientes['fecha_registro'], errors='coerce').dt.date
    return dim_clientes

def transform_dim_productos(df):
    expected = ['producto_id_origen','nombre_producto','categoria','subcategoria','proveedor']
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise KeyError(f"Faltan columnas en productos: {missing}. Columnas disponibles: {df.columns.tolist()}")

    dim_productos = df[expected].drop_duplicates(subset=['producto_id_origen']).copy()
    return dim_productos

def transform_fact_ventas(df, conn):
    # Asegurarse de que dimensiones ya existen en la BD para mapear claves
    clientes = pd.read_sql("SELECT cliente_key, cliente_id_origen FROM dim_clientes", conn)
    productos = pd.read_sql("SELECT producto_key, producto_id_origen FROM dim_productos", conn)

    # Merge en df para obtener cliente_key y producto_key
    if 'cliente_id_origen' not in df.columns or 'producto_id_origen' not in df.columns:
        raise KeyError("Las columnas 'cliente_id_origen' o 'producto_id_origen' no están en el CSV")

    df_fact = df.merge(clientes, on='cliente_id_origen', how='left') \
                .merge(productos, on='producto_id_origen', how='left')

    # Mostrar filas donde no se encontró la clave para depuración
    missing_clients = df_fact[df_fact['cliente_key'].isna()]
    missing_products = df_fact[df_fact['producto_key'].isna()]
    if not missing_clients.empty:
        print(f"⚠️ Filas con cliente_key faltante: {len(missing_clients)} (ejemplo):")
        print(missing_clients.head(5).to_string(index=False))
    if not missing_products.empty:
        print(f"⚠️ Filas con producto_key faltante: {len(missing_products)} (ejemplo):")
        print(missing_products.head(5).to_string(index=False))

    # Fecha -> fecha_key
    df_fact['fecha_venta'] = pd.to_datetime(df_fact['fecha_venta'], errors='coerce')
    df_fact = df_fact.dropna(subset=['fecha_venta'])
    df_fact['fecha_key'] = df_fact['fecha_venta'].dt.strftime('%Y%m%d').astype(int)

    df_fact_final = df_fact[['fecha_key','cliente_key','producto_key','cantidad_vendida','precio_unitario','monto_total_venta']].copy()
    # Asegurar tipos
    df_fact_final['cantidad_vendida'] = pd.to_numeric(df_fact_final['cantidad_vendida'], errors='coerce').fillna(0).astype(int)
    df_fact_final['precio_unitario'] = pd.to_numeric(df_fact_final['precio_unitario'], errors='coerce').fillna(0)
    df_fact_final['monto_total_venta'] = pd.to_numeric(df_fact_final['monto_total_venta'], errors='coerce').fillna(0)
    return df_fact_final

# --- LOAD ---
def load_to_sql(df, table_name):
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✅ Cargado {len(df)} filas en {table_name}")
    except Exception as e:
        print(f"❌ Error cargando {table_name}: {e}")
        raise

# --- PIPELINE ---
def run_etl():
    # Ajusta la ruta al CSV si es necesario
    csv_path = 'datafact_ventas.csv'  # asegúrate que este archivo exista
    df = extract_facturas(csv_path)

    # Transformaciones
    dim_clientes = transform_dim_clientes(df)
    dim_productos = transform_dim_productos(df)
    dim_tiempo = generate_dim_tiempo(df)

    # Cargar dimensiones primero
    load_to_sql(dim_clientes, 'dim_clientes')
    load_to_sql(dim_productos, 'dim_productos')
    load_to_sql(dim_tiempo, 'dim_tiempo')

    # Cargar hechos (ahora que dimensiones existen en BD)
    with engine.connect() as conn:
        fact_ventas = transform_fact_ventas(df, conn)
        load_to_sql(fact_ventas, 'fact_ventas')

if __name__ == "__main__":
    run_etl()
