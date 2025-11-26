import os
import sys
import re
import shutil
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, initcap, to_date, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
# ==========================================
# CONFIGURACIÓN CRÍTICA PARA WINDOWS (JAVA)
# ==========================================
# Usamos Java 17 para satisfacer requerimientos de PySpark (Class Version 61.0)
# y evitar errores de SecurityManager de Java 18+
java_path = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"

if os.path.exists(java_path):
    print(f"--- Configurando entorno Java: {java_path} ---")
    os.environ["JAVA_HOME"] = java_path
    os.environ["PATH"] = os.path.join(java_path, "bin") + os.pathsep + os.environ["PATH"]
else:
    print("ADVERTENCIA: No se encontró Java 17 en la ruta esperada.")

# ==========================================
# CONFIGURACIÓN CRÍTICA PARA WINDOWS (HADOOP)
# ==========================================
# Configuramos winutils.exe necesario para operaciones de escritura en Windows
hadoop_home = os.path.join(os.getcwd(), "hadoop")
if os.path.exists(hadoop_home):
    print(f"--- Configurando entorno Hadoop: {hadoop_home} ---")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ["PATH"]
else:
    print("ADVERTENCIA: No se encontró la carpeta 'hadoop'. La escritura de archivos podría fallar.")

# SOLUCIÓN CRÍTICA ERROR SOCKET WINDOWS / PYTHON 3.12+
# Evita "WinError 10038" deshabilitando la reutilización de workers
os.environ["PYSPARK_PYTHON_WORKER_REUSE"] = "0"

# ==========================================
# CONFIGURACIÓN INICIAL
# ==========================================

# Definición de rutas base (simulando estructura de proyecto)
BASE_DIR = os.getcwd()

# ==========================================
# TAREA 2.1: AUTOMATIZACIÓN DESCARGA (GIT)
# ==========================================
REPO_URL = "https://github.com/pconstancioteacher/lidl_project.git"
# Carpeta donde se clonará el repo. Evitamos conflictos con archivos existentes.
REPO_DIR = os.path.join(BASE_DIR, "lidl_project_source")

print(f"\n--- Verificando fuente de datos ---")
if not os.path.exists(REPO_DIR):
    print(f"Carpeta de datos no encontrada. Intentando clonar desde: {REPO_URL}")
    try:
        # Ejecutamos git clone. Requiere Git instalado en el sistema.
        subprocess.check_call(["git", "clone", REPO_URL, REPO_DIR])
        print("-> Repositorio descargado correctamente.")
        RAW_DATA_DIR = REPO_DIR
    except Exception as e:
        print(f"ADVERTENCIA: No se pudo clonar el repositorio ({e}).")
        print("-> Se buscarán los archivos en el directorio local actual.")
        RAW_DATA_DIR = BASE_DIR
else:
    print(f"-> Repositorio detectado en: {REPO_DIR}")
    RAW_DATA_DIR = REPO_DIR

BRONZE_DIR = os.path.join(BASE_DIR, "bronze", "ventas")
SILVER_DIR = os.path.join(BASE_DIR, "silver", "ventas")

# Crear directorios si no existen
for directory in [BRONZE_DIR, SILVER_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Directorio creado: {directory}")

# Iniciar Sesión de Spark
spark = SparkSession.builder \
    .appName("Lidl_ETL_Team_A") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=== INICIO DEL PROCESO ETL (EQUIPO A) ===")

# ==========================================
# ÉPICA 2: CAPA BRONZE - INGESTA (Member 1)
# ==========================================
print("\n--- [BRONZE] Iniciando Ingesta de Datos ---")

# ---------------------------------------------------------
# TAREA 2.3: Guardar datos crudos en /bronze/ventas/
# ---------------------------------------------------------

# 1. Ingesta de clientes_info.csv
# -------------------------------
csv_path = os.path.join(RAW_DATA_DIR, "clientes_info.csv")
if os.path.exists(csv_path):
    print(f"Procesando {csv_path}...")
    df_csv = spark.read.option("header", "true").csv(csv_path)
    
    # Guardar en Bronze (Parquet para eficiencia, o podría ser copia raw)
    # Aquí guardamos como parquet para facilitar la lectura en Silver
    output_csv_bronze = os.path.join(BRONZE_DIR, "clientes_info")
    df_csv.write.mode("overwrite").parquet(output_csv_bronze)
    print(f" -> Guardado en {output_csv_bronze}")
else:
    print(f"ERROR: No se encontró {csv_path}")

# 2. Ingesta de clientes_extra.txt
# --------------------------------
# Formato: 1, APP, XMOR34, 2025-01-06 (CSV sin header)
txt_path = os.path.join(RAW_DATA_DIR, "clientes_extra.txt")
if os.path.exists(txt_path):
    print(f"Procesando {txt_path}...")
    # Definimos esquema manual ya que no tiene header
    schema_txt = StructType([
        StructField("codigo_cliente", IntegerType(), True),
        StructField("canal_registro", StringType(), True),
        StructField("codigo_interno", StringType(), True),
        StructField("fecha_registro", StringType(), True) # Leemos como string primero
    ])
    
    df_txt = spark.read.csv(txt_path, schema=schema_txt, sep=",", ignoreLeadingWhiteSpace=True)
    
    output_txt_bronze = os.path.join(BRONZE_DIR, "clientes_extra")
    df_txt.write.mode("overwrite").parquet(output_txt_bronze)
    print(f" -> Guardado en {output_txt_bronze}")
else:
    print(f"ERROR: No se encontró {txt_path}")

# 3. Ingesta de clientes.sql
# --------------------------
# Formato: INSERT INTO clientes VALUES (1, 'Felipe', 'Fuentes', ...);
import csv # Importación local para esta sección específica

sql_path = os.path.join(RAW_DATA_DIR, "clientes.sql")
if os.path.exists(sql_path):
    print(f"Procesando {sql_path}...")
    
    data_rows = []
    with open(sql_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip().startswith("INSERT INTO"):
                # Extraer contenido dentro de los paréntesis VALUES (...)
                match = re.search(r"VALUES \((.*)\);", line)
                if match:
                    content = match.group(1)
                    # Separar por comas, pero cuidado con strings.
                    parts = [p.strip().strip("'") for p in content.split(",")]
                    
                    if len(parts) == 7:
                        data_rows.append(tuple(parts))

    # --- CORRECCIÓN CRÍTICA PARA WINDOWS / PYTHON 3.13 ---
    # spark.createDataFrame() falla con sockets en Python 3.13 en Windows.
    # WORKAROUND: Guardamos datos parseados a CSV temporal y leemos con el lector nativo de Spark (JVM).
    
    temp_csv_path = os.path.join(BRONZE_DIR, "temp_clientes_sql.csv")
    
    try:
        # 1. Escribir CSV temporal con Python puro
        with open(temp_csv_path, 'w', newline='', encoding='utf-8') as f_temp:
            writer = csv.writer(f_temp)
            # Escribimos header para facilitar la lectura
            writer.writerow(["codigo", "nombre", "apellido", "comuna", "rut", "fecha_nacimiento", "religion"])
            writer.writerows(data_rows)
        
        # 2. Leer CSV con Spark (Proceso JVM seguro)
        df_sql = spark.read.option("header", "true").csv(temp_csv_path)
        
        # 3. Guardar en formato final (Parquet)
        output_sql_bronze = os.path.join(BRONZE_DIR, "clientes_sql")
        df_sql.write.mode("overwrite").parquet(output_sql_bronze)
        print(f" -> Guardado en {output_sql_bronze}")
        
    except Exception as e:
        print(f"ERROR procesando SQL: {e}")
    finally:
        # 4. Limpieza
        if os.path.exists(temp_csv_path):
            try:
                os.remove(temp_csv_path)
            except:
                pass
else:
    print(f"ERROR: No se encontró {sql_path}")


# ==========================================
# ÉPICA 3: CAPA SILVER - LIMPIEZA (Member 2)
# ==========================================
print("\n--- [SILVER] Iniciando Limpieza y Transformación ---")

# Tarea 3.1: Leer datos de Bronze
try:
    df_bronze_info = spark.read.parquet(os.path.join(BRONZE_DIR, "clientes_info"))
    df_bronze_extra = spark.read.parquet(os.path.join(BRONZE_DIR, "clientes_extra"))
    df_bronze_sql = spark.read.parquet(os.path.join(BRONZE_DIR, "clientes_sql"))
except Exception as e:
    print(f"Error leyendo Bronze: {e}")
    spark.stop()
    exit(1)

# Unificar DataFrames (Join)
# Clave común: codigo / codigo_cliente
# Convertimos codigo a entero para asegurar el join
df_bronze_info = df_bronze_info.withColumn("codigo_cliente", col("codigo_cliente").cast(IntegerType()))
df_bronze_extra = df_bronze_extra.withColumn("codigo_cliente", col("codigo_cliente").cast(IntegerType()))
df_bronze_sql = df_bronze_sql.withColumn("codigo", col("codigo").cast(IntegerType()))

print("Unificando datasets...")
df_master = df_bronze_sql.alias("sql") \
    .join(df_bronze_info.alias("info"), col("sql.codigo") == col("info.codigo_cliente"), "left") \
    .join(df_bronze_extra.alias("extra"), col("sql.codigo") == col("extra.codigo_cliente"), "left") \
    .select(
        col("sql.codigo").alias("id_cliente"),
        col("sql.nombre"),
        col("sql.apellido"),
        col("sql.rut"),
        col("sql.comuna"),
        col("sql.fecha_nacimiento"),
        col("sql.religion"),
        col("info.tipo_cliente"),
        col("info.promedio_compras"),
        col("info.tipo_alimentacion"),
        col("info.tiempo_permanencia_min"),
        col("extra.canal_registro"),
        col("extra.fecha_registro")
    )

# Tarea 3.2: Normalización de Strings
# - Trim espacios
# - Nombres/Apellidos/Comuna en Capitalize (InitCap)
# - Otros textos en minúscula para estandarizar (religion, tipo_alimentacion)
print("Aplicando normalización de strings...")

df_silver = df_master.withColumn("nombre", initcap(trim(col("nombre")))) \
                     .withColumn("apellido", initcap(trim(col("apellido")))) \
                     .withColumn("comuna", initcap(trim(col("comuna")))) \
                     .withColumn("religion", lower(trim(col("religion")))) \
                     .withColumn("tipo_alimentacion", lower(trim(col("tipo_alimentacion")))) \
                     .withColumn("canal_registro", lower(trim(col("canal_registro")))) \
                     .withColumn("rut", trim(col("rut"))) # Solo trim al RUT

# Tarea 3.3: Estandarizar fechas
# Convertir a formato fecha real (YYYY-MM-DD)
print("Estandarizando fechas...")
df_silver = df_silver.withColumn("fecha_nacimiento", to_date(col("fecha_nacimiento"), "yyyy-MM-dd")) \
                     .withColumn("fecha_registro", to_date(col("fecha_registro"), "yyyy-MM-dd"))

# Tarea 3.4: Manejo de Nulos
# Reglas simples: Strings -> "sin_dato", Numericos -> 0
print("Manejando valores nulos...")
df_silver = df_silver.fillna({
    "tipo_alimentacion": "sin_dato",
    "religion": "sin_dato",
    "canal_registro": "sin_dato",
    "promedio_compras": 0,
    "tiempo_permanencia_min": 0
})

# Tarea 3.5: Escribir dataframe resultante en /silver/ventas/
output_silver_path = os.path.join(SILVER_DIR, "clientes_consolidado")
print(f"Escribiendo resultado final en {output_silver_path}...")

df_silver.write.mode("overwrite").parquet(output_silver_path)

# Mostrar muestra del resultado
print("\n--- Muestra de Datos Silver ---")
df_silver.show(5)
df_silver.printSchema()

print(f"PROCESO COMPLETADO EXITOSAMENTE.")
print(f"Datos Silver disponibles en: {output_silver_path}")

spark.stop()
