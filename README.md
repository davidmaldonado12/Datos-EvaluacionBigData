# 🛒 Pipeline ETL - Proyecto Lidl (Equipo A)

Este repositorio contiene la implementación de un proceso **ETL (Extracción, Transformación y Carga)** automatizado en Python utilizando **PySpark**. El script principal, `etl_lidl_team_a.py`, está optimizado específicamente para entornos **Windows**, manejando la ingesta, limpieza y consolidación de datos de clientes.

---

## 📊 Diagrama Workflow y Arquitectura

Los siguientes esquemas ilustran la arquitectura del pipeline, desde la configuración del entorno hasta la carga final en la capa Silver.

<div align="center">
  <img src="https://github.com/user-attachments/assets/95fff1af-71a7-4a2c-8675-2987ca0b5550" alt="Workflow Diagram" width="90%">
</div>

<div align="center">
  <img width="3091" height="1763" alt="Untitled diagram-2025-11-28-233736" src="https://github.com/user-attachments/assets/787d5abe-02d1-4613-b34a-0eefe0fd454b" />
</div>

---

## 🛠️ Descripción del Workflow

A continuación se detalla el funcionamiento técnico de cada etapa del script:

### 1. Configuración del Entorno (Windows Optimization)
El script prepara automáticamente el entorno de ejecución para mitigar errores comunes de Hadoop y Spark en Windows:
* **Variables de Entorno:** Configura dinámicamente `JAVA_HOME` (Java 17) y `HADOOP_HOME`.
* **Fix de Sockets (WinError 10038):** Establece `PYSPARK_PYTHON_WORKER_REUSE=0`, crucial para evitar fallos de conexión en versiones recientes de Python.
* **Inicialización:** Crea una `SparkSession` local bajo el nombre "Lidl_ETL_Team_A".

### 2. Extracción de Datos
El sistema asegura la disponibilidad de los datos fuente mediante una lógica de redundancia:
1.  Verifica la existencia del directorio `lidl_project_source`.
2.  Si no existe, intenta clonar el repositorio oficial desde **GitHub**.
3.  **Fallback:** Si la clonación falla, utiliza los archivos locales como respaldo.

### 3. Capa Bronze (Ingesta Raw)
Se procesan archivos de diversos formatos y se estandarizan a **Parquet** en `bronze/ventas/`.

| Archivo Fuente | Formato | Estrategia de Procesamiento |
| :--- | :--- | :--- |
| **`clientes_info.csv`** | CSV | Lectura estándar con inferencia de cabeceras. |
| **`clientes_extra.txt`** | TXT | Lectura sin header + Aplicación de esquema manual (`StructType`). |
| **`clientes.sql`** | SQL | **Parsing Avanzado:** Extracción de valores `INSERT` vía Regex.<br>**Workaround:** Escritura intermedia a CSV temporal para evitar conflictos de memoria JVM en Windows. |

### 4. Capa Silver (Transformación y Limpieza)
Generación del dataset maestro consolidado mediante las siguientes reglas de negocio:

* 🔗 **Unificación:** Join de los tres datasets usando `codigo_cliente` como llave primaria.
* 📝 **Normalización de Texto:**
    * `Trim`: Eliminación de espacios excedentes.
    * `InitCap`: Formato de Título para *Nombres*, *Apellidos* y *Comunas*.
    * `Lower`: Minúsculas para *Religión*, *Alimentación* y *Canales*.
* 📅 **Estandarización Temporal:** Conversión de strings a objetos `Date` (formato `yyyy-MM-dd`).
* 🚫 **Manejo de Nulos:**
    * Campos de texto $\rightarrow$ `"sin_dato"`
    * Campos numéricos $\rightarrow$ `0`

### 5. Carga Final
El resultado limpio y unificado se escribe en formato **Parquet** en la ruta:
> `silver/ventas/clientes_consolidado`

---

## 📋 Gestión del Proyecto

El seguimiento de tareas y evolutivos del desarrollo se realizó mediante un tablero Kanban en Azure DevOps.

<div align="center">
  <img src="https://github.com/user-attachments/assets/32e7144f-22e9-402e-9e71-93e0318d8ed2" alt="Azure Kanban Board" width="60%">
</div>
