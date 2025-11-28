🛒 ETL Pipeline - Lidl Data Project (Team A)
Este repositorio contiene un flujo de trabajo ETL (Extract, Transform, Load) automatizado, desarrollado en Python utilizando PySpark. El script está optimizado específicamente para ejecutarse en entornos Windows, manejando la ingesta, limpieza y consolidación de datos de ventas y clientes.

🚀 Resumen del Workflow
El archivo principal etl_lidl_team_a.py orquesta las siguientes etapas:

🛠️ Configuración del Entorno: Ajuste automático de variables de entorno (Java/Hadoop) y mitigación de errores de sockets en Windows.

📥 Extracción: Clonación automática del repositorio de datos o uso de respaldo local.

🥉 Capa Bronze (Raw): Ingesta de múltiples formatos (.csv, .txt, .sql) y conversión a Parquet.

🥈 Capa Silver (Curated): Unificación de datasets, normalización de strings, casteo de fechas y manejo de nulos.

📤 Carga: Escritura del dataset maestro consolidado.

📖 Documentación Técnica Detallada
1. Configuración del Entorno (Windows Optimization)
El script prepara el entorno de ejecución para evitar conflictos comunes en Windows:

Configura JAVA_HOME (Java 17) y HADOOP_HOME dinámicamente.

Fix Crítico: Establece PYSPARK_PYTHON_WORKER_REUSE=0 para prevenir el error WinError 10038 (común en Python 3.12+ con Spark).

Inicializa una SparkSession local.

2. Adquisición de Datos
Verifica la existencia del directorio lidl_project_source.

Si no existe, ejecuta un git clone del repositorio fuente.

Fallback: Si falla la red, utiliza los datos locales.

3. Capa Bronze: Ingesta y Normalización de Formatos
Procesamiento de archivos crudos hacia formato Parquet (bronze/ventas/):
Archivo Fuente,Formato,Estrategia de Procesamiento
clientes_info.csv,CSV,Lectura estándar con inferencia de headers.
clientes_extra.txt,TXT,Lectura como CSV sin header + Aplicación de esquema manual (StructType).
clientes.sql,SQL,Parsing Avanzado: Extracción de valores INSERT INTO mediante Regex.  Workaround: Escritura intermedia a CSV temporal para evitar conflictos de memoria JVM/Python en Windows.

4. Capa Silver: Limpieza y TransformaciónGeneración del dataset maestro en silver/ventas/clientes_consolidado:Unificación (Joins): Cruce de los tres dataframes usando codigo_cliente como llave primaria.Normalización de Texto:Trim: Eliminación de espacios en blanco.InitCap: Formato de título para Nombres, Apellidos y Comunas.Lower: Estandarización a minúsculas para metadatos (Religión, Canales).Casteo de Tipos: Conversión de strings a objetos Date (formato yyyy-MM-dd).Manejo de Nulos:Textos $\rightarrow$ "sin_dato"Numéricos $\rightarrow$ 0📋 Gestión del ProyectoEl desarrollo y seguimiento de tareas de este ETL se gestionó mediante un tablero Kanban en Azure DevOps.💻 Requisitos de EjecuciónPython 3.10+Java 17 (JDK)Binarios de Hadoop (winutils)Librerías: pysparkDesarrollado por Team APor qué esta estructura funciona mejor:Jerarquía Visual: Uso de encabezados (#, ##) para separar claramente las secciones.Uso de Iconos: Los emojis (🛠️, 📥, 🥉) ayudan a identificar rápidamente las etapas del proceso sin tener que leer todo el texto.Tabla para la Capa Bronze: La información sobre los tipos de archivos (csv, txt, sql) se lee mucho mejor en una tabla que en una lista de texto plano.Destacados Técnicos: Se hace énfasis en el "Workaround de Windows" y el "Parsing de SQL", lo cual demuestra que el código es robusto y resuelve problemas complejos.Diagramas Integrados: Las imágenes están colocadas estratégicamente: el diagrama de flujo al principio para entender la lógica, y el Kanban al final para mostrar la metodología de trabajo.
