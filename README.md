# Prueba Técnica - Ceiba Software

**Descripción:** Solución de Data Engineering para procesamiento y análisis de datos financieros de FinTrust usando Google BigQuery.

---

## 📋 Tabla de Contenidos

- [Instalación](#-instalación-del-proyecto)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Archivos Python](#-archivos-python)
- [Configuración](#-configuración)

---

## 🚀 Instalación del Proyecto

### Requisitos Previos

- **Python 3.14+**
- **UV** (gestor de paquetes moderno)
- Cuenta de Google Cloud con acceso a BigQuery

### Instalación con UV

### 1. Instalar `uv`
 
**macOS / Linux:**
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
 
**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
 
> Después de instalarlo, reinicia tu terminal para que el comando `uv` quede disponible.
 
---
 
#### 2. Clonar el repositorio
 
```bash
git clone https://github.com/tu-usuario/tu-repositorio.git
cd tu-repositorio
```
 
---
 
#### 3. Crear el entorno virtual e instalar dependencias
 
Con `uv`, un solo comando crea el entorno virtual e instala todas las dependencias declaradas en `pyproject.toml`:
 
```bash
uv sync
```
 
Esto genera una carpeta `.venv/` en la raíz del proyecto. No necesitas activar el entorno manualmente para ejecutar el código 
 
---
 
#### 4. Ejecutar el proyecto
 
Usa `uv run` para correr cualquier script dentro del entorno virtual sin necesidad de activarlo:
 
```bash
uv run .\python\validaciones.py
```

#### 5. Configurar credenciales de Google Cloud
bash
# Autenticarse con Google Cloud
gcloud auth application-default login

# Establecer el proyecto por defecto
gcloud config set project prueba-tecnica-ceiba-software

## 📁 Estructura del Proyecto
```bash
Prueba-Tecnica---Ceiba-Software/
│
├── python/                          # Scripts Python del pipeline
│   ├── pipeline.py                 # Orquestador principal del ETL
│   └── validaciones.py             # Controles de calidad de datos
│
├── sql/                            # Scripts SQL organizados por capas
│   ├── 01-raw/                    # Capa Raw - Datos originales
│   │   ├── create_raw_tables.sql
│   │   ├── load_data.sql
│   │   └── new_records.sql
│   │
│   ├── 02-staging/                # Capa Staging - Datos limpiados
│   │   ├── create_stg_schema.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_installments.sql
│   │   ├── stg_loans.sql
│   │   └── stg_payments.sql
│   │
│   ├── 03-analitycs/              # Capa Analytics - Datos analíticos
│   │   ├── create_anly_schema.sql
│   │   └── anly_dm_fintrust_performance.sql
│   │
│   └── 04-queries-negocio/        # Queries específicas del negocio
│       ├── q01_desembolso_diario.sql
│       ├── q02_recaudo_diario.sql
│       ├── q03_cartera_por_cohorte.sql
│       └── q04_top_atraso.sql
│
├── docs/                          # Documentación y reportes
│   ├── decisiones-tecnicas.md    # Decisiones arquitectónicas
│   ├── evidencia-calidad-datos.md
│   └── quality_report_*.xlsx      # Reportes generados
│
├── bonus/                         # Bonus llm
│   └── llm_proposal.md
│
├── pyproject.toml                # Configuración de dependencias (UV)
├── uv.lock                       # Lock file de dependencias
├── .python-version               # Versión de Python (3.14)
├── .gitignore                    # Archivos ignorados por Git
└── README.md                     # Este archivo
```

## 🐍 Descripción de Archivos Python
### `python/pipeline.py`

**Propósito:** Orquestador principal del pipeline ETL que automatiza todo el flujo de datos.

**Funciones clave:**

| Función | Descripción |
|---------|-------------|
| `dataset_existe(dataset_id)` | Valida si un dataset existe en BigQuery y retorna sus tablas |
| `ejecutar_querys(carpeta, list_sql)` | Ejecuta scripts SQL desde una carpeta específica |
| `cargue_incremental(target_table, carpeta)` | Realiza cargue incremental usando operaciones MERGE SQL |
| `main()` | Orquesta el flujo completo: Raw → Staging → Analytics |

**Flujo de ejecución:**
1. Verifica existencia de datasets (raw, staging, analytics)
2. Si no existen, crea las tablas base y carga datos iniciales
3. Realiza transformaciones a capa staging con MERGE
4. Crea modelos analytics para BI
5. En ejecuciones posteriores, inserta registros nuevos incrementalmente

**Uso:**
```bash
python python/pipeline.py
```
---

### `python/validaciones.py`

**Propósito:** Implementa controles de calidad de datos y genera reportes de integridad.

**Funciones clave:**

| Función | Descripción |
|---------|-------------|
| `run_query(sql)` | Ejecuta una consulta SQL y retorna resultados como lista de diccionarios |
| `get_tables(dataset)` | Lista todas las tablas de un dataset |
| `get_pk(dataset, table)` | Obtiene la clave primaria de una tabla |
| `get_required_cols(dataset, table)` | Obtiene columnas requeridas (NOT NULL) |
| `check_nulos(dataset, table)` | Valida que columnas requeridas no tengan nulos |
| `check_duplicados(dataset, table)` | Verifica duplicados basados en clave primaria |
| `check_conteo_capas(table)` | Valida consistencia de conteos entre capas raw y staging |
| `ejecutar_controles()` | Ejecuta todos los controles y exporta reporte a Excel |

**Controles ejecutados:**
- ✅ **Nulos**: Verifica ausencia de valores NULL en columnas requeridas
- ✅ **Duplicados**: Detecta PKs duplicadas que violarían integridad
- ✅ **Conteo entre capas**: Valida que raw y staging tengan misma cantidad de registros

**Salida:**
- Excel con dos pestañas: "Todos" (todos los controles) y "FAILS" (solo fallos)
- Ruta: `docs/quality_report_YYYYMMDD_HHMMSS.xlsx`

**Uso:**
```bash
python python/validaciones.py
```
---
## 📊 Estructura de Capas de Datos

### 🟡 Capa RAW (01-raw/)
**Características:**
- Datos sin transformar, tal como vienen de la fuente
- Mantiene historial completo de cambios
- Estructura: customers, installments, loans, payments

**Archivos SQL:**
- `create_raw_tables.sql` - DDL para crear tablas
- `load_data.sql` - DML para carga inicial con datos completos
- `new_records.sql` - Inserción de nuevos registros (incremental)

---

### 🟠 Capa STAGING (02-staging/)
**Características:**
- Datos limpios, validados y estandarizados
- Transformaciones básicas y enriquecimiento
- Prefijo `stg_` en nombres de tabla

**Tablas staging:**
- `stg_customers` - Clientes con datos normalizados
- `stg_loans` - Préstamos con cálculos de interés
- `stg_payments` - Pagos con auditoría temporal
- `stg_installments` - Cuotas procesadas

**Archivo principal:**
- `create_stg_schema.sql` - Crea todas las tablas de staging con transformaciones

---

### 🔵 Capa ANALYTICS (03-analitycs/)
**Características:**
- Modelos optimizados para análisis e BI
- Enfoque en desempeño de consultas

**Modelos:**
- `anly_dm_fintrust_performance` - Data Mart con métricas de desempeño
  - Tasas de pago
  - Atrasos
  - Morosidad
  - Performance general del portafolio

---

### 💼 Queries de Negocio (04-queries-negocio/)
**Propósito:** Análisis específicos para toma de decisiones

| Query | Descripción |
|-------|-------------|
| `q01_desembolso_diario.sql` | Desembolsos por día (volumen, montos) |
| `q02_recaudo_diario.sql` | Recaudos por día (por tipo, cumplimiento) |
| `q03_cartera_por_cohorte.sql` | Cartera segmentada por cohorte de originación |
| `q04_top_atraso.sql` | Top de clientes con mayor atraso |

---
## 🔧 Configuración (Personalización)

En `python/pipeline.py`, modifica estas constantes para tu entorno:

```python
PROJECT_ID = 'prueba-tecnica-ceiba-software'  # Tu proyecto GCP
RAW_DATASET_ID = 'raw_fintrust'               # Nombre dataset raw
STAGING_DATASET_ID = 'stg_fintrust'           # Nombre dataset staging
ANLY_DATASET_ID = 'analytics_fintrust'        # Nombre dataset analytics
```

En `python/validaciones.py`, modifica:

```python
PROJECT_ID = 'prueba-tecnica-ceiba-software'
OUTPUT_DIR = "docs"  # Carpeta de salida de reportes
```

---
