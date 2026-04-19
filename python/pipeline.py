from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os


PROJECT_ID = 'prueba-tecnica-ceiba-software'
CLIENT = bigquery.Client(project=PROJECT_ID)
# Configuración
RAW_DATASET_ID = 'raw_fintrust'
STAGING_DATASET_ID = 'stg_fintrust'
ANLY_DATASET_ID = 'analytics_fintrust'

def dataset_existe(dataset_id):
    """Valida la existencia del dataset, y valida si este tiene tablas creadas"""
    try:
        # Intenta obtener el dataset. Si lo encuentra devuelve una lista de tablas
        CLIENT.get_dataset(dataset_id)
        print(f"El dataset {dataset_id} ya existe.")
        list_crud_tables = list(CLIENT.list_tables(dataset_id))
        list_tables = [table.table_id for table in list_crud_tables]
        return True, list_tables
    except NotFound:
        #Si no lo encuentra, devuelve un false y una lista vacia
        print(f"El dataset {dataset_id} no fue encontrado.")
        return False, []


def ejecutar_querys(carpeta, list_sql):
    """Ejecuta el script SQL de carga completa para una tabla específica."""

    #Revisa la carpeta pasada por parametro
    sql_dir = "./sql/"
    carpeta_path = os.path.join(sql_dir, carpeta)

    if not os.path.exists(carpeta_path):
        print(f"No se encontró el archivo: {carpeta_path}")
        return
    try:
    #Lee la lista de querys, y devuelve una lista con la query ejecutada.    
        list_out_query = [] 
        for query in list_sql:
            file_path = os.path.join(carpeta_path, query)
            with open(file_path, 'r') as f:
                query_script = f.read()

            print(f"Ejecutando consulta {query}")
            CLIENT.query(query_script).result()
            list_out_query.append(query_script)
        return list_out_query
    except Exception as e:
        print(f"Error al ejecutar consulta {query}: {e}")
    

def cargue_incremental(target_table: str, carpeta: str):
    """Realiza el cargue incremetal usando como fuente las querys generadas para staging"""
    target_ref = f"{PROJECT_ID}.{STAGING_DATASET_ID}.{target_table}"

    try:
        target = CLIENT.get_table(target_ref)

        # Extrae las columnas Columnas objetivo
        target_cols = [field.name for field in target.schema]

        #Usa la primer columna como Primary Key.
        pk = target.schema[0].name

        # SET: columnas a actualizar excepto PK y loaded_at
        set_clause = ", ".join([
            f"target.{col} = source.{col}"
            for col in target_cols if col not in (pk, 'loaded_at')
        ])
        if 'loaded_at' in target_cols:
            set_clause += ", target.loaded_at = CURRENT_TIMESTAMP()"

        # INSERT: usar solo columnas comunes, ambos lados con el mismo nombre
        insert_cols = ", ".join(target_cols)
        insert_vals = ", ".join([f"source.{col}" for col in target_cols])

        #SOURCE: extrae la query de la tabla que se va a ejecutar
        list_sql = [f"{target_table}.sql"]
        source = ejecutar_querys(carpeta, list_sql)

        #MERGE: Se arma sentencia merge
        sql = f"""
        MERGE `{target_ref}` AS target
        USING (
            {source[0]}
        ) AS source
        ON target.{pk} = source.{pk} 
        WHEN MATCHED THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        return sql

    except Exception as e:
        print(f"Error en creacion del merge : {e}")
    

def main():

    #Valida por primera vez si existen los datasets. Si no existen, se genera el cargue full
    dataset_raw_existe, tables_raw = dataset_existe(RAW_DATASET_ID)
    dataset_stg_existe, tables_stg = dataset_existe(STAGING_DATASET_ID)
    dataset_anly_existe, tables_anly = dataset_existe(ANLY_DATASET_ID)
    
    print("---VALIDANDO TABLAS RAW---")
    if not tables_raw or dataset_raw_existe == False:
        list_sql_raw = ["create_raw_tables.sql", "load_data.sql"]
        print(f"El dataset {RAW_DATASET_ID} no contiene tablas. Ejecutando carga full")
        ejecutar_querys("01-raw", list_sql_raw)
    else:
        print("Tablas raw ya creadas")
    
    print("---VALIDANDO TABLAS STAGING---")
    if not tables_stg or dataset_stg_existe == False:
        list_sql_stg = ["create_stg_schema.sql"]
        print(f"El dataset {STAGING_DATASET_ID} no contiene tablas. Ejecutando carga full")
        ejecutar_querys("02-staging", list_sql_stg)
        dataset_stg_existe, tables_stg = dataset_existe(STAGING_DATASET_ID)
        creacion_terminada = True
    else:
        creacion_terminada = False
        print("Tablas staging ya creadas")
    

    print("---EJECUCION DE CARGA STAGING INICIAL---")    
    if tables_stg and creacion_terminada:
        print("Realizando cargue de datos a staging")
        try:
            for tabla_stg in tables_stg:
                print(f"Ejecutando merge")
                try:
                    sql_merge = cargue_incremental(
                        target_table=tabla_stg,
                        carpeta="02-staging"
                    )
                    CLIENT.query(sql_merge).result()
                    print(f"Merge completado: {tabla_stg}")
                except Exception as e:
                    raise ValueError(f"Error en ejecucion del merge {tabla_stg}: {e}")
        except Exception as e:
                    print(f"Error en paso del merge : {e}")

    else:
        print("Ya se realizo el cargue inicial")            
    
    #Genera esquema de analitycs para BI
    print("---VALIDANDO TABLAS ANALITYCS---")
    if not tables_anly or dataset_anly_existe == False:
        list_sql_anly = ["create_anly_schema.sql", "anly_dm_fintrust_performance.sql"]
        print(f"El dataset {ANLY_DATASET_ID} no contiene tablas. Ejecutando carga full")
        ejecutar_querys("03-analitycs", list_sql_anly)
    else:
        print("Tablas analytics ya creadas")
    
    #Si es la segunda vez o superior que se ejecuta pipeline.py, este bloque se ejecutara, ingresando nuevos datos 
    #y realizando la carga incremental
    print("---EJECUCION DE CARGA INCREMENTAL---")    
    if tables_anly:
        list_sql_ins = ["new_records.sql"]
        print("Insertando y actualizando datos")
        ejecutar_querys("01-raw", list_sql_ins)
        print("Realizando cargue incremental")
        for tabla_stg in tables_stg:
            print(f"Ejecutando merge")
            try:
                sql_merge = cargue_incremental(
                    target_table=tabla_stg,
                    carpeta="02-staging"
                )
                CLIENT.query(sql_merge).result()
                print(f"Merge completado: {tabla_stg}")
            except Exception as e:
                raise ValueError(f"Error en ejecucion del merge {tabla_stg}: {e}. No se deben insertar registros duplicados.")
    else:
        print("Sin data nueva por cargar")

if __name__ == "__main__":
    main()
