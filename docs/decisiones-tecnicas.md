# Decisiones Tecnicas

Como primera decision tecnica, se opta por usar un manejador de ambientes vituales llmado UV (https://pypi.org/project/uv/), 
ya que este permite generar de forma rapida un ambiente vitual en python listo para ser manejado, facil de instalar dependencias y evitando problemas de compatibilidad de versiones o problemas de librerias.

La segunda decicion tecnica es usar las credencias por defecto del Google Cloud SDK, esto con el fin de facilitar el acceso a un 
proyecto de prueba generado para la realizacion de esta prueba, y facilitar validaciones e iteraciones con las tablas.

Para la parte de automatizacion, se busco cumplir los requisitos de orquestacion y generacion de ETL y carga incremental en las tablas, 
por ende se genero un archivo pipeline.py que pudiera cumplir con estas necesidades, generando incluso un ejemplo de 
actualizacion e insercion de datos para poder probar la carga incremental, la cual se realiza mediante una sentencia MERGE iterativa en cada tabla, la cual insertara o actualizara nuevos datos que lleguen a la capa raw. Se manejan Try-Excepts para el manejo y monitoreo de errores.

Para la capa analitica, se decidio generar un datamart con la informacion consolidada proveniente desde staging, 
esto para facilidad de analisis de la informacion, reduciendo la necesidad de generar joins en las preguntas de negocio que se hagan.

Las preguntas de negocio fueron generadas como Vistas de Bigquery, lo cual facilita su incorporacion a plataformas de BI como 
Looker o PBI

En cuanto a la validacion de Calidad de Datos, se declara que se uso la IA Claude para que generara un script de validacion de datos
que permitiera revisar todos los datasets generados en el proyecto y aplicara las reglas estipuladas en el archivo "evidencia-calidad-datos.md".
La IA tambien facilito un archivo README.md claro sobre este repositorio y su contenido.