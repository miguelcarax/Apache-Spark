# Apache spark
Aplicaciones escritas en Java para el testeo de un cluster con Apache Spark.

## Compilación
#### Dependencias
La gestión de las dependencias se realiza a través de Maven (mirar archivo POM.xml)
<<<<<<< HEAD

+ Java 1.8  - para soportar **lambda functions**
+ Scala 2.11
+ Spark 2.1.0

Podemos compilar mediante maven: `$ mvn clean package`

## Ejecución
Desde la carpeta raíz ejecutamos `spark-submit --class "esi.uclm.es.spark.CLASS_NAME" target/OUTPUT_FILE.jar`
