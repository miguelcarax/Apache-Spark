# Apache spark
Aplicaciones escritas en Java para el testeo de un cluster con Apache Spark.

## Compilación
#### Dependencias
La gestión de las dependencias se realiza a través de Maven (mirar archivo POM.xml)
<ul>
    ··*Java 1.8  - para soportar **lambda** functions </li>
    <li>Scala 2.11</li>
    <li>Apache Spark 2.1.0</li>
</ul>
Para compilar ejecutar el siguiente comando desde la raiz
```
mvn clean package
```
## Ejecución
Desde la carpeta raíz
<pre><code>$ spark-submit --class "esi.uclm.es.spark.CLASS_NAME" target/OUTPUT_FILE.jar
