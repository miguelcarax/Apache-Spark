/*
 * Lectura importante para entender el manejo de memoria en Apache Spark:
 *  -> https://0x0fff.com/spark-memory-management/
 */

package esi.uclm.es.spark;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.SizeEstimator;

public class RAMTest {
	
	/* Colores para la salida por terminal */
	public static final String ANSI_RESET = "\u001B[0m";
	public static final String ANSI_YELLOW = "\u001B[33m";
	
	/*
	 * Imprime mensajes por terminal con colores.
	 */
	public static void putInfoMessage(String... messages) {
		System.out.println(ANSI_YELLOW + "[INFO] " + 
				 "------------------------------------------------------------------------"
				+ ANSI_RESET);
		for (String message : messages) 
		{
			System.out.println(ANSI_YELLOW + "[INFO] "+ ANSI_RESET +  message);
		}
		System.out.println(ANSI_YELLOW + "[INFO] " + 
				"------------------------------------------------------------------------"
				+ ANSI_RESET);
	}
	
	public static void main(String[] args) {
		putInfoMessage("STARTING EXECUTION");
		
		/*
		 * Creamos RDD intermedios y los vamos añadiendo al global para no colapsar la memoria creando
		 * la lista de  Longs de Java, ya que no queremos que Java ocupe la memoria que necesita Spark.
		 */
		
		/* Número de elementos de cada RDD (en millones) pasados por argumentos (por defecto 1.000.000). */
		int elementsPerRDD = (args.length >= 1) ? Integer.parseInt(args[0]) * 1000000 : 1000000;
		/* Cuantas veces se va a añadir ese RDD al RDD final. */
		int RDDs = (args.length > 1) ? Integer.parseInt(args[1]) : 1;
		
		SparkConf conf = new SparkConf().
				setAppName("RAM Test").
				setMaster("local[4]"); // Lo ejecutamos en local para la prueba con 4 cores
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		ArrayList<Long> randomListNumbers = new ArrayList<>();
		JavaRDD<Long> auxRDD = sc.emptyRDD();
		JavaRDD<Long> finalRDD = sc.emptyRDD();
		
		try 
		{
			for (long i=0; i < RDDs; i++)
			{	
				for (long k=0; k < elementsPerRDD; k++) 
				{
					randomListNumbers.add(k);
				}
				
				auxRDD = sc.parallelize(randomListNumbers);
				finalRDD = finalRDD.union(auxRDD);
				
				/* Le dice a Spark que si no cabe en la memoria que lo guarde en el disco */
				finalRDD.persist(StorageLevel.MEMORY_AND_DISK());
				finalRDD.count();
				
				/* Clean aux structures */
				auxRDD.unpersist(); // Lo libera de la memoria
				auxRDD = sc.emptyRDD();
				randomListNumbers.clear();
			}
			putInfoMessage(String.format("RDD has %d Mb size.",  SizeEstimator.estimate(finalRDD) / 1000000),
					String.format("RDD has %d million elements.", finalRDD.count() / 1000000 ));
			
		} catch (Exception e) 
		{
			putInfoMessage(e.toString());
			
		} finally 
		{
			sc.stop();
		}
	
	}

}
