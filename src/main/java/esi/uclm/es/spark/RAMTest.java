package esi.uclm.es.spark;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.RDDInfo;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.storage.StorageStatus;

public class RAMTest {

	public static void main(String[] args) {
		/* Número de iteraciones pasadas por argumento (por defecto 1.000.000). */
		int iteraciones = (args.length == 1) ? Integer.parseInt(args[0]) : 1000000;
		
		SparkConf conf = new SparkConf().
				setAppName("RAM Test").
				setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		ArrayList<Long> listNumAleatorios = new ArrayList<>();
		
		try 
		{
			for (long i=0; i < iteraciones; i++)
			{
				listNumAleatorios.add(i);
			}
			
			JavaRDD<Long> numsRDD = sc.parallelize(listNumAleatorios);
			numsRDD.cache(); // cache() is lazy operation
			numsRDD.count();
			
			System.out.println("--------------------------------------------------------------------");
			System.out.println(sc.sc().getRDDStorageInfo()[0]);
			System.out.println("--------------------------------------------------------------------");
			
		} catch (Exception e) 
		{
			System.out.println("[ERROR] " + e.toString());
			
		} finally 
		{
			sc.stop();
		}
	
	}

}
