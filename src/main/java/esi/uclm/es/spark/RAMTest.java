package esi.uclm.es.spark;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.RDDInfo;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.storage.StorageStatus;
import org.apache.spark.util.SizeEstimator;

public class RAMTest {
	
	/* Colores para la salida por terminal */
	public static final String ANSI_RESET = "\u001B[0m";
	public static final String ANSI_YELLOW = "\u001B[33m";
	
	public static final String INFO = String.format("%s%s %s", ANSI_YELLOW, "[INFO]" , ANSI_RESET);
	
	public static void putInfoMessage(String message) {
		System.out.println(INFO+"------------------------------------------------------------------------");
		System.out.println(INFO + message);
		System.out.println(INFO+"------------------------------------------------------------------------");
	}
	
	public static void main(String[] args) {
		putInfoMessage("STARTING EXECUTION");
		
		/* Número de iteraciones pasadas por argumento (por defecto 1.000.000). */
		int iterations = (args.length >= 1) ? Integer.parseInt(args[0]) : 1000000;
		int multiply = (args.length == 2) ? Integer.parseInt(args[1]) : 1;
		
		SparkConf conf = new SparkConf().
				setAppName("RAM Test").
				setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		ArrayList<Long> randomListNumbers = new ArrayList<>();
		
		try 
		{
			for (long i=0; i < iterations * multiply; i++)
			{
				randomListNumbers.add(i);
			}
			
			JavaRDD<Long> numsRDD = sc.parallelize(randomListNumbers, 5);
			randomListNumbers.clear();
			numsRDD.cache(); // cache() is lazy operation
			//numsRDD.count();
			float size = SizeEstimator.estimate(numsRDD);
			// Bytes => Gbytese
			size = size / 1000 / 1000 / 1000;
			
			putInfoMessage("Size is: " + size + " Gb.");
			
		} catch (Exception e) 
		{
			System.out.println("[ERROR] " + e.toString());
			
		} finally 
		{
			sc.stop();
		}
	
	}

}
