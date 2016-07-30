package esi.uclm.es.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.storage.StorageStatus;

import scala.Tuple2; // for Tuple2 class

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.spark.SparkConf;


public class WordCount
{
    public static void main( String [] args )
    {
    	String master = "local";
    	String appName = "local RAM test";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        /* Accedemos al archivo */
        JavaRDD<String> textFile = sc.textFile("bible.txt");
        
        /* Sacamos las palabras del archivo*/
        /*JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
        	public Iterable<String> call(String s) { return Arrays.asList(s.split(" "));}
        });*/
        JavaRDD<String> words = textFile.flatMap(s -> Arrays.asList(s.split(" ")));
        
        /* Crearemos pares (palabra, valor) */
        JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
        
        /* Contamos cuantas veces está cada palabra */
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

       
        /* Guardamos en memoria */
        counts.persist(StorageLevel.MEMORY_ONLY());
        
        sc.close();
    }
}