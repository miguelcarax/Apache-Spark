package esi.uclm.es.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2; // for Tuple2 class

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

import org.apache.spark.SparkConf;


public class Main
{
    public static void main( String [] args )
    {
    	String master = "local";
    	String appName = "local RAM test";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        /* some work */
        JavaRDD<String> lines = sc.textFile("data.txt");
        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        
        List<Tuple2<String, Integer>> output = pairs.take(5);
        
        for (Tuple2<String, Integer> element : output)
        {
        	System.out.printf("Palabra: %20s Nº veces: %3x\n", element._1(), element._2());
        }
        
        sc.stop();
    }
}