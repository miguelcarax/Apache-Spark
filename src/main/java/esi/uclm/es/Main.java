package esi.uclm.es;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Main
{
    public static void main( String [] args )
    {
        SparkConf conf = new SparkConf().setAppName("RAM test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("/home/miguel/git/Apache-Spark/target/apache-spark-1.0-SNAPSHOT.jar");
        Random randomGenerator = new Random();
        
        for(int i=0; i< 1000; i++) 
        {
        	List<Integer> data = Arrays.asList(randomGenerator.nextInt(100), randomGenerator.nextInt(100), randomGenerator.nextInt(100), randomGenerator.nextInt(100));
            JavaRDD<Integer> distData = sc.parallelize(data);
            distData.persist(StorageLevel.MEMORY_ONLY());
        }
        
        sc.stop();
    }
}