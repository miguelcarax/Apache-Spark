package esi.uclm.es.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Ordering {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Num Ordering").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> raw = jsc.textFile("numbers.dat");
		//JavaRDD<Long> numbers = raw.flatMap(line -> Arrays.asList(Long.parseLong(line)));
		JavaPairRDD<Long, Integer> numbers = raw.flatMap(line -> Arrays.asList(Long.parseLong(line)))
				.mapToPair(number -> new Tuple2<Long, Integer>(number, 1))
				.reduceByKey((a, b) -> a + b);
		numbers.count();
		
		System.out.println("[INFO] " + jsc.sc().getRDDStorageInfo()[0]);
		
		jsc.stop();
	}

}
