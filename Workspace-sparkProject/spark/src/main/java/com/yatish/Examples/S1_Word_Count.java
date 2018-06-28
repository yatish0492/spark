package com.yatish.Examples;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class S1_Word_Count {

	public static void main(String[] args) {
		
		SparkConf sc = new SparkConf().setAppName("word count").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		JavaRDD<String> lines = jsc.textFile("src/main/resources/data.txt");
		
		JavaRDD<String> eachword = lines.flatMap(line -> {
			String[] arr = line.split(" ");
			return Arrays.asList(arr).iterator();
		});
		
		JavaPairRDD<String,Integer> wordPair = eachword.mapToPair(word -> {
			return new Tuple2<String,Integer>(word,1);
		});
		
		JavaPairRDD<String,Integer> finalWordCount = wordPair.reduceByKey((word1count,word2count) -> {
			return word1count+word2count;
		});
		

	}

}
