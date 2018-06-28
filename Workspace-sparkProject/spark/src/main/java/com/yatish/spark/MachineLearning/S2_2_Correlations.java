package com.yatish.spark.MachineLearning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;

public class S2_2_Correlations {

	public static void main(String[] args) {
		
		SparkConf sc = new SparkConf().setAppName("machine_learning_correlations").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		List<Double> series1 = Arrays.asList(1.0,2.0);
		List<Double> series2 = Arrays.asList(1.0,5.0);
		
		JavaDoubleRDD doubleRDDseries1 = jsc.parallelizeDoubles(series1);
		JavaDoubleRDD doubleRDDseries2 = jsc.parallelizeDoubles(series2);
		
		Double correlation = Statistics.corr(doubleRDDseries1.srdd(),doubleRDDseries2.srdd(),"pearson");
		
		System.out.println(correlation);
		
		List<Vector> vectorList = new ArrayList<Vector>();
		Vector dv = Vectors.dense(1.0,2.0);
		Vector dv1 = Vectors.dense(1.0,2.0);
		
		JavaRDD<Vector> vectorRDD = jsc.parallelize(vectorList);
		
		Matrix matrix = Statistics.corr(vectorRDD.rdd());
		
		

	}

}
