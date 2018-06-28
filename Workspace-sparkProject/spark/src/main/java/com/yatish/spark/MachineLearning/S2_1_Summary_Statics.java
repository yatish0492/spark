package com.yatish.spark.MachineLearning;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

public class S2_1_Summary_Statics {

	public static void main(String[] args) {
		
		SparkConf sc = new SparkConf().setAppName("SummaryStatics").setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(sc);
		
		List<Vector> vectorList = new ArrayList<Vector>();
		Vector dv = Vectors.dense(1.0,2.0,3.0,4.0);
		Vector dv1 = Vectors.dense(5.0,6.0,7.0,8.0);
		vectorList.add(dv);
		vectorList.add(dv1);
		
		JavaRDD<Vector> vectorRDD = jsc.parallelize(vectorList);
		
		MultivariateStatisticalSummary summary = Statistics.colStats(vectorRDD.rdd());
		
		System.out.println("row/vectors count : " + summary.count());
		System.out.println("Max in column : " + summary.max());
		System.out.println("Min in column : " + summary.min());
		System.out.println("Mean in column : " + summary.mean());
		System.out.println("number of non-zeros in column : " + summary.numNonzeros());
		System.out.println("Variance in column : " + summary.variance());
		

	}

}
