package com.yatish.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class S4_RDD_Source_File {

	public static void main(String[] args) {
		
		
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/*
		 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! IMPORTANT !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		 * In the interview if they ask whether to use '.parallelize()' to create RDD in your current project then say NO. because, in production products will be using huge amount of data(GB/TBs), 
		 * which cannot fit into a variable like List/Set/Map Variable in JVM. '.parallelize()' will is used for only creating RDDs from a variable like List/Set/Map Variable in JVM. Hence say we 
		 * create RDDs directly from streams/Cassandara/files(HDFS/S3) etc.
		 * 
		 * 
		 * In the below code, we are reading the input data from a file. '.textFile()' method provided by spark automatically parses the file and returns RDDs of type 'List<String>' based on '\n' 
		 * as the delimiter. user need not manually convert it into RDDs using '.parallelize()'. 
		 * syntax: sc.textFile(String <File_Path>);
		 * 
		 * In the following code, 'lines' will have [Hi Bro whats up, i am getting this, from someone]
		 * Each line is treated as a RDD.
		 * 
		 */
		JavaRDD<String> lines = sc.textFile("src/main/resources/data.txt");
		
		
		/*
		 * In '.textFile' method, we can also specify the minimum partition. it specifies the minimum number of partitions that should be created. partitions means the number of RDDs. as shown
		 * in the below code.
		 * syntax: sc.textFile(String <File_Path>, int minNumberOfPartition);
		 * 
		 * NOTE: Do you think, if we give minNumberOfPartions as 5, it will create 5 Partitions/RDD's even though we have only 3 lines in the file 'data.txt'?
		 * 			No!!! it actually depends, spark internally calls hadoop method 'InputFormat.getSplits()' to get the number of partition/RDD's. the parameter 'minNumberOfPartitions' will be sent
		 * 			to it. it may consider it or not based on its implementation.
		 * 
		 */
		JavaRDD<String> lines1 = sc.textFile("src/main/resources/data.txt",5);
		
		
		/*
		 * So consider, the data are present in different files. Should we write '.text()' method for each file?
		 * No!!!  spark also provides one more function 'wholeTextFiles', this will read all the files present in the path and returns the 'JavaPairRDD'.
		 * syntax:  sc.wholeTextFiles(<Directory_Path>);
		 * 
		 * In the directory 'src/main/resources/' there are 2 text files, 'data.txt' and 'data1.txt'. the value returned to 'allFileLines' is as follows,
		 * [file:/Users/yatcs/yatish/spark/Workspace-sparkProject/spark/src/main/resources/data.txt, file:/Users/yatcs/yatish/spark/Workspace-sparkProject/spark/src/main/resources/data1.txt]
		 * actually, file path acts as key and the contents act as value. if we print them seperately it will be as shown below(the new line charecters also will be part of the content),
		 * data.txt -->	(file:/Users/yatcs/yatish/spark/Workspace-sparkProject/spark/src/main/resources/data.txt,Hi Bro whats up
							i am getting this
							from someone)
		   data1.txt --> (file:/Users/yatcs/yatish/spark/Workspace-sparkProject/spark/src/main/resources/data1.txt,This is the other 
							Text File bro)	
		 * 
		 * NOTE: 'minNumberOfPartition' is supported on '.wholeTextFiles' as well.
		 */
		JavaPairRDD<String,String> allFileLines = sc.wholeTextFiles("src/main/resources/");
		
		
		
		
		System.out.println(lines.collect());
		lines.collect().forEach(a -> System.out.println("-----" +a));
		//JavaPairRDD<String,String> allFileLines = sc.wholeTextFiles("src/main/resources/");
		System.out.println(allFileLines.keys().collect());
		allFileLines.collect().forEach((a) -> System.out.println("-----" +a));
		
	}

}
