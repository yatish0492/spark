package com.yatish.spark.persistance;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class S3_Types_of_persistence {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);	
		JavaRDD<Integer> rddData = sc.parallelize(data);
		
		JavaRDD<Integer> rddData1 =	rddData.map(a -> {
			System.out.println("Executing the map funtion to create rddData1");
			return a;
		});
		
		
		rddData1.persist(StorageLevel.MEMORY_ONLY());  // store the RDD in JVM heap memory as a object without serializing it.
		rddData1.persist(StorageLevel.MEMORY_ONLY_2()); //same as above but it will replicate the RDD in 2 slave nodes.
		rddData1.persist(StorageLevel.MEMORY_ONLY_SER());  // store the RDD in JVM heap memory, it will serialize the object into bytes and then store it.
		rddData1.persist(StorageLevel.MEMORY_ONLY_SER_2()); //same as above but it will replicate the RDD in 2 slave nodes.
		rddData1.persist(StorageLevel.MEMORY_AND_DISK());	  // store the RDD in JVM heap memory, but if the space is not availale, then it will be stored in the disk as object without serializing it.
		rddData1.persist(StorageLevel.MEMORY_AND_DISK_2()); //same as above but it will replicate the RDD in 2 slave nodes.
		rddData1.persist(StorageLevel.MEMORY_AND_DISK_SER()); // store the RDD in JVM heap memory, but if the space is not availale, then it will be stored in the disk. spark will serialize the object
															// into bytes and store it.
		rddData1.persist(StorageLevel.MEMORY_AND_DISK_SER_2()); //same as above but it will replicate the RDD in 2 slave nodes.
		
		rddData1.persist(StorageLevel.DISK_ONLY());	// store the RDD in the disk(HDD/SDD) as a object without serializing it. there is on 'DISK_ONLY_SER()'
		rddData1.persist(StorageLevel.DISK_ONLY_2()); //same as above but it will replicate the RDD in 2 slave nodes.
		rddData1.persist(StorageLevel.OFF_HEAP()); // This is a experimental API, it will store the RDD in the main memory(RAM) but outside of JVM heap memory. So it is not as fast as 'MEMORY_ONLY'
												  // but faster than HDD/SDD.
		
		
		/*
		 * Why do we need 'SER' why do we need to serialize the object and store it?
		 * Serialized object will consume less space and we can fit in more serialized objects than non-serialized object. But we do have a drawback, as it will be bit more load on CPU and performance
		 * as CPU needs to de-serialize for processing.
		 * 
		 * 
		 * Why do we need 'OFF_HEAP' as we know it is slower than 'MEMORY_ONLY'?
		 * There is one concept called 'GC storm',
		 * 			GC storm means, if the number of objects are more in the JVM heap memory then the garbage collector daemon thread will take lot of time to scan them for garbage collection. so during
		 * 			scanning so much of objects, the application might freeze, this problem is called as GC storm.
		 * By using 'OFF_HEAP', those objects will not be considered for garbage collection as it is not present in JVM heap. hence there is not 'GC storm' problem that can occur for these objects as 
		 * the garbage collection will not be done for these at all.
		 * 
		 * 
		 * In case of 'DISK_ONLY()' or 'DISK_ONLY_2', where will the RDD be saved?
		 * Consider an RDD is distributed across cluster and partition A is present in X node and partition B is present in Y node, then when persist is called on the RDD, the partition 'A' will be saved
		 * in the disk of 'X' node and the the partition 'B' will be saved in disk of 'Y' node.
		 * 
		 * Can we persist the RDD/DataFrame/Dataset into HDFS instead of individual node disks?
		 * No!! using persist you cannot save it into HDFS. If you want to save it to HDFS then you have to use, as follows,
		 * 	If on DataFrame/DataSet,
		 * 			df.write().save("hdfs://hostname:port/file/path");
		 *  If RDD,
		 *  			rdd.saveAsTextFile("hdfs://hostname:port/file/path",anyencodingthings)
		 *  
		 *  
		 *  When we do '.saveAsTextFil('hdfs://hostname:port/Users/yatcs/a.txt',anyencodingthings)', how will the output be present?
		 *  '/Users/yatcs/a.txt' will not be a text file actually. It will be a folder by name 'a.txt' which will contain files like as follows, 
		 *  			part-00000			// This is the output written by one worker node.
		 *  			part-00001			// This is the output written by another worker node.
		 *  			.					// The number of 'part' files will depend on number of cores * worker nodes. for example, there are 4 cores and there are 14 worker nodes, then number of 
		 *  			.					// part files will be '4*14'.
		 *  			part-00000.crc
		 *  			part-00001.crc
		 *  			.
		 *  			.
		 */
		

	}

}
