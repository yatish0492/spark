package com.yatish.datasets;

public class S1_What_is_DataSet {

	/*
	 * What is a DataSet?
	 * Data Set is same as RDD but has additional advantages over RDD. we can say DataSet is an upgraded version of RDD.
	 * 
	 * What is the additional advantage DataSet has over RDD?
	 * benefits of Spark SQL’s optimized execution engine.
	 * RDD
	 * ---
	 * RDD uses Java Serialization or Krypto(specialized class used for serialization) to serialize and de-serialize the objects.
	 * DataSet/DataFrame
	 * -----------------
	 * DataSet uses Encoders to do serialization and de-serialization. These 'Encoders' are more than 10x faster than 'Java Serialization' and 'Krypto'. Hence, 'DataSets' are 10 times 
	 * faster than RDD.
	 * RDD
	 * ---
	 * While RDD using Java Serialization or Krypto will convert the objects into bytes for sending through network. Spark cannot do any operations like filtering,sorting and hashing without
	 * de-serializing the bytes back into an object.
	 * DataSet/DataFrame
	 * -----------------
	 * While 'Dataset' using 'Encoders' will convert the objects into bytes for sending through networking in such a format that spark can do any operations like filtering, sorting and hashing without
	 * de-serializing the bytes back to an object. Hence consider we needs to send an object from A to C node and there is a B node in between these and this node will do some filtering on the object.
	 * In this case, there is no need to de-serialize the object in B node and do filtering and then serialize it and then send to B. The time and effort of de-serializing and serializing to perform
	 * an operation is saved hence it is more efficient than RDD
	 * RDD
	 * ---
	 * Consider, a data takes 10MB
	 * DataSet/DataFrame
	 * -----------------
	 * The same amount of data which takes 10MB in RDD will take only 1MB space. Hence 'DataSet' is more space efficient then 'RDD'.
	 * 
	 * 
	 * 
	 * What is 'Encoders'?
	 * 'Encoders' is a class which extends 'Serializable' and adds much more capability to it to make it fast and efficient.
	 * 
	 * 
	 */
}
