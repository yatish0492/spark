package com.yatish.spark.partitioner;

import org.apache.spark.Partitioner;

public class Custom_Partitioner extends Partitioner{

	/*
	 * We can create our own custom Partitioner as shown in this file. make sure the following mandatory things are taken care while writing 'Custom_Partitioner'
	 * 1) it should implement 2 abstract methods of 'Partitioner' class,
	 * 		a) 'int numPartitions()' --> This will return the number of partitions to be created.
	 * 		b) 'int getPartiton(object a)'  --> This will return the id/number of the partition this object 'a' belongs to. In this method you can give whatever the partitioning logic you want.
	 * 
	 */
	int numberOfPartitions;
	
	public Custom_Partitioner(int numberOfPartitions) {
		this.numberOfPartitions = numberOfPartitions;
	}
	
	@Override
	public int numPartitions()
	{
	    return numberOfPartitions;
	}
	
	@Override
	public int getPartition(Object key){
		Integer in = (Integer)key;
		if(in <5)
			return 1;
		else if(in<10)
			return 2;
		else 
			return 3;
	}
	
}
