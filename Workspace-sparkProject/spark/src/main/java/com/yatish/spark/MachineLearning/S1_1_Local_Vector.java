package com.yatish.spark.MachineLearning;


import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class S1_1_Local_Vector {

	public static void main(String[] args) {
		
		/*
		 * MLib supports 2 types of local vectors called as dense and sparse vector.
		 * 
		 */
		
		/*
		 * Dense vector
		 */
		Vector dv = Vectors.dense(1.0, 0.0, 3.0, 4.0, 0.0, 0.0, 7.0, 0.0, 0.0, 10.0);
		
		
		/*
		 * Sparse vector
		 * 
		 * Sparse Vector is more efficient than Dense vector in-term of storage, because we don't specify the values if the value is '0'. we  just specify only the values which are not '0' hence,
		 * we need not simply save '0' which will simply consume so much of storage.
		 * 
		 * If Sparse Vector doesn't store 0, how to we actually store data?
		 * Spark provides, a method '.sparse(arg0, arg1, arg2)' through which, we can create a sparse vector. we pass the following parameters to it as follows,
		 *   arg0 --> number of the elements, in vector which are not '0' value
		 *   arg1 --> array containing indexes of the data which are not '0' value. the missing index values are index values of the '0' value data.
		 *   arg2 --> array of data which are not '0' value, corresponding to the index values specified in 'arg1' 
		 * 
		 * In the below code, we are creating the vector of the same data which we used for creating dense vector in above code - '1.0, 0.0, 3.0, 4.0, 0.0, 0.0, 7.0, 0.0, 0.0, 10.0'
		 */
		Vector sv = Vectors.sparse(5,new int[] {0,2,3,6,9}, new double[] {1.0,3.0,4.0,7.0,10.0});

	}

}
