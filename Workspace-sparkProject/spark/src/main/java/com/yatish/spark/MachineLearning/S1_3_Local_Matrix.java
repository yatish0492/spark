package com.yatish.spark.MachineLearning;

import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;

public class S1_3_Local_Matrix {

	public static void main(String[] args) {
		
		/*
		 * It is nothing but a normal matrix. the dimension is stored as int type and the values are double type.
		 * 
		 * Spark MLIb provides a class called 'Matrices' which has a method called 'dense(int arg1, int arg2, double[] arg3)' using which we will create a matrix. 'dense' function accepts 3 
		 * arguments as follows,
		 * 		arg1  -->  Number of rows in matrix
		 * 		arg2  -->  Number of columns in matrix
		 * 		arg3  -->  Elements of the matrix in an array
		 * 
		 * 
		 * In the below code we are creating a 3*2 matrix, as shown below,
		 * 		1.0		2.0
		 * 		3.0		4.0
		 * 		5.0		6.0
		 */
		
		Matrix matrix = Matrices.dense(3, 2, new double[] {1.0,2.0,3.0,4.0,5.0,6.0});
		
		
		/*
		 * NOTE:
		 * ----
		 * 1) There is no support for sparse matrix only dense matrix is supported.
		 */

	}

}
