package com.yatish.spark.MachineLearning;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

public class S1_4_Distributed_Matrix {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("SparkStart").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Vector> vectorList = new ArrayList<Vector>();
		Vector dv = Vectors.dense(1.0, 0.0, 3.0, 4.0, 0.0, 0.0, 7.0, 0.0, 0.0, 10.0);
		Vector dv1 = Vectors.dense(1.0, 0.0, 3.0, 4.0, 0.0, 0.0, 7.0, 0.0, 0.0);
		vectorList.add(dv);
		vectorList.add(dv1);
		
		
		JavaRDD<Vector> vectorRDD = sc.parallelize(vectorList);
		
		/*
		 * What is Distributed Matrix?
		 * The Local Matrix is a normal matrix type, which can be operated in a single machine but cannot be distributed across the nodes in the cluster because, it is not an RDD. Hence a matrix,
		 * which can be distributed across the nodes in the cluster is called distributed matrix.
		 * 
		 * 
		 * What are the types of Distributed Matrix?
		 * There are 3 types of Distributed matrix as follows,
		 * 1) RowMatrix
		 * 2) IndexedRowMatrix
		 * 3) CoordinateMatrix
		 * 
		 * 
		 * What is RowMatrix?
		 * 
		 * 
		 * 
		 */
		
		
		
		
		/*
		 * RowMatrix
		 * ---------
		 * 'RowMatrix' constructor accepts only 'RDD<Vector>', not 'JavaRDD<Vector>' hence we have to manually call 'rdd()' on the 'JavaRDD<Vector>' which will return 'RDD<Vector>'.
		 * 
		 * The Row ordering will be based on the order of the vectors present in the list, in this case 'vectorList'. The 1st vector in list will be 1st row and 2nd vector in the list will be 2nd
		 * row.
		 */
		RowMatrix rowMatrix = new RowMatrix(vectorRDD.rdd());
		
		System.out.println("rowMatrix Columns : " + rowMatrix.numCols());
		System.out.println("rowMatrix Rows : " + rowMatrix.numRows());
		
		
		
		/*
		 * IndexedRowMatrix
		 * ----------------
		 * It is same as 'RowMatrix', but only difference is you can manually specify the row number along with the vector. 
		 * 
		 * We use 'IndexedRow' type to achieve this. it has a constructor with 2 parameters 'IndexedRow(long index,Vector data)'
		 */
		List<IndexedRow> indexedRowList = new ArrayList<IndexedRow>();
		IndexedRow indexedRow = new IndexedRow(1,dv);	// even though this vector is 1st in the list, it will be added as the 2nd row in matrix, as we are specifying row index as '1'
		IndexedRow indexedRow1 = new IndexedRow(0,dv1);	// even though this vector is 2nd in the list, it will be added as the 1st row in matrix, as we are specifying row index as '0'
		IndexedRow indexedRow2 = new IndexedRow(4,dv);	// it will be added as the 5th row in matrix. 3rd and 4th rows will be left empty or filled with 0s since we are not specifying the vectors
														// with indexes 2 and 3 in this list.
		IndexedRow indexedRow3 = new IndexedRow(1,dv);	// we already have assigned a vector for 2nd row of matrix in the 1st element of this list, if we again mention the same index then the
														// old row data will be replaced with new vector data mentioned here.
		
		indexedRowList.add(indexedRow);
		indexedRowList.add(indexedRow1);
		indexedRowList.add(indexedRow2);
		indexedRowList.add(indexedRow3);
		
		JavaRDD<IndexedRow> indexedRowRDD = sc.parallelize(indexedRowList);
		
		IndexedRowMatrix indexedRowMatrix = new IndexedRowMatrix(indexedRowRDD.rdd());
		
		System.out.println("indexedRowMatrix Columns : " + indexedRowMatrix.numCols());
		System.out.println("indexedRowMatrix Rows : " + indexedRowMatrix.numRows());
		
		
		/*
		 * CoordinateMatrix
		 * ----------------
		 * In 'IndexedRowMatrix', we can specify which vector will be saved as which row in the matrix. But in 'CoordinateMatrix', we can specify at each element level, like individually specify
		 * a metric element and mention its position using row and column index. i.e. x and y value.
		 * 
		 *  We use 'MatrixEntry' type to achieve this. it has a constructor with 3 parameters 'IndexedRow(long row-index, long column-index, Vector data)'
		 */
		List<MatrixEntry> matrixEntryList = new ArrayList<MatrixEntry>();
		MatrixEntry matrixEntry1 = new MatrixEntry(0,1,10);	// '10' will be stored at (0,1) index position in matrix
		MatrixEntry matrixEntry2 = new MatrixEntry(1,1,11);	// '11'  will be stored at (1,1) index position in matrix
		MatrixEntry matrixEntry3 = new MatrixEntry(0,1,25);	// '25' will be stored at (0,1) index replacing '10'
		MatrixEntry matrixEntry4 = new MatrixEntry(3,1,66);	// '66' will be stored at (3,1) index position in matrix. so it will create a matrix of 3*1 and remaining element which are not
															// specified will be kept empty or '0'
		
		matrixEntryList.add(matrixEntry1);
		matrixEntryList.add(matrixEntry2);
		matrixEntryList.add(matrixEntry3);
		matrixEntryList.add(matrixEntry4);
	
		JavaRDD<MatrixEntry> matrixEntryRDD = sc.parallelize(matrixEntryList);
		
		CoordinateMatrix coordinateMatrix = new CoordinateMatrix(matrixEntryRDD.rdd());
		
		System.out.println("coordinateMatrix Columns : " + coordinateMatrix.numCols());
		System.out.println("coordinateMatrix Rows : " + coordinateMatrix.numRows());
		
		
		
		
		
		

	}

}
