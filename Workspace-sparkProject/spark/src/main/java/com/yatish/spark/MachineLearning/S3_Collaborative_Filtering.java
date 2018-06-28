package com.yatish.spark.MachineLearning;

public class S3_Collaborative_Filtering {

	/*
	 * Why do we need these collaborative filtering?
	 *  These techniques aim to fill in the missing entries of a user-item association matrix.
	 *  
	 * Which algorithm  does spark-MLib uses for collaborative filtering?
	 * Alternating least squares(ALS).
	 * 
	 * What are the parameters we need for ALS in spark-MLib?
	 * 1) 'numBlocks' -->  is the number of blocks used to parallelize computation (set to -1 to auto-configure).
	 * 2) 'rank' --> is the number of latent factors in the model.
	 * 3) 'iterations' --> is the number of iterations to run.
	 * 4) 'lambda' --> specifies the regularization parameter in ALS.
	 * 5) 'implicitPrefs' --> specifies whether to use the explicit feedback ALS variant or one adapted for implicit feedback data.
	 * 6) 'alpha' --> is a parameter applicable to the implicit feedback variant of ALS that governs the baseline confidence in preference observations.
	 *  
	 * We saw that we specify 'implicitPrefs' like implicit and explicit feedback. What is feedback first of all?
	 * feedback is nothing but the user information available to provide recommendation to him like  (e.g. views, clicks, purchases, likes, shares etc.)
	 * 
	 * NOTE:
	 * ----
	 * 1) As of now, Spark Mlib only supports model based collaborative filtering. in which users and products are described by a small set of latent factors that can be used to predict missing entries.
	 * 
	 * 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
