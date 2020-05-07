package com.upgrad.bde.perfanalysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class UseCase3 {

	public static void main(String[] args) {

		JavaRDD<String> rows                       = null;
		JavaRDD<String> dataWithoutHeader          = null;
		JavaPairRDD<String, Long> paymentType      = null;
		JavaPairRDD<String, Long> finalStat        = null;
		JavaPairRDD<Long, String> xchangeKey       = null;
		JavaPairRDD<Long, String> orderData        = null;
		JavaPairRDD<String, Long> exchangeKeyAgain = null;
		
		JavaSparkContext jsc                       = null;
		
		String inputFile                           = args[0].trim();
		String outputFile                          = args[1].trim();
		
		final String APP_NAME                      = "PIG_And_Spark_Performance_Analysis_UseCase_3";
		
		try {
			
			SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");

			jsc = new JavaSparkContext(sparkConf);
			
			rows = jsc.textFile(inputFile);
	        
			dataWithoutHeader = rows.filter(row -> {
	        			
				String[] columns = row.split(",");
	        		
	        	if (columns[0].equals("VendorID") || columns[0].equals("") || columns[0]==null){
	        		
	        		return false;
	        		
	        	} 
	        				
	        	return true;
	        				
	        });     
	        
	        paymentType = dataWithoutHeader.mapToPair(row -> {
	        		
	        	String[] vals = row.split(",");
	        		
	        	return new Tuple2<String, Long>(vals[9], 1L);
	        		
	        });
	        
	        finalStat = paymentType.reduceByKey((x,y) -> x+y);
	        
	        xchangeKey = finalStat.mapToPair(
	        		tuple -> {
	        			Long key = tuple._2;
	        					
	        			String value = tuple._1;
	        					
	        			return new Tuple2<Long, String>(key,value);
	        		}
	        );
	        
	        orderData = xchangeKey.sortByKey(false);
	        
	        exchangeKeyAgain = orderData.mapToPair(
	        		tuple -> {
	        			Long value = tuple._1;
	        					
	        			String key = tuple._2;
	        					
	        			return new Tuple2<String, Long>(key,value);
	        		}
	        );
	        
	        exchangeKeyAgain.saveAsTextFile(outputFile);
        	        	        
		}catch (Exception e){
			
			e.printStackTrace();
			
		}finally{
		
			if (jsc!=null){
			
				jsc.close();
				
			}
		
		}
		
	}
	
}