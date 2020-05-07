package com.upgrad.bde.perfanalysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UseCase2 {

	public static void main(String[] args) {
		
		JavaRDD<String> rows              = null;
		JavaRDD<String> dataWithoutHeader = null;
		JavaRDD<String> filteredData      = null;

		JavaSparkContext jsc              = null;
		SparkConf sparkConf               = null;		

		String inputFile                  = args[0].trim();
		String outputFile                 = args[1].trim();
		
		final String APP_NAME             = "PIG_And_Spark_Performance_Analysis_UseCase_2";
		
		try {
			
			sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local[*]");

			jsc = new JavaSparkContext(sparkConf);
			
	        rows = jsc.textFile(inputFile);
	        
	        dataWithoutHeader = rows.filter(row -> {
	        			
	        	String[] columns = row.split(",");
	        		
	        	if (columns[0].equals("VendorID")){
	        		
	        		return false;
	        		
	        	} 
	        				
	        	return true;
	        				
	        });     
	        
	        filteredData = dataWithoutHeader.filter(row -> {
	        			
	        	String[] columns = row.split(",");
	        				
	        	if (columns[0].equals("") || columns[1].equals("") || columns[2].equals("") || columns[3].equals("") || columns[4].equals("") || columns[5].equals("4")){
	        					
	        		return true;
	        				
	        	} 
	        					
	        	return false;
	        			
	        });
	        
	        filteredData.saveAsTextFile(outputFile);
        	        	        
		}catch (Exception e){
			
			e.printStackTrace();
			
		}finally{
		
			if (jsc!=null){
			
				jsc.close();
				
			}
		
		}
		
	}
	
}