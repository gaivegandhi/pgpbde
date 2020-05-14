package com.upgrad.bde.sparkstreaming;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;

public class StockDataAnalysisClient {
	
	private static boolean isConfigLoaded                    = false;
	private static final String CONFIG_FILE                  = "config.properties";
	
	private static HashMap<String,String> configForAnalysis1 = new HashMap<>();
	private static HashMap<String,String> configForAnalysis2 = new HashMap<>();
	private static HashMap<String,String> configForAnalysis3 = new HashMap<>();
	private static HashMap<String,String> configForAnalysis4 = new HashMap<>();
	
	
	private static void readConfig() throws IOException {
		
		Properties prop = new Properties();
		
    	try (InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_FILE)){

    		prop.load(input);
            
            configForAnalysis1.put("analysis.batch.interval",prop.getProperty("analysis1.batch.interval"));
            configForAnalysis1.put("analysis.window.duration",prop.getProperty("analysis1.window.duration"));
            configForAnalysis1.put("analysis.sliding.window.duration",prop.getProperty("analysis1.sliding.window.duration"));
            configForAnalysis1.put("analysis.name",prop.getProperty("analysis1.name"));		
            configForAnalysis1.put("common.input.path",prop.getProperty("common.input.path"));
            configForAnalysis1.put("common.output.path",prop.getProperty("common.output.path"));

            configForAnalysis2.put("analysis.batch.interval",prop.getProperty("analysis2.batch.interval"));
            configForAnalysis2.put("analysis.window.duration",prop.getProperty("analysis2.window.duration"));
            configForAnalysis2.put("analysis.sliding.window.duration",prop.getProperty("analysis2.sliding.window.duration"));
            configForAnalysis2.put("analysis.name",prop.getProperty("analysis2.name"));		
            configForAnalysis2.put("common.input.path",prop.getProperty("common.input.path"));
            configForAnalysis2.put("common.output.path",prop.getProperty("common.output.path"));

            configForAnalysis3.put("analysis.batch.interval",prop.getProperty("analysis3.batch.interval"));
            configForAnalysis3.put("analysis.window.duration",prop.getProperty("analysis3.window.duration"));
            configForAnalysis3.put("analysis.sliding.window.duration",prop.getProperty("analysis3.sliding.window.duration"));
            configForAnalysis3.put("analysis.name",prop.getProperty("analysis3.name"));		
            configForAnalysis3.put("common.input.path",prop.getProperty("common.input.path"));
            configForAnalysis3.put("common.output.path",prop.getProperty("common.output.path"));

            configForAnalysis4.put("analysis.batch.interval",prop.getProperty("analysis4.batch.interval"));
            configForAnalysis4.put("analysis.window.duration",prop.getProperty("analysis4.window.duration"));
            configForAnalysis4.put("analysis.sliding.window.duration",prop.getProperty("analysis4.sliding.window.duration"));
            configForAnalysis4.put("analysis.name",prop.getProperty("analysis4.name"));		
            configForAnalysis4.put("common.input.path",prop.getProperty("common.input.path"));
            configForAnalysis4.put("common.output.path",prop.getProperty("common.output.path"));
            
            isConfigLoaded = true;
                       
        } catch (IOException e) {
        	
            e.printStackTrace();
        
        }

	}
	
	
	public static void main(String[] args) {
			
		char ch;
		Scanner sc = new Scanner(System.in);
			
		try{
						
			while(true) {
		
				System.out.println("Welcome To The Project On Stock Data Analysis Using Spark Streaming");
				System.out.println("===================================================================");
				System.out.println("");
				System.out.println("1. Load Configuration Properties");
				System.out.println("2. Simple Moving Average - Close Price");
				System.out.println("3. Stock With Maximum Profit");
				System.out.println("4. Stocks With RSI");
				System.out.println("5. Stock With Maximum Volume");
				System.out.println("6. Exit");
				System.out.println("");
					
				System.out.print("Please Enter The Menu Number: ");
				System.out.print("");
					
				ch = (char) sc.nextInt();
					
				switch(ch) {
		
					case 1:{
					
						readConfig();
						break;
					
					}
					
					case 2:{
			
						if (isConfigLoaded) {
							
							StockDataAnalysis.simpleMovingAvgClosePrice(configForAnalysis1);
						
						}else {
						
							System.out.println("The Configuration File Is Not Loaded. Please Use Option 1 And Load The Configurations.");
						}
						
						break;
						
					}
						
					case 3:{
	
						if (isConfigLoaded) {
							
							StockDataAnalysis.stockWithMaxProfit(configForAnalysis2);
						
						}else {
						
							System.out.println("The Configuration File Is Not Loaded. Please Use Option 1 And Load The Configurations.");
						
						}
						
						break;
					}
						
					case 4:{
						
						if (isConfigLoaded) {
						
							StockDataAnalysis.stocksWithRSI(configForAnalysis3);
						
						}else {
						
							System.out.println("The Configuration File Is Not Loaded. Please Use Option 1 And Load The Configurations.");
						
						}
						
						break;
							
					}
	
					case 5:{
						
						if (isConfigLoaded) {
							
							StockDataAnalysis.stockWithMaximumVolume(configForAnalysis4);
						
						}else {
						
							System.out.println("The Configuration File Is Not Loaded. Please Use Option 1 And Load The Configurations.");
						
						}
						
						break;
							
					}
						
					case 6:{
							
						System.exit(0);
						
						break;
							
					}
											
					default:{
													
						break;
						
					}
						
				}
					
			}
					
		}catch(Exception e) {
				
			e.printStackTrace();
			
		}finally {
				
			sc.close();
				
		}
		
	}

	
}