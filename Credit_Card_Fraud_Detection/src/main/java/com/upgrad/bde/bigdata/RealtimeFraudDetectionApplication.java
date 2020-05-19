package com.upgrad.bde.bigdata;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;


public class RealtimeFraudDetectionApplication {

	static boolean isConfigLoaded                                  = false;
	
	static HashMap<String,String> dBConfig                         = new HashMap<>();
	
	static HashMap<String,Object> kafkaConfig                      = new HashMap<>();

	static JavaDStream<TransactionPOJO> transaction                = null;

	static JavaInputDStream<ConsumerRecord<String, String>> stream = null;

	static JavaStreamingContext jssc                               = null;

	static SparkConf sparkConf                                     = null;
	
	static final String CONFIG_FILE                                = "config.properties";

	static final String APP_NAME                                   = "RealtimeCreditCardFraudDetection";
	
	
	public static void initializeConfig() {
		
		Properties properties = new Properties();
		 
		try(InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_FILE)){
		
			properties.load(inputStream);
			
			/* A HashMap object to store NOSQL database parameters to connect to the HBase database. */
			dBConfig.put("hbase.host.name", properties.getProperty("hbase.host.name"));
			dBConfig.put("hbase.master.port", properties.getProperty("hbase.master.port"));
			dBConfig.put("hbase.client.port", properties.getProperty("hbase.client.port"));

			/* A HashMap object to store Kafka parameters to connect to the Kafka server. */
			kafkaConfig.put("bootstrap.servers", String.valueOf(properties.getProperty("kafka.bootstrap.servers.port")));
			kafkaConfig.put("group.id", String.valueOf(properties.getProperty("kafka.group.id")+Math.random()));
			kafkaConfig.put("auto.offset.reset", String.valueOf(properties.getProperty("kafka.auto.offset.reset")));
			kafkaConfig.put("enable.auto.commit", Boolean.valueOf(properties.getProperty("kafka.enable.auto.commit")));
			kafkaConfig.put("kafka.topic.name", String.valueOf(properties.getProperty("kafka.topic.name")));
			kafkaConfig.put("key.deserializer", StringDeserializer.class);
			kafkaConfig.put("value.deserializer", StringDeserializer.class);
			
			isConfigLoaded = true;
			
		}catch(IOException e){
			
			e.printStackTrace();
		
		}
		
	}
	
	
	public static void initializeStreamingContext(){

		try{

			/* Introduced Logger to reduce Spark Logging on console */
			Logger.getLogger("org").setLevel(Level.ERROR);
	        Logger.getLogger("akka").setLevel(Level.ERROR);
 
			/* The SparkConf object is created and initialized to run in local mode. 
			 * An APPNAME is set for Spark to uniquely distinguish the spark streaming context for this program execution. 
			 * */
	        sparkConf = new SparkConf().setAppName(APP_NAME).setMaster("local");

	        /* The JavaStreamingContext is created and initialized. The SparkConf object is passed along with Batch Interval (in seconds) */
	        jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
			
		}catch(Exception e){
			
			e.printStackTrace();
			
		}
		
	}
	
	
	public static void initializeNOSQLDBConfig(){

		try{
			        
	        /* The HashMap object is passed to the TransactionDAO class via the initializeTransactionDAO() method.
	         * */
	        TransactionDAO.initializeTransactionDAO(dBConfig);
						
		}catch(Exception e){
			
			e.printStackTrace();
			
		}
		
	}
	
	
	public static void initializeKafkaStream(){

		final String TOPIC_NAME = (String) kafkaConfig.get("kafka.topic.name");

		try{

	        Collection<String> topics = Arrays.asList(TOPIC_NAME);
	        
	        /* Using the createDirectStream() method of the KafkaUtil class, create a JavaInputDStream which will 
	         * contain transactions data (as JSON strings) as RDDs within DStreams 
	         * */
	        stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaConfig));
			
		}catch(Exception e){
			
			e.printStackTrace();
			
		}
		
	}
	
	
	public static void processTransactions(){

		try{
			
	        /* The FlatMapFunction STREAM_PROCESSING  is used for a flatMap() transformation. 
			 * This transformation operates on the JSON data collected as JavaDStream<String>.
			 * */
	        final FlatMapFunction<String, TransactionPOJO> streamProcessing = new FlatMapFunction<String, TransactionPOJO>() {
				
				private static final long serialVersionUID = 1L;

				@Override
				public Iterator<TransactionPOJO> call(String transaction) throws IOException, ParseException  {
					
					TransactionPOJO transactionPOJO;
		
					boolean ruleUCL              = false;
					boolean ruleScore            = false;
					boolean ruleZipCode          = false;
					
					Date lastTransactionDate     = null;
					Date currentTransactionDate  = null;
					
					Double ucl                   = null;
					Double transactionAmt        = null;
					double distanceInKM          = 0;
					double dateDiffenceInSecs    = 0;
					double distanceCoveredInSecs = 0;
					
					Integer memberScore          = null;
					
					String lastZipCode           = null;
					String currentZipCode        = null;
					String lastTransDate         = null;
					String currentTransDate      = null;
					
					/*  This method is called to initialize the zipCodesMap HashMap with Key as ZipCode (type as String) and Value as ZipCode (in object of type ZipCode) 
					 * */
					ZipCodeDistance.initializeZipCodeMap();
					
					/* Using the toJSON() method of the JSONSerializer class, parse the JSON strings as JSON objects. */
					JSONObject jsonTransaction = (JSONObject) JSONSerializer.toJSON(transaction);
					
					System.out.println("JSON: "+transaction);
					
					ArrayList<TransactionPOJO> transactionArrList= new ArrayList<>(); 
			    
					transactionPOJO = new TransactionPOJO();
					
					/* Retrieve and store the transaction data from the JSON objects using the get() method. 
					 * For each record (transaction), an object of TransactionPOJO class and initialize the transaction data (retrieved from the JSON object) 
					 * using the setter methods of the class
					*/
					transactionPOJO.setCardID(jsonTransaction.get("card_id").toString());
					transactionPOJO.setMemberID(jsonTransaction.get("member_id").toString());
					transactionPOJO.setAmount((String)jsonTransaction.get("amount").toString());
					transactionPOJO.setPosID((String)jsonTransaction.get("pos_id").toString());
					transactionPOJO.setPostCode((String)jsonTransaction.get("postcode").toString());
					transactionPOJO.setTransactionDate((String)jsonTransaction.get("transaction_dt").toString());
					
					
					/* Using the getUCL() method of the TransactionDAO, retrieve the ucl from the look_up table for the card holder.
					 * Using the getAmount() method of the TransactionPOJO, retrieve the amount from the TransactionPOJO object containing transaction data.
					 * Using the getScore() method of the TransactionDAO, retrieve the member score from the look_up table for the card holder. 
					 * */ 
					ucl              = TransactionDAO.getUCL(transactionPOJO);
					transactionAmt   = Double.parseDouble(transactionPOJO.getAmount());
					memberScore      = TransactionDAO.getScore(transactionPOJO);
					
					/* Using the getPostCode() method of the TransactionDAO, retrieve the zip code of the last transaction from the look_up table for the card holder 
					 * Using the getPostcode() method of the TransactionPOJO, retrieve the zip code of current transaction from the TransactionPOJO object containing transaction data.
					 * Using the getDistanceViaZipCode() method of the ZipCodeDistance class, calculate the distance in Kilometers between the 02 zip codes.
					 * */
					lastZipCode      = TransactionDAO.getPostCode(transactionPOJO).toString();
					currentZipCode   = transactionPOJO.getPostCode(); 
					distanceInKM     = ZipCodeDistance.getDistanceViaZipCode(lastZipCode,currentZipCode);
					
					/* Using the getTransactionDate() method of the TransactionDAO, retrieve the transaction date of the last transaction from the look_up table for the card holder
					 * Using the getTransaction_dt() method of the TransactionPOJO, retrieve the transaction date of current transaction from the TransactionPOJO object containing transaction data. 
					 * */
					lastTransDate    = TransactionDAO.getTransactionDate(transactionPOJO);
					currentTransDate = transactionPOJO.getTransactionDate();
					
					/* Check if transaction amount is less than or equal to ucl. If yes, set ruleUCL boolean variable to true. */
					if (transactionAmt!= null && ucl!= null && transactionAmt<=ucl){
			
							ruleUCL = true;
					
					}
					
					/* Check if score is greater than or equal to 200. If yes, set ruleScore boolean variable to true. */
					if (memberScore!=null && memberScore>=200){
					
							ruleScore = true;
						
					}
					
					/* Calculate the difference in time between the last transaction date and current transaction date.
					 * Calculate the distance covered in secs using the below formula:
					 * distanceCoveredInSecs  = distanceInKM/dateDiffenceInSecs
					 * */
					if (lastTransDate!= null && currentTransDate!= null){
					
						lastTransactionDate    = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss").parse(lastTransDate);
						currentTransactionDate = new SimpleDateFormat("dd-MM-yyyy hh:mm:ss").parse(currentTransDate);
						
						/* In order to deal with incorrect transaction date data i.e. 
						 * the last transaction date in the look_up table is greater than or newer than the current transaction date, 
						 * it was proposed to take an absolute value of transaction dates difference to make sure a lot of data does not get labeled as FRAUD.
						 * I have taken the suggested and most popular approach.
						 *  */
						dateDiffenceInSecs     = Math.abs((currentTransactionDate.getTime()-lastTransactionDate.getTime())/1000);
						
						distanceCoveredInSecs  = distanceInKM/dateDiffenceInSecs;
					
					}
					
					/* Check if distanceCoveredInSecs is less than 0.25. If yes, set ruleZipCode boolean variable to true. */	
					if (distanceCoveredInSecs<0.25){
						
						ruleZipCode = true;
					
					}
					
					/* If all the rules are met, then classify the transaction to be GENUINE. 
					 * 	Using the setStatus() method of the TransactionPOJO class, set the status as GENUINE.
					 * 	Using the updateLookUp() method of the TransactionDAO class, update the new post code and transaction date in the look_up table.
					 * Else classify it as FRAUD Using the setStatus() method of the TransactionPOJO class, set the status as FRAUD. 
					*/
					if (ruleUCL && ruleScore && ruleZipCode){
					
						transactionPOJO.setStatus("GENUINE");
						TransactionDAO.updateLookUp(transactionPOJO);
					
					}else{
					
						transactionPOJO.setStatus("FRAUD");
						
					}
					
					/* Using the insertTransaction() method of the TransactionDAO class, insert the new transaction details in the card_transactions table. */
					TransactionDAO.insertTransaction(transactionPOJO);
										
					transactionArrList.add(transactionPOJO);
										
					return transactionArrList.iterator();
				
				}
			};

			JavaDStream<String> transactionData = stream.map(x -> x.value());
	        
			transaction = transactionData.flatMap(streamProcessing);
									
		}catch(Exception e){
			
			e.printStackTrace();
			
		}
		
	}
	
	
	public static void 	printTransactionStatus(){
		
        /* Print the transactions categorized as GENUINE or FRAUD */
        transaction.foreachRDD(new VoidFunction<JavaRDD<TransactionPOJO>>() {

        	private static final long serialVersionUID = 1L;

            @Override
            public void call(JavaRDD<TransactionPOJO> rdd) {
             
            	rdd.foreach(trans -> trans.print());
                    
            }
        });	
		
	}
	
	
	public static void main(String[] args) {
				
		try{
	    	        	
			initializeConfig();
	        	
	        initializeNOSQLDBConfig();
	        	
			initializeStreamingContext();
				
			initializeKafkaStream();
				
			processTransactions();
				
			printTransactionStatus();
				
			jssc.start();
			        
			jssc.awaitTermination();
	        
		}catch(Exception e){
			
			e.printStackTrace();
			
		}finally{
			
			if (jssc!= null){
				
				jssc.close();
			
			}
			
		}

	}

}