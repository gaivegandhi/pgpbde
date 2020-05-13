package com.upgrad.bde.storm.twitter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class HashTagCountTopology {

	private static boolean isHashTagsAvailable             = false;
	private static String filteredHashTags                 = null;
	
	private static final String CONFIG_FILE                = "config.properties";
	private static final String TOPOLOGY_NAME              = "HashTag-Count-Topology";
	
	private static final String TWEET_STREAM_SPOUT         = "TweetStreamSpout";
	private static final String EXTRACT_HASHTAG_BOLT       = "ExtractHashTagBolt";
	private static final String HASHTAG_COUNT_BOLT         = "HashTagCountBolt";
    private static final String HASHTAG_COUNT_REPORT_BOLT  = "HashTagCountReportBolt";

    private static HashMap<String,String> twitterConfig    = new HashMap<>();
    private static HashMap<String,String> dbConfig         = new HashMap<>();
 
    
    private static void readConfig() throws IOException {

        Properties prop = new Properties();
    			
    	try (InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_FILE)){

            prop.load(input);

            twitterConfig.put("apiKey", prop.getProperty("twitter.apiKey"));
            twitterConfig.put("apiSecretKey", prop.getProperty("twitter.apiSecretKey"));
            twitterConfig.put("accessToken", prop.getProperty("twitter.accessToken"));
            twitterConfig.put("accessTokenSecret", prop.getProperty("twitter.accessTokenSecret"));
            
            filteredHashTags = prop.getProperty("twitter.filteredHashTags");
            
            if (filteredHashTags!=null && !filteredHashTags.isEmpty() && filteredHashTags.length()>0) {
            	
            	twitterConfig.put("filteredHashTags", filteredHashTags);
            	
            	isHashTagsAvailable = true;
            	
            }
            
            dbConfig.put("username", prop.getProperty("db.username"));
            dbConfig.put("password", prop.getProperty("db.password"));
            dbConfig.put("jdbcDriverClass", prop.getProperty("db.jdbcDriverClass"));
            dbConfig.put("jdbcConnectionUrl", prop.getProperty("db.jdbcConnectionUrl"));
            dbConfig.put("deleteQuery", prop.getProperty("db.deleteQuery"));
            dbConfig.put("findQuery", prop.getProperty("db.findQuery"));
            dbConfig.put("insertQuery", prop.getProperty("db.insertQuery"));
            dbConfig.put("updateQuery", prop.getProperty("db.updateQuery"));            
            
        } catch (IOException e) {
        	
            e.printStackTrace();
        
        }
    	
    }

    
    private static String[] stripHashTagChar(String hashTags) {
    	
    	String []tempArr = hashTags.split(" ");
    	int tempSize     = tempArr.length;
    	
    	ListIterator<String> iterator = Arrays.asList(tempArr).listIterator();
		
    	String[] strippedHashTagsArr  = new String[tempSize];
    	
    	int incr = 0;
    	
    	while(iterator.hasNext()) {
    			
    		String hashTag = iterator.next();
    			
    		if (hashTag.contains("#")) {
    				
    			strippedHashTagsArr[incr] = hashTag.substring(1);
    				
    		}else {
    			
    			strippedHashTagsArr[incr] = hashTag;
    			
    		}
    		
    		incr++;
    	}
    	
    	return strippedHashTagsArr;
    
    }
    
    
    public static void main(String[] args) throws Exception {

    	readConfig();  
    	
    	ExtractHashTagBolt extractBolt    = null;
    	
        TweetStreamSpout spout            = new TweetStreamSpout(twitterConfig);
        
        if (isHashTagsAvailable) {	
        	
        	String [] strippedHashTagsArr = stripHashTagChar(filteredHashTags);
        	
        	extractBolt                   = new ExtractHashTagBolt(strippedHashTagsArr);
        
        }else {
        
        	extractBolt                   = new ExtractHashTagBolt();
        	
        }
        
        HashTagCountBolt countBolt        = new HashTagCountBolt();
        HashTagCountReportBolt reportBolt = new HashTagCountReportBolt(dbConfig);
          
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TWEET_STREAM_SPOUT, spout);
        
        //The shuffleGrouping method is used for evenly distributing the original tuples i.e. tweets from the TweetStreamSpout to the ExtractHashTagBolt.
        builder.setBolt(EXTRACT_HASHTAG_BOLT, extractBolt,2).shuffleGrouping(TWEET_STREAM_SPOUT);
        
        //The fieldsGrouping method is used for distributing the tuples from the ExtractHashTagBolt to the HashTagCountBolt on the basis of tuple values i.e. hashtags.
        builder.setBolt(HASHTAG_COUNT_BOLT, countBolt, 3).fieldsGrouping(EXTRACT_HASHTAG_BOLT, new Fields("hashtag"));
        
        //The globalGrouping method is used for distributing all the tuples originating from the HashTagCountBolt to the HashTagCountReportBolt.
        builder.setBolt(HASHTAG_COUNT_REPORT_BOLT, reportBolt).globalGrouping(HASHTAG_COUNT_BOLT); 
        
        Config config = new Config();
        
        // The default time-out of 30 seconds is changed to 5 seconds. 
        // This is to create a possibility of tuple processing failure caused due to time-out.
        config.setMessageTimeoutSecs(5);        
                
        if (args != null && args.length > 0) {
        	
        	// The number of Worker Processes is increased to 2 to facilitate better parallelism.
            config.setNumWorkers(2);
            
            // The number of ACKER Bolt instances is increased to 2 to facilitate better parallelism.
            config.setNumAckers(2); 
            
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
        
        }
        else {
  			
        	LocalCluster cluster = new LocalCluster();
        	
        	cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
        	
        	Thread.sleep(90000);
        	
        	cluster.shutdown();
        
        }

    }
    
}