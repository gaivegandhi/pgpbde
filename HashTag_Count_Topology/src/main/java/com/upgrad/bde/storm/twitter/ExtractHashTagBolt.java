package com.upgrad.bde.storm.twitter;

import java.util.Arrays;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class ExtractHashTagBolt extends BaseRichBolt {
	
	private OutputCollector collector;
	
	private String[] filteredHashTagsArr;

	
    public ExtractHashTagBolt() {
		// TODO Auto-generated constructor stub
	}

    
	public ExtractHashTagBolt(String... filteredHashTags) {
	    		
		this.filteredHashTagsArr = filteredHashTags;
    	    
    }
    

	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
        
    	this.collector = collector;
    
    }
    
    
	@Override
	public void execute(Tuple tuple) {	

		Status tweet = (Status)tuple.getValueByField("tweet");   
        
        try {
        
        	boolean isHashTagsAvailable = filteredHashTagsArr!=null && filteredHashTagsArr.length > 0;
        	
        	if (isHashTagsAvailable){
        		
        		for(HashtagEntity hashtag : tweet.getHashtagEntities()) {
        	
        			String hashTag = hashtag.getText();

        			if (Arrays.asList(filteredHashTagsArr).contains(hashTag)) {

        				this.collector.emit(tuple, new Values(hashTag));
        				
        			}
            		
        		}
 
        	}else {
        		
        		for(HashtagEntity hashtag : tweet.getHashtagEntities()) {
                	
        			String hashTag = hashtag.getText();
        		
        			this.collector.emit(tuple, new Values(hashTag));
        		  		
        		}
        		
        	}        		
        	
	        // On successful completion of the tuple processing, the ack() method is called.
	        this.collector.ack(tuple); 
	        	        
        }catch (Exception e) {
        	
        	// In case of an erroneous situation or exception, the tuple processing is considered failed and the fail() method is called.
        	this.collector.fail(tuple); 
        	
        	e.printStackTrace();
        
        }
		
	}


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    	declarer.declare(new Fields("hashtag"));
    
    }
    
    
    @Override
    public void cleanup() {
    	
    	// Do Nothing
       
    }
    
    
    @Override
    public Map<String, Object> getComponentConfiguration() {
       
    	return null;
    
    }
    

}
