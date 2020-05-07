package com.upgrad.pgpbde.project.map.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/** 
 * The <b> SaavnStreamingKey </b> class is used as the Output Key class for the Mapper class <b> SaavnStreamingMapper </b>. 
 * <p>
 * A Map Output Key is constructed using an object of this class.
 * <p>
 * It contains <b> trendingDay </b> and <b> songId </b> as its members and overrides the following methods:
 * <p>
 * readFields()
 * <p>
 * write()
 * <p>
 * hashCode()
 * <p>
 * compareTo()
 */
public class SaavnStreamingKey implements WritableComparable<SaavnStreamingKey> {

	/** The value for the <b> trendingDay </b> member could be any value between 25th December to 31st December (both days inclusive). 
	 *  It is the day on which a particular song is trending.
	 * */
	private IntWritable trendingDay;
	
	/** The value for the <b> songId </b> member is an id which identifies a song. */
	private Text songId;

	SaavnStreamingKey() {
		
		this.trendingDay = new IntWritable();
        this.songId      = new Text();
		
	}

	/**
	 * The constructor constructs an object of type <b> SaavnStreamingKey </b> by setting values for <b> trendingDay </b> and <b> songId </b>.
	 * 
	 * @param  trendingDay is a 2 digit number of type <b> IntWritable </b> which acts as part of the <b> Key </b> for the record.
	 * @param  songId is an alphanumeric value of type <b> Text </b> which acts as part of the <b> Key </b> for the record.		 
	 */
	public SaavnStreamingKey(IntWritable trendingDay, Text songId) {

		this.trendingDay = trendingDay;
		this.songId    = songId;
	
	}
	
	/**
	 * This method returns the trendingDay for a particular <b> SaavnStreamingKey </b> record.
	 * 
	 * @return trendingDay is a 2 digit number of type <b> IntWritable </b> which acts as part of the <b> Key </b> for the record.
	 */
	public IntWritable getTrendingDay() {
		
		return trendingDay;
	
	}

	/**
	 * This method sets value for the trendingDay field for a particular <b> SaavnStreamingKey </b> record.
	 * 
	 * @param trendingDay is a 2 digit number of type <b> IntWritable </b> which acts as part of the <b> Key </b> for the record.
	 */
	public void setTrendingDay(IntWritable trendingDay) {
		
		this.trendingDay = trendingDay;
	
	}

	/**
	 * This method returns the songId for a particular <b> SaavnStreamingKey </b> record.
	 * 
	 * @return songId is an alphanumeric value of type <b> Text </b> which acts as part of the <b> Key </b> for the record
	 */
	public Text getSongID() {
		
		return songId;
	
	}
	
	/**
	 * This method sets value for the songID field for a particular <b> SaavnStreamingKey </b> record.
	 * 
	 * @param songId is an alphanumeric value of type <b> Text </b> which acts as part of the <b> Key </b> for the record
	 */
	public void setSongID(Text songId) {
		
		this.songId = songId;
	
	}
	
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		
		this.trendingDay.readFields(dataInput);
        this.songId.readFields(dataInput);
		 
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
	    
		this.trendingDay.write(dataOutput);
        this.songId.write(dataOutput);
		
	}
	
	
	/**
	 * The default implementation of the hashCode() method is overridden with a custom implementation of the hashCode() method.
	 * 
	 * Since a custom key class is used in the mapper output, it is important to override the method and provide our own implementation of generating hashCode for the keys.
	 * 
	 * @return It returns a hashCode of type <b> int </b>.
	 */
	public int hashCode() {
		
	    final int prime = 31;
	    int result = 1;
	    result = prime * result + ((trendingDay == null) ? 0 : trendingDay.hashCode());
	    result = prime * result + ((songId == null) ? 0 : songId.hashCode());
	    return result;
	
	}
	
	/**
	 * The default implementation of the equals() method is overridden with a custom implementation of the equals() method as the hashCode() method is also overridden.
	 * 
	 * Since a custom key class is used in the mapper output, it is important to override the method and provide our own implementation of comparing keys.
	 * 
	 * @param key is an object of type <b> SaavnStreamingKey </b>.
	 * 
	 * @return It returns true if objects are equal else it returns false.
	 */
	public boolean equals(Object key) {
		
		if (key instanceof SaavnStreamingKey){
			
			if (key != null){
				
				SaavnStreamingKey temp = (SaavnStreamingKey)key;
				
				return trendingDay.toString().equals(temp.trendingDay.toString()) && songId.toString().equals(temp.songId.toString())?true:false; 
			}
		}
		
		return false;
		
	}
	
	/**
	 * The default implementation of the compareTo() method is overridden with a custom implementation of the compareTo() method.
	 * 
	 * Since a custom key class is used in the mapper output, it is important to override the method and provide our own implementation of sorting the keys.
	 * 
	 * @param key is an object of type <b> SaavnStreamingKey </b>.
	 * 
	 */
	public int compareTo(SaavnStreamingKey key) {
		
        if (key == null)
            return 0;
        
        int intcnt = trendingDay.compareTo(key.trendingDay);
        
        if (intcnt != 0) {
            
        	return intcnt;
        
        } else {
            
        	return songId.compareTo(key.songId);

        }
    }

}