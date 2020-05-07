package com.upgrad.pgpbde.project.map.reduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/** 
 * The <b> SaavnStreamingValue </b> class is used as the Output Value class for the Mapper class <b> SaavnStreamingMapper </b>. 
 * <p>
 * A Map Output Value is constructed using an object of this class.
 * <p>
 * It contains <b> trendingDay </b> and <b> songWeight </b> as its members and overrides the following methods:
 * <p>
 * readFields()
 * <p>
 * write()
 */

public class SaavnStreamingValue implements Writable {
	
	/** The value for the <b> trendingDay </b> member could be any value between 25th December to 31st December (both days inclusive). 
	 *  It is the day on which a particular song is trending.
	 * */
	private int trendingDay;
	
	/** The value for the <b> songWeight </b> member is the weight of a particular instance of a song streamed. 
	 * It is calculated using the following formula:
	 * <p>
	 * songWeight = streamingHour * streamingDay * 1
	 *  */
	private int songWeight;

	SaavnStreamingValue() {
		
		super();
		
	}

	/**
	 * The constructor constructs an object of type <b> SaavnStreamingValue </b> by setting values for <b> trendingDay </b> and <b> songWeight </b>.
	 * 
	 * @param  trendingDay is a 2 digit number of type <b> int </b> which acts as part of the <b> Value </b> for the record.
	 * @param  songWeight is a value of type <b> int </b> which acts as part of the <b> Value </b> for the record.		 
	 */
	public SaavnStreamingValue(int trendingDay, int songWeight) {
	    
		this.trendingDay = trendingDay;
	    this.songWeight   = songWeight;
	
	}
	
	/**
	 * This method returns the trendingDay for a particular <b> SaavnStreamingValue </b> record.
	 * 
	 * @return trendingDay is a 2 digit number of type <b> int </b> which acts as part of the <b> Value </b> for the record.
	 */
	public int getTrendingDay() {
		return trendingDay;
	}

	/**
	 * This method sets value for the trendingDay field for a particular <b> SaavnStreamingValue </b> record.
	 * 
	 * @param trendingDay is a 2 digit number of type <b> int </b> which acts as part of the <b> Value </b> for the record.
	 */
	public void setTrendingDay(int trendingDay) {
		this.trendingDay = trendingDay;
	}
	
	/**
	 * This method returns the songWeight for a particular <b> SaavnStreamingValue </b> record.
	 * 
	 * @return songWeight is a value of type <b> int </b> which acts as part of the <b> Value </b> for the record.
	 */
	public int getSongWeight() {
		return songWeight;
	}

	/**
	 * This method sets value for the songWeight field for a particular <b> SaavnStreamingValue </b> record.
	 * 
	 * @param songWeight is a value of type <b> int </b> which acts as part of the <b> Value </b> for the record.
	 */
	public void setSongWeight(int songWeight) {
		this.songWeight = songWeight;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		
		trendingDay = WritableUtils.readVInt(dataInput);
	    songWeight  = WritableUtils.readVInt(dataInput);      
	 
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
	    
		WritableUtils.writeVInt(dataOutput, trendingDay);
	    WritableUtils.writeVInt(dataOutput, songWeight);

	}

}