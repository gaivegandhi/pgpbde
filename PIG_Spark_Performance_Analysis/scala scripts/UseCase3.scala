val rows = sc.textFile("/home/gaive/input/trip_yellow_taxi.data");

val dataWithoutHeader = rows.filter(row => {

	var columns = row.split(",");
		
	if (columns(0) == "VendorID" || columns(0) == "" || columns(0) == null){
	        		
		false;
	        		
	}else{ 

		true;

	}
	        				
});

val paymentType = dataWithoutHeader.map(row => {
	        		
	var vals = row.split(",");
	        		
	(vals(9), 1L);
	        		
});
	        
val finalStat = paymentType.reduceByKey((x,y) => x+y);
	        
val xchangeKey = finalStat.map(tuple => {

	var key = tuple._2;
	        					
	var value = tuple._1;
	        					
	(key,value);

});
	        
val orderData = xchangeKey.sortByKey(false);
	        
val exchangeKeyAgain = orderData.map(tuple => {

	var value = tuple._1;
	        					
	var key = tuple._2;
	        					
	(key,value);

});
	        
exchangeKeyAgain.saveAsTextFile("/home/gaive/output/UseCase3/");
