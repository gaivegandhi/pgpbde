val rows = sc.textFile("/home/gaive/input/trip_yellow_taxi.data");

val dataWithoutHeader = rows.filter(row => {

	var columns = row.split(",");
		
	if (columns(0) == "VendorID"){
	        		
		false;
	        		
	}else{ 

		true;

	}
	        				
});

val filteredData = dataWithoutHeader.filter(row => {
	        				
	var columns = row.split(",");
	        				
	if (columns(0).equals("") || columns(1).equals("") || columns(2).equals("") || columns(3).equals("") || columns(4).equals("") || columns(5).equals("4")){
	        					
		true;
	        				
	} else{
	        					
		false;

	}
	        			
});
	        	       
filteredData.saveAsTextFile("/home/gaive/output/UseCase2/");
