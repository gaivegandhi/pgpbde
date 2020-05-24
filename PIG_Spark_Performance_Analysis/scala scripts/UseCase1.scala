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
	        				
	if (columns(0).equals("2") && columns(1).equals("2017-10-01 00:15:30") && columns(2).equals("2017-10-01 00:25:11") && columns(3).equals("1") && columns(4).equals("2.17")){
	        					
		true;
	        				
	} else{
	        					
		false;

	}
	        			
});
	        	       
filteredData.saveAsTextFile("/home/gaive/output/UseCase1/");
