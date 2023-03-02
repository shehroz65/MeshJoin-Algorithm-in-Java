import java.sql.*;
import org.apache.commons.collections4.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.commons.collections4.multimap.*;


public class meshJoin {

	public static void InitMessage()
	{
		System.out.println("Connected With the database successfully");
	}
	
	public static void userpass(String user, String pass)
	{
		user = "root";
		pass = "1234";
	}
	
	public static void calculatequarter(int q, int m)
	{
		q = m / 4;
		q = q + 1;
	}
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		
		//initializing the multivaluedmap and queue of size 10 that will be used to store the partitions
		MultiValuedMap<String, Map<String, String>> mhm = new ArrayListValuedHashMap<>();
        ArrayBlockingQueue<List<Map<String,String>>> qu = new ArrayBlockingQueue<List<Map<String,String>>>(10);
		
        //initializing the username, password and connection values for connecting to the MYSQL database.
        
        String username = "root";
		String password = "1234";
		
		userpass(username, password);
		String connection_url = "jdbc:mysql://localhost:3306/metro_dw2";
		Connection connection = DriverManager.getConnection(connection_url, username, password);
        InitMessage();
        
        int transaction_count = 0;
        int master_count = 0;
        

        
        int iter = 0;
       
        //this will be our main loop for the 10000 entries of transaction table
        while(iter <= 9999)
        {
        	//when the master tuple reaches the end, goes to the start
        	if(master_count == 100)
        	{
        		master_count = 0;
        	}
        	
        	//for execution of queries
        	Statement stmnt1 = connection.createStatement();
        	Statement stmnt2 = connection.createStatement();
        	
        	//storing the sql queries inside variables for execution
        	String transaction_str;
        	String master_str;
        	try
        	{
        		transaction_str = "select * from transactions limit " + transaction_count + ",50";
            	master_str = "select * from masterdata limit " + master_count + ",10";
            	
        	}
        	finally
        	{
        	}
        	
        	
            ResultSet master_data;
            ResultSet transaction_data;
            try
            {
            	//data from master
            	master_data =stmnt2.executeQuery(master_str);
            	//data from transactions
            	transaction_data=stmnt1.executeQuery(transaction_str);
            }
            
            finally
            {
            	
            }

            List<Map<String, String>> l1 = new ArrayList<Map<String, String>>(); 
            
            //looping over the transaction data variables
        	while(transaction_data.next())
        	{
        		try
        		{
                	//mapping data into simple hash map
            		Map<String, String> transaction_map = new HashMap<String, String>();
                	
                	//making everything into strings
            		String custid = transaction_data.getString("CUSTOMER_ID");
                	
                	String pid = transaction_data.getString("PRODUCT_ID");
                	
                	String transid = transaction_data.getString("TRANSACTION_ID");
                	
                	String storeid = transaction_data.getString("STORE_ID");
                	String custname = transaction_data.getString("CUSTOMER_NAME");
                	
                	String tdate = transaction_data.getString("T_DATE");
                	
                	
                	String quantity = transaction_data.getString("QUANTITY");
                	
                	String storename = transaction_data.getString("STORE_NAME");
                	
                	//putting in map
                	transaction_map.put("T_DATE" , tdate);
                	
                	transaction_map.put("PRODUCT_ID" , pid);
                	
                	transaction_map.put("TRANSACTION_ID" , transid);
                	
                	
                	transaction_map.put("STORE_ID" , storeid);
                	
                	transaction_map.put("CUSTOMER_ID" , custid);
                	transaction_map.put("STORE_NAME" , storename);
                	transaction_map.put("CUSTOMER_NAME" , custname);
                	
                	transaction_map.put("QUANTITY" , quantity);
                	
                	
                	l1.add(transaction_map);
                	
                	//putting the values of the single hash map into the multihash map against their Product IDs
                	mhm.put(transaction_map.get("PRODUCT_ID"), transaction_map);
        		}
        		catch(SQLException e) {System.out.println("Error"); }
        		finally
        		{
        			
        		}

        	}
        	int queue_size = qu.size();
        	int max_queue_size = 9;
        	//removing from map if our partition queue size exceeds 10
        	if (queue_size>max_queue_size)
            {
            	for(Map<String,String> Maps: qu.poll())
            	{
            		String pid2 = "PRODUCT_ID";
            		mhm.removeMapping(Maps.get(pid2), Maps);
            	}
            }

         
           qu.add(l1);

       		Statement insertion = connection.createStatement();
       		String trdd;
       		String PRIDD;
       		String cuID;
       		String STRN;
        	while(master_data.next())
        	{
        		//Joining and inserting
        		for(Map<String, String> itr: mhm.get(master_data.getString("PRODUCT_ID")))
        		{
        			
		        	String pid = itr.get("PRODUCT_ID");
		        	String custid = itr.get("CUSTOMER_ID");
		        	String custname = itr.get("CUSTOMER_NAME");
		        	String storeid = itr.get("STORE_ID");
		        	String tdate = itr.get("T_DATE");
		        	String storename = itr.get("STORE_NAME");
		        	
		        	String prodid = master_data.getString("PRODUCT_ID");
        			String prodname = master_data.getString("PRODUCT_NAME");
        			String suppid = master_data.getString("SUPPLIER_ID");
        			String suppname = master_data.getString("SUPPLIER_NAME");
        			float price = master_data.getFloat("PRICE");
        			float qty = Integer.parseInt(itr.get("QUANTITY"));
        			
        			float sales = price * qty;
        			String insert_str;
        			
        			//the following try catches will insert the data into the Data Warehouse Star Schema
        			try
        			{
        				insert_str = "INSERT INTO product Values ('" + pid + "',' " + prodname + "')";
            			insertion.executeUpdate(insert_str);
        			}
        			catch(SQLException e) {System.out.println("Rejected duplicate primary key entry for dimension table"); }
        				
        			finally {
        				
        			}
        			
        			try
        			{
        				insert_str = "INSERT INTO customer Values ('" + custid + "',' " + custname + "')";
            			insertion.executeUpdate(insert_str);
        			}
        			catch(SQLException e) {System.out.println("Rejected duplicate primary key entry for dimension table");}
        			finally {
        				
        			}
        			
        			try
        			{
        				insert_str = "INSERT INTO store Values ('" + storeid + "',' " + storename + "')";
            			insertion.executeUpdate(insert_str);
        			}
        			catch(SQLException e) { System.out.println("Rejected duplicate primary key entry for dimension table");}
        			finally {
        				
        			}
        			
        			try
        			{
        				String supplier_name = suppname.replace("'", "");
        				insert_str = "INSERT INTO supplier Values ('" + suppid + "',' " + supplier_name + "')";
            			insertion.executeUpdate(insert_str);
        			}
        			catch(SQLException e) { System.out.println("Rejected duplicate primary key entry for dimension table"); }
        			finally {
        				
        			}
        			
        			
        			
        			String[] date2 = tdate.split("-");
        			//System.out.println(date2[2]);
        			//date2[2] is for day
        			//date2[1] is for month
        			//date2[0] is for year
        			int month = Integer.valueOf(date2[1]);
        			int quarter = 0;
        			//
        			calculatequarter(quarter, month);
        			quarter = 1*((month / 4));
        			quarter = quarter + 1;
        			
        			try
        			{												  //day				//day
        				insert_str = "INSERT INTO TIMING Values ('" + tdate + "',' " + date2[2] + "',' "  +  
        						date2[1] + "',' " + quarter + "')";
        						//month^
            			insertion.executeUpdate(insert_str);
        			}
        			catch(SQLException e) {System.out.println("Rejected duplicate primary key entry for dimension table");}
        			finally {
        				
        			}
        			
        			try
        			{
        				insert_str = "INSERT INTO fact Values ('" + sales + "', '" + prodid + "', '"  + suppid + "', '" + tdate + "', '" + storeid + "', '" + custid + "', '" + qty + "')";
            			insertion.executeUpdate(insert_str);
        			}
        			catch(SQLException e) {System.out.println("Rejected duplicate primary key entry for dimension table");}
        			finally {
        				
        			}
        			
		        	iter++;
        		}
        		
        	}
        	master_count = master_count + 10;
	        transaction_count = transaction_count + 50; 
	        
        }
    	System.out.println(iter);
	}

}
