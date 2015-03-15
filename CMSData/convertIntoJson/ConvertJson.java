import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.*;

import org.json.simple.*;
// For this JSONConvertion, we need to include json-simple library file

public class ConvertJson {
	public static void main(String[] args) {
		try{
			/* Connection con that has been established with server
			 * has details of server to which we need connection to
			 * access CMS data i.e.  server name,Login and Password.
			 */
	    Connection con = DriverManager.getConnection(
	                     "jdbc:sqlserver://sdtqjk71tc.database.windows.net:1433;databaseName=BDR-CMSData",
	                         "BDR","P@ssw0rd");

	    Statement stmt = con.createStatement();
	    
	    // The query enclosed is used for retrieving data from CMS server
	    ResultSet rs = stmt.executeQuery("select top 10 * from DE1_0_2008_to_2010_Carrier_Claims_Sample_2A");

	    // JSONArray will contain the json output.
	    JSONArray json = new JSONArray();
	    
	    // rsmd is used to count the number of columns in specified table.
	    ResultSetMetaData rsmd = rs.getMetaData();
	    
	    //create an print writer for writing to a file
 	    PrintWriter out = new PrintWriter(new FileWriter("CMSJson.json"));
 	    
	    while(rs.next()) {
	      int numColumns = rsmd.getColumnCount();

	      //This loop is converting sql data in json format.
	      for (int i=1; i<numColumns+1; i++) {
	        String column_name = rsmd.getColumnName(i);
	         json.add(rs.getObject(column_name));   	     	  
	 	    
	        }
	    //output to the file a line
	 	    out.println(json);
		}
	    
	  
 	    //close the file
 	    out.close();
	    		//System.out.println(json);   
		}
		catch (Exception e) {
			System.out.println("Error aa gaya :( ");
	         e.printStackTrace();
	      }
	}
}
