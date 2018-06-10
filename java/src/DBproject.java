/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Plane");
				System.out.println("2. Add Pilot");
				System.out.println("3. Add Flight");
				System.out.println("4. Add Technician");
				System.out.println("5. Book Flight");
				System.out.println("6. List number of available seats for a given flight.");
				System.out.println("7. List total number of repairs per plane in descending order");
				System.out.println("8. List total number of repairs per year in ascending order");
				System.out.println("9. Find total number of passengers with a given status");
				System.out.println("10. < EXIT");
				
				switch (readChoice()){
					case 1: AddPlane(esql); break;
					case 2: AddPilot(esql); break;
					case 3: AddFlight(esql); break;
					case 4: AddTechnician(esql); break;
					case 5: BookFlight(esql); break;
					case 6: ListNumberOfAvailableSeats(esql); break;
					case 7: ListsTotalNumberOfRepairsPerPlane(esql); break;
					case 8: ListTotalNumberOfRepairsPerYear(esql); break;
					case 9: FindPassengersCountWithStatus(esql); break;
					case 10: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddPlane(DBproject esql) {//1
            Scanner read = new Scanner(System.in);
	    try{ 
                int max = 0;
                String query = "INSERT INTO Plane (id, make, model, age, seats) VALUES (";
                String findmax = "(SELECT Max(Plane.id)  FROM Plane)";
                List<List<String>> maxResult = esql.executeQueryAndReturnResult(findmax);
                for(List<String> it : maxResult){
                    for(String st : it){
                        max = Integer.parseInt(st);
                    }
                }
                max++;
                System.out.print("\tEnter make: $");
                String make2;
		do{    
		    try{
                	make2 = in.readLine();
			if(make2.length() <= 0 || make2.length() >= 32) {
			    throw new RuntimeException();
			}
			break;
		    }catch (Exception e) {
			    System.out.println("Your input is invalid!");
			    continue;
		    }
		}while (true);

                System.out.print("\tEnter model: $");
                String model2;
		do{    
		    try{
                	model2 = in.readLine();
			if(model2.length() <= 0 || model2.length() >= 64) {
			    throw new RuntimeException();
			}
			break;
		    }catch (Exception e) {
			    System.out.println("Your input is invalid!");
			    continue;
		    }
		}while (true);

                System.out.print("\tEnter age that is greater than 0: $");
                String age2;
		do{    
		    try{
			age2 = read.nextLine();
			int x = Integer.parseInt(age2);
			if(x <= 0) {
			    throw new RuntimeException();
			}
			break;
		    }catch (Exception e) {
			    System.out.println("Your input is invalid!");
			    continue;
		    }
		}while (true);

                System.out.print("\tEnter number of seats greater than 0 and less than 500: $");
		int numSeats;
		do{    
		    try{
                	numSeats = read.nextInt();
			if(numSeats <= 0 || numSeats > 500) {
			    throw new RuntimeException();
			}
			break;
		    }catch (Exception e) {
			    System.out.println("Your input is invalid!");
			    continue;
		    }
		}while (true);

                query += max + ", \'" + make2 + "\', \'" + model2 + "\' , " + age2 + ", " + numSeats + ")";
                int num = esql.executeQuery(query);
            }catch(Exception e){
           System.err.println (e.getMessage());
	   }
        }

	public static void AddPilot(DBproject esql) {//2
	    try{
		int max = 0;
		String query = "INSERT INTO Pilot (id, fullname, nationality) VALUES (";
		String findmax = "(SELECT Max(Pilot.id) FROM Pilot)";
                List<List<String>> maxResult = esql.executeQueryAndReturnResult(findmax);
                for(List<String> it : maxResult){
                    for(String st : it){
                        max = Integer.parseInt(st);
                    }
                }
                max++;

		System.out.print("\tEnter pilot name: $");
		String name;
		do{    
		    try{
                	name = in.readLine();
			if(name.length() <= 0 || name.length() >= 128) {
			    throw new RuntimeException();
			}
			break;
		    }catch (Exception e) {
			    System.out.println("Your input is invalid!");
			    continue;
		    }
		}while (true);

		System.out.print("\tEnter pilot nationality: $");
		String nationality;
		do{    
		    try{
                	nationality = in.readLine();
			if(nationality.length() <= 0 || nationality.length() > 24) {
			    throw new RuntimeException();
			}
			break;
		    }catch (Exception e) {
			    System.out.println("Your input is invalid!");
			    continue;
		    }
		}while (true);
	

		query += max + ", \'" + name + "\', \'" + nationality + "\' )";

		int num = esql.executeQuery(query);
	    }catch(Exception e){
	   System.err.println (e.getMessage());
	   }
	}

	public static void AddFlight(DBproject esql) {//3
/*		// Given a pilot, plane and flight, adds a flight in the DB
	Scanner read = new Scanner(System.in);
	try{ 
                String query = "INSERT INTO Flight (fnum, cost, num_sold, num_stops, actual_departure_date, actual_arrival_date, arrival_airport, departure_airport) VALUES (";
                System.out.print("\tEnter Flight number: $");
                String flightNum = in.readLine();
                System.out.print("\tEnter Flight cost: $");
                int cost;
		do{    
		    try{
                	cost = read.nextInt();
			if(cost < 0) {
			    throw new RuntimeException();
			}
			break;
		    }catch (Exception e) {
			    System.out.println("Your input is invalid!");
			    continue;
		    }
		}while (true);

                System.out.print("\tEnter number of seats sold: $");
                String numSold = in.readLine();
                System.out.print("\tEnter number of stops during the flight: $");
                String numStops = in.readLine();
                System.out.print("\tEnter actual departure date: $");
                String departDate = in.readLine();
		System.out.print("\tEnter actual arrival date: $");
		String arriveDate = in.readLine();
		System.out.print("\tEnter arrival airport location: $");
		String arriveAirport = in.readLine();
		System.out.print("\tEnter Departure airport location: $");
		String departAirport = in.readLine();

                query += flightNum + ", '" + cost + ", " + numSold + ", " + numStops + ", " + numseats + ")";
                System.out.print(query);
                int num = esql.executeQuery(query);
            }catch(Exception e){
           System.err.println (e.getMessage());
	   }*/
	}

	public static void AddTechnician(DBproject esql) {//4
	    try{
		int max = 0;
		String query = "INSERT INTO Technician (id, full_name) VALUES (";
		String findmax = "(SELECT Max(Technician.id) FROM Technician)";
                List<List<String>> maxResult = esql.executeQueryAndReturnResult(findmax);
                for(List<String> it : maxResult){
                    for(String st : it){
                        max = Integer.parseInt(st);
                    }
                }
                max++;

		System.out.print("\tEnter Technician full name: $");
		String name;
		do{    
		    try{
                	name = in.readLine();
			if(name.length() <= 0 || name.length() > 128) {
			    throw new RuntimeException("enter a name between 0 and 128 characters");
			}
			break;
		    }catch (Exception e) {
			    System.out.println("Your input is invalid!");
			    continue;
		    }
		}while (true);


		query += max + ", \'" + name + "\' )";

		int num = esql.executeQuery(query);
	    }catch(Exception e){
	   System.err.println (e.getMessage());
	   }
	}

	public static void BookFlight(DBproject esql) {//5a
		// Given a customer and a flight that he/she wants to book, add a reservation to the 
	try{
               System.out.print("\tEnter Flight Number: $");
               String fl_id = in.readLine();
               System.out.print("\tEnter Customer ID: $");
               String cus_id = in.readLine();
               String query = "SELECT SUM(pl.seats - z.num_sold) AS seats_available FROM (SELECT * FROM Flight f, FlightInfo fl WHERE f.fnum = fl.flight_id AND f.fnum = " + fl_id + ")AS z, Plane pl WHERE z.plane_id = pl.id;";

               List<List<String>> query_out = esql.executeQueryAndReturnResult(query);
               int seats_avail = 0;
               for(List<String> it : query_out){
                   for(String st : it){
                       seats_avail = Integer.parseInt(st);
                   }
               }
               //System.out.print("this is seats_avail");
               //System.out.print(seats_avail);
               String findmax = "(SELECT Max(Reservation.rnum)  FROM Reservation)";
               List<List<String>> maxResult = esql.executeQueryAndReturnResult(findmax);
               int max = 0;
               for(List<String> it : maxResult){
                   for(String st : it){
                       max = Integer.parseInt(st);
                   }
               }
               max++;

               if(seats_avail > 0){
                   String sold_q = "SELECT num_sold FROM Flight WHERE Flight.fnum = " + fl_id + ";";
                   List<List<String>> new_sold = esql.executeQueryAndReturnResult(sold_q);
                   int seats_act = 0;
                   for(List<String> it : new_sold ){
                       for(String st : it){
                           seats_act = Integer.parseInt(st);
                       }
                   }
                   seats_act++;
                   System.out.print(seats_act);
                   String updateQ = "UPDATE Flight SET num_sold = num_sold + 1 WHERE fnum = " + fl_id + ";";
                   String makeres = "INSERT INTO Reservation(rnum, cid, fid, status) VALUES(" + max + ", " + cus_id + ", " + fl_id + ", \'R\');";
                   try{
                       int wr = esql.executeQuery(updateQ);
                   }catch(SQLException e){
                    System.err.println(e.getMessage());
                    }
                   try{
                       int mr = esql.executeQuery(makeres);
                   }catch(SQLException e){
                    System.err.println(e.getMessage());
                    }
                   //String updateQ = "UPDATE Flight SET num_sold = 1 WHERE fnum = 12";
                   //wr = esql.executeQuery(makeres);
               }
               //if(seats_avail > 0){
                   //String makeres = "INSERT INTO Reservation(rnum, cid, fid, status) VALUES(" + max + ", " + cus_id + ", " + fl_id + ", \'R\');";
                   //int mr = esql.executeQuery(makeres);
               //}
               else{
                   String makeres = "INSERT INTO Reservation(rnum, cid, fid, status) VALUES(" + max + ", " + cus_id + ", " + fl_id + ", \'W\'";
                   int rows4wait = esql.executeQuery(makeres);
               }

           }catch(Exception e){
           System.err.println(e.getMessage());
           }
        }	

	public static void ListNumberOfAvailableSeats(DBproject esql) {//6
		// For flight number and date, find the number of availalbe seats (i.e. total plane capacity minus booked seats )
		Scanner read = new Scanner(System.in);
            try{
                System.out.print("\tEnter a flight number: $");
                String flightNum;

                do{
                    try{
                        int flightQuery = 0;
                        String findFlight = "SELECT fnum FROM Flight WHERE fnum = \'";
                        flightNum = in.readLine();
                        findFlight += flightNum + "\'";
                        flightQuery = esql.executeQuery(findFlight);
                        if(flightQuery == 0){
                            throw new RuntimeException();
                        }
                        break;
                    }catch(Exception e){
                        System.out.println("Your input is invalid!");
                        continue;
                    }
                }while(true);
		
		System.out.print("\tEnter a date following the format YYYY-MM-DD: $");
		String userDate = in.readLine();

		String query = "SELECT SUM(pl.seats - z.num_sold) AS seats_available FROM (SELECT * FROM Flight f, FlightInfo fl WHERE f.fnum = fl.flight_id AND f.fnum = \'" + flightNum + "\')" + "AS z, Plane pl WHERE z.plane_id = pl.id;";
		int num = esql.executeQueryAndPrintResult(query);

		
	    }catch(Exception e){
	   System.err.println(e.getMessage());
	   }
	}

	public static void ListsTotalNumberOfRepairsPerPlane(DBproject esql) {//7
		// Count number of repairs per planes and list them in descending order
	    try{
		String query = "(SELECT P.id, COUNT(R.rid) FROM Plane P, Repairs R GROUP BY P.id) ORDER BY COUNT(R.rid) DESC";
		int num = esql.executeQueryAndPrintResult(query); 
	    }catch(Exception e){
	   System.err.println(e.getMessage());
	   }
	}

	public static void ListTotalNumberOfRepairsPerYear(DBproject esql) {//8
		// Count repairs per year and list them in ascending order
	    try{
		String query = "SELECT EXTRACT(YEAR FROM repair_date), COUNT(*) FROM REPAIRS GROUP BY (EXTRACT(YEAR FROM repair_date)) ORDER BY COUNT(*) ASC";
		int num = esql.executeQueryAndPrintResult(query); 
	    }catch(Exception e){
	   System.err.println(e.getMessage());
	   }

	}
	
	public static void FindPassengersCountWithStatus(DBproject esql) {//9
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number
	    Scanner read = new Scanner(System.in);
	    try{
		System.out.print("\tEnter a flight number: $");
		String flightNum;

		do{
		    try{
			int flightQuery = 0;
			String findFlight = "SELECT fnum FROM Flight WHERE fnum = \'";
			flightNum = in.readLine();
			findFlight += flightNum + "\'";
                        try{
			    flightQuery = esql.executeQuery(findFlight);
                        }catch(SQLException e){
                        System.err.println(e.getMessage());
                        }
			if(flightQuery == 0){
			    throw new RuntimeException();
			}
		        break;
		    }catch(Exception e){
			System.out.println("Your input is invalid! Try again");
			continue;
		    }
		}while(true);

		int flag = 0;
		System.out.print("\tEnter a status: W, C, or R: $");
		String userStat;
		char stat;

		do{    
		    try{
			userStat = in.readLine();
                	stat = userStat.charAt(0);
			/*if(stat.length() < 1 || stat.length() > 1) {
			    throw new RuntimeException();
			}*/
			if(userStat.length() > 1){
			    throw new RuntimeException();
			}
			else if(stat == 'C' || stat == 'W' || stat == 'R' || stat == 'r' || stat == 'c' || stat == 'w'){
				flag = 1;
			}
			if(flag == 0){
			    throw new RuntimeException();
			}
			break;
		    }catch (Exception e) {
			    System.out.println("Your input is invalid! Try again");
			    continue;
		    }
		}while (true);
                
                try{
		    String query = "SELECT COUNT(*) FROM Reservation R WHERE R.status = 'C' AND R.fid = "+ flightNum;
		    int num = esql.executeQueryAndPrintResult(query); 
                }catch(SQLException e){
                System.err.println(e.getMessage());
                }
	    }catch(Exception e){
	   System.err.println(e.getMessage());
	   }
	
	}
}
