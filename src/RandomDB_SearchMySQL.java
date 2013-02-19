import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;


// ODocument vs OIdentifiable
// OSQLSyncQuery vs OSQLAsynchQuery vs OCommand



public class RandomDB_SearchMySQL {
   	final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   	final String DB_URL = "jdbc:mysql://localhost:3307/";
   	Connection db;
   	Statement stmt;
   	String DB_NAME;

	//  Database credentials
	static final String USER = "lexgrid";
	static final String PASS = "lexgrid";
   	
   	RandomDB_Environment env;
	int iterations, depth;
	String databaseDirectory, fileDirectory;
	Long [] randomRID_list;

	String performancePath = "/home/m113216/scratch/";
	String performanceFile = "mysql_results.txt";
	File performance;
	FileWriter fstream;
	BufferedWriter out;
	
	public enum QueryType {TRAVERSE, UNION, INTERSECTION, DIFFERENCE, MINUS};
	
	public static void main(String[] args) {
		int iterations = 10;
		int minDepth = 10, maxDepth = 10;
		boolean print = false;
		boolean storeEdges = false;
		QueryType query = QueryType.TRAVERSE;
		
		// small, medium, large, huge
		String [] size = {"small", "medium", "large", "huge"}; 
		int sizeIndex = 0;
		
		RandomDB_SearchMySQL searchDB;
		long memory = Runtime.getRuntime().totalMemory();
		
		try{
//			for(int i=0; i < size.length; i++){
			for(int i=0; i < 1; i++){
				System.out.println("-----------------------------");
				System.out.println("Connecting to " + size[i]);
				boolean completed = true;
				for(int depth=minDepth; completed && depth <= maxDepth; depth++){
					searchDB = new RandomDB_SearchMySQL(iterations, depth, size[i]);
					if(storeEdges){
						System.out.println("Collecting edges to tempEdges");
						searchDB.createEdgeTable();
					}
					
						
					System.out.print("Iterations = " + iterations + ", depth = " + depth);
					System.out.println(", Database: " + searchDB.env.DB_PATH + "\n");
					searchDB.printToFile(memory + ", " + searchDB.DB_NAME + ", " + searchDB.stringQueryType(query) + ", " + iterations + ", " + depth);
					searchDB.openDatabase();
						
	//				System.out.println("Collecting random RIDs");
					searchDB.collectRandomRIDs();
					completed = searchDB.timePerformance(query);
					searchDB.closeDatabase();
					searchDB.closeFiles();
				}
			}
		} catch(Exception e){
			System.out.println("Some sort of error occurred.");
			e.printStackTrace();
		}
		System.out.println("Done");
	}

	// Constructor
	public RandomDB_SearchMySQL(int i, int d, String size){
		iterations = i;
		depth = d;
		databaseDirectory = "/home/m113216/orient/databases/randomDB_" + size;
		fileDirectory = "/home/m113216/orient/datafiles/randomDB_" + size + "/";
		env = new RandomDB_Environment(fileDirectory);
		DB_NAME = env.DB_NAME_PREFIX + env.DB_SIZE;
//		openDatabase();
		boolean addHeaders  = true;
//		performance = new File(performancePath + size + "_" + performanceFile);
		performance = new File(performancePath + performanceFile);
//		if(performance.exists()) performance.delete();
		if(performance.exists()) addHeaders = false;
		try{
			fstream = new FileWriter(performance, true);
			out = new BufferedWriter(fstream);
			if(addHeaders){
				printToFile("HeapSize, DB_Name, Query, Iterations, Depth, #V, #E, AvgRecs, AvgMS, AvgMin, AvgSec\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
			
	}

	public void closeFiles(){
//		System.out.println("Closing Files");
		try {
			out.flush();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void openDatabase(){
		   try{
			   Class.forName("com.mysql.jdbc.Driver");
//			   System.out.println("Connecting to database...");
			   db = DriverManager.getConnection(DB_URL, USER, PASS);
			   stmt = db.createStatement();
			   stmt.executeUpdate("USE " + DB_NAME);
		   }catch(SQLException se){
			   se.printStackTrace();
		   }catch(ClassNotFoundException ce){
			   ce.printStackTrace();
		   }
	}
	
	public void closeDatabase(){
		try {
			stmt.close();
			db.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void collectRandomRIDs(){
		// Add #iterations plus one for the set operations to have two subgraphs per iteration
		int numRIDs = iterations + 1;
		randomRID_list = new Long[numRIDs];
		String randomTableName, primaryKey, sql;
		ResultSet idRange;
		long randomPosition;
		long minID, maxID;
		// Collect #iterations of random RID's
		for(int i=0; i < numRIDs; i++){
			randomTableName = env.VERTEX_PREFIX + (int) (Math.random() * env.NUM_VERTEX_TYPE);
			primaryKey = randomTableName + "_ID";
			try {
				stmt = db.createStatement();
				sql = "Select min(" + primaryKey + ") as minID, max(" + primaryKey + ") as maxID from " + randomTableName;
//				System.out.println(sql);
				idRange = stmt.executeQuery(sql);
								
				if(idRange.next()){
					minID = idRange.getInt("minID");
					maxID = idRange.getInt("maxID");
					
					randomPosition = (long) (Math.random() * (maxID)) + minID;
					randomRID_list[i] = randomPosition;
				}
				else{
					System.out.println("No min/max values returned");
				}
				
				idRange.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	private void setRandomRID(int i, long value){
		randomRID_list[i] = value;
	}
	
	private void printToFile(String msg){
		try {
			out.write(msg);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	
	private boolean timePerformance(QueryType query){
		ResultSet results = null;
		
		long [] times = new long[iterations];
		int numRecords = 0;
		
		String sql, sql_traverse1, sql_traverse2;
		long startTime = 0, endTime = 0;
		int minutes, seconds;
		
		System.out.println("Timing " + stringQueryType(query) + " of graph with " + env.TOTAL_VERTICES + " vertices");
		printToFile(", " + env.TOTAL_VERTICES + ", " + env.TOTAL_EDGES);
		long totalTimes = 0;
		boolean outOfMemory = false;
		for(int i=0; i < iterations; i++){
			System.out.print((i + 1) + " ... ");
			sql = createSQL_Query(query, randomRID_list[i], randomRID_list[i+1]);
			System.out.println(sql);
			
			try {
				// Time query
				startTime = System.currentTimeMillis();
				results = stmt.executeQuery(sql);
				endTime = System.currentTimeMillis();

//				results.close();
				sql = "Select found_rows()";
				results = stmt.executeQuery(sql);
				results.next();
				numRecords += results.getInt(1);
				results.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (OutOfMemoryError oome){
				System.out.println("Out of memory");
				outOfMemory = true;
				break;
			}

//			if(results != null)			numRecords += results.size();
			
			times[i] = endTime - startTime;
			minutes = (int) (times[i] / (1000 * 60));
			seconds = (int) ((times[i] / 1000) % 60);
			totalTimes += times[i];
		}
		System.out.println();
		
		if(!outOfMemory){
			long avgTime = totalTimes / iterations;
			minutes = (int) (avgTime / (1000 * 60));
			seconds = (int) ((avgTime / 1000) % 60);
			
			System.out.println("Average #records: " + (numRecords / iterations) + " of " + env.TOTAL_VERTICES);
			System.out.println(String.format("Average Time: %d ms or (%d min, %d sec)\n", avgTime, minutes, seconds)); 
	
			printToFile(", " + (numRecords / iterations));
			printToFile(", " + avgTime + ", " + minutes + ", " + seconds + "\n"); 
		}
		try {
			out.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return !outOfMemory;
	}

	
	private void printRecords(ResultSet records){	
		System.out.println("Printing records: ");
		ResultSetMetaData metaData;
		try {
			metaData = records.getMetaData();
			for(int i=0; i < metaData.getColumnCount(); i++){
				System.out.print(metaData.getColumnLabel(i+1) + "\t");
			}
			System.out.println();
			while(records.next()){
				for(int i=0; i < metaData.getColumnCount(); i++){
					System.out.print(records.getString(metaData.getColumnLabel(i+1)) + "\t");
				}
				System.out.println();
			}
			
			System.out.println("Done\n\n");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void createEdgeTable(){
		String sql;
		ResultSet rs;
		try {
//			stmt = db.createStatement();
			sql = "DROP TABLE IF EXISTS tempEdges";
			System.out.println(sql);
			stmt.execute(sql);
			
//			sql = "show tables like 'tempEdges'";
//			rs = stmt.executeQuery(sql);
//			if(rs.next()){
//				// Do nothing, table exists.
//			}
//			else {
				sql = "CREATE TABLE tempEdges ";
				sql += " (edgeID BIGINT NOT NULL, edgeIN BIGINT NOT NULL, edgeOUT BIGINT NOT NULL)"; 
				
				System.out.println(sql);
				stmt.execute(sql);
			
				
				sql = "show tables like 'Edge%'";
//				rs.close();
//				rs = stmt.executeQuery(sql);
				/*
				if(rs != null){
					while(rs.next()){
//						System.out.println(rs.getString(1));
						sql = "insert into tempEdges (edgeID, edgeIN, edgeOUT) select " + rs.getString(1) + "_ID, edgeIN, edgeOUT from ";
						sql += rs.getString(1);
						System.out.println(sql);
						stmt.executeUpdate(sql);
					}
				}
				*/
				
				sql = "insert into tempEdges (edgeID, edgeIN, edgeOUT) select Edge_0_ID, edgeIN, edgeOUT from Edge_0";
				System.out.println(sql);
				stmt.executeUpdate(sql);
				sql = "insert into tempEdges (edgeID, edgeIN, edgeOUT) select Edge_1_ID, edgeIN, edgeOUT from Edge_1";
				System.out.println(sql);
				stmt.executeUpdate(sql);
				
				System.out.println("Creating index for tempEdges");
				sql = "create index idx_tempEdges on tempEdges (edgeIN, edgeOUT)";
				stmt.executeUpdate(sql);
//				rs.close();
//			}			
//			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String createSQL_Traverse(long id){
		String sql = "select ";
		String sqlFields = "", sqlTables = "", sqlWhere = "", sqlOrder = "";
		String comma = "";
		for(int i=1; i <= depth; i++){
			sqlFields += comma + "L" + i + ".edgeOUT as L" + i + "_OUT, L" + i + ".edgeIN as L" + i + "_IN";
			sqlTables += comma + "tempEdges as L" + i;
			sqlOrder += comma + "L" + i + ".edgeOUT";
			if(i>1){
				sqlWhere += "L" + (i-1) + ".edgeIN = L" + i + ".edgeOUT and ";
			}
			comma = ", ";
		}
		
		sql = "Select " + sqlFields + " from " + sqlTables + " where " + sqlWhere + " L1.edgeOUT = " + id;
		sql += " order by " + sqlOrder;
		return sql;
	}
	
	private String createSQL_Query(QueryType operation, long id1, long id2){
		String traverse1, traverse2, sql = "";
		traverse1 = createSQL_Traverse(id1);
//		traverse2 = createSQL_Traverse(id2);
		
		
		if(operation == QueryType.TRAVERSE)
			return traverse1;
		
//		if(operation == QueryType.INTERSECTION)	// DONE		
//			sql = "SELECT FROM (" + traverse1 + ") WHERE rid1 IN (" + traverse2 + ")";
//		else if(operation == QueryType.MINUS) // DONE
//			sql = "SELECT FROM (" + traverse1 + ") WHERE rid1 NOT IN (" + traverse2 + ")";
//		else if(operation == QueryType.UNION) { 
//			String rids = "[" + id2 + "," + id1 + "]";	
//			sql = "SELECT FROM (" + traverse1 + ") WHERE rid1 IN (" + traverse2 + ") OR rid1 IN (" + traverse1 + ")";
//			sql = "SELECT @RID as rid1 FROM V WHERE @RID IN (" + traverse1 + ") OR @RID IN (" + traverse2 + ")";
//		}
//		else if(operation == QueryType.DIFFERENCE){
//			String union = createSQL_Query(QueryType.UNION, id1, id2);
//			String intersect = createSQL_Query(QueryType.INTERSECTION, id1, id2);
//			sql = "SELECT FROM (" + union + ") WHERE rid1 NOT IN (" + intersect + ")";
//		}
//		
		return sql;
	}
	
	private String stringQueryType(QueryType t){
		String type = "";
		
		if(t == QueryType.TRAVERSE) type = "TRAVERSE";
		else if(t == QueryType.DIFFERENCE) type = "DIFFERENCE";
		else if(t == QueryType.MINUS) type = "MINUS";
		else if(t == QueryType.UNION) type = "UNION";
		
		return type;
	}
	
	private void testQuery(QueryType query, int id1, int id2, boolean print){
		String sql;
		ResultSet records = null;
		String message = "";
		sql = createSQL_Query(query, randomRID_list[id1], randomRID_list[id2]);
		
		System.out.print("Root node1 = " + randomRID_list[id1]);
		if(query != QueryType.TRAVERSE) message += ", Root node2 = " + randomRID_list[id2];
		System.out.println(message);
		
		if(print) System.out.println(sql);
			
		try {
			records = stmt.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		System.out.println("# Records = " + records.);
		if(print) printRecords(records);
		try {
			records.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
