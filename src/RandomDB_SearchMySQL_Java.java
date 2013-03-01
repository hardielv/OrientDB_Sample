import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.management.MBeanServerConnection;


@SuppressWarnings("unused")
public class RandomDB_SearchMySQL_Java {
	// DATABASE TYPE SPECIFIC
	String DB_TYPE = "mysql";
	final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   	final String DB_URL = "jdbc:mysql://localhost:3307/";
   	Connection db;
   	Statement stmt;
	//  Database credentials
	static final String USER = "lexgrid";
	static final String PASS = "lexgrid";
	
	HashSet<Long> GRAPH1, GRAPH2;
   	
	// GENERAL
   	String DB_NAME;

	String performanceFile = DB_TYPE + "_results.txt";
	String performancePath = "/home/m113216/scratch/";

//	final String HEADERS = "HeapSize, DB_Name, Query, Iterations, Depth, #V, #E, #Processors, LoadBefore, LoadAfter, AvgRecs, AvgMS, AvgMin, AvgSec\n";
	final String HEADERS = "HeapSize, DB_Name, Query, Iterations, Depth, #V, #E, AvgRecs, AvgMS, AvgMin, AvgSec\n";
	RandomDB_Environment env;
	String databaseDirectory, fileDirectory;
	Long [] randomRID_list;
	int iterations, depth;

	File performance;
	FileWriter fstream;
	BufferedWriter out;

	public enum QueryType {TRAVERSE, UNION, INTERSECTION, DIFFERENCE, SYMMETRIC_DIFFERENCE};
	
	public static void main(String[] args) {
		int iterations = 5;
		int minDepth = 10, maxDepth = 10;
		boolean print = false;
		boolean storeEdges = false;
		
//		String [] size = {"small", "medium", "large", "huge"}; 
		String [] size = {"large"}; 
//		QueryType [] queryList = {QueryType.TRAVERSE, QueryType.INTERSECTION, QueryType.UNION, QueryType.DIFFERENCE};
		QueryType [] queryList = {QueryType.TRAVERSE};
		int sizeIndex = 0;
		
		RandomDB_SearchMySQL_Java searchDB;
		long memory = Runtime.getRuntime().totalMemory();
		
		try{
			for(int i=0; i < size.length; i++){
				System.out.println("-----------------------------");
				System.out.println("Connecting to " + size[i]);
				boolean completed = true;
				for(int k=0; k < queryList.length; k++){
					for(int depth=minDepth; completed && depth <= maxDepth; depth++){
						searchDB = new RandomDB_SearchMySQL_Java(iterations, depth, size[i]);
						
							
						System.out.print("Iterations = " + iterations + ", depth = " + depth);
						System.out.println(", Database: " + searchDB.env.DB_PATH);
						searchDB.printToFile(memory + ", " + searchDB.DB_NAME + ", " + searchDB.stringQueryType(queryList[k]) + ", " + iterations + ", " + depth);
						
						searchDB.openDatabase();	
						// NEEDED ONLY FOR MYSQL
						if(storeEdges){
							System.out.println("Collecting edges to tempEdges");
							searchDB.createEdgeTable();
						}
						// --------------------
						searchDB.collectRandomRIDs();
						completed = searchDB.timePerformance(queryList[k], depth);
						
						searchDB.closeDatabase();
						searchDB.closeFiles();
					}
					System.out.println();
				}
			}
		} catch(Exception e){
			System.out.println("Some sort of error occurred.");
			e.printStackTrace();
		}
		System.out.println("Done");
	}

	// Constructor
	public RandomDB_SearchMySQL_Java(int i, int d, String size){
		iterations = i;
		depth = d;
//		databaseDirectory = "/home/m113216/orient/databases/randomDB_" + size;
		fileDirectory = "/home/m113216/datafiles.HOLD/randomDB_" + size + "/";
		env = new RandomDB_Environment(fileDirectory);
		DB_NAME = env.DB_NAME_PREFIX + env.DB_SIZE;
		
		GRAPH1 = new HashSet<Long>();
		GRAPH2 = new HashSet<Long>();
		
		boolean addHeaders  = true;
		performance = new File(performancePath + performanceFile);
		if(performance.exists()) addHeaders = false;
		try{
			fstream = new FileWriter(performance, true);
			out = new BufferedWriter(fstream);
			if(addHeaders){
				printToFile(HEADERS);
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

	private void traverseJava(Long root, int level, int graphID) throws OutOfMemoryError{
		String sql;
		Long id;
		if(graphID == 1){
			GRAPH1.add(root);
		}
		else{
			GRAPH2.add(root);
		}
		if(level <= depth){
			sql = "Select edgeOUT from tempEdges where edgeIN = " + root;
			Statement stmt2;
			try {
				stmt2 = db.createStatement();
				ResultSet resultSet = stmt2.executeQuery(sql);
				while(resultSet.next()){
					id = resultSet.getLong("edgeOUT");
					traverseJava(id, level+1, graphID);
				}
				resultSet.close();
				stmt2.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private HashSet<Long> runQuery(QueryType query, int index) throws OutOfMemoryError{
		GRAPH1.clear(); // = new HashSet<Long>();
		
		traverseJava(randomRID_list[index], 0, 1);

		if(query != QueryType.TRAVERSE){
			GRAPH2.clear(); // = new HashSet<Long>(); 
			traverseJava(randomRID_list[index + 1], 0, 2);
			if(query == QueryType.UNION){
				GRAPH1.addAll(GRAPH2);
			}
			else if(query == QueryType.DIFFERENCE){
				GRAPH1.removeAll(GRAPH2);
			}
			else if(query == QueryType.INTERSECTION){
				GRAPH1.retainAll(GRAPH2);				
			}
			else if(query == QueryType.SYMMETRIC_DIFFERENCE){
				Set<Long> symmetricDiff = new HashSet<Long>(GRAPH1);
				symmetricDiff.addAll(GRAPH2);
				Set<Long> tmp = new HashSet<Long>(GRAPH1);
				tmp.retainAll(GRAPH2);
				symmetricDiff.removeAll(tmp);
				GRAPH1 = (HashSet<Long>) symmetricDiff;
			}
		}
		
		return GRAPH1;
	}
	

	private boolean timePerformance(QueryType query, int depth){
//		HashMap<Long, Object> results;
		HashSet<Long> results = null;
		
		ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
		
		String sql, sql_traverse1, sql_traverse2;
		
		long [] times = new long[iterations];
		int numRecords = 0;
		long startTime = 0, endTime = 0;
		int minutes, seconds;
		long totalTimes = 0;

		long [] cpuTimes = new long[iterations];
		long cpuStart, cpuEnd;
		int cpuMin, cpuSec;
		long cpuTotalTimes = 0;
		
		
		
		

		System.out.println("Timing " + stringQueryType(query) + " of graph with " + env.TOTAL_VERTICES + " vertices");
		printToFile(", " + env.TOTAL_VERTICES + ", " + env.TOTAL_EDGES);
		boolean outOfMemory = false;
		for(int i=0; i < iterations; i++){
			System.out.print((i + 1) + " ... ");			
			try {
				// Time query
				cpuStart = threadBean.getCurrentThreadCpuTime();
				startTime = System.currentTimeMillis();
				//				results = traverseJava(randomRID_list[i], 0, depth);
				results = runQuery(query, i);
				endTime = System.currentTimeMillis();
				cpuEnd = threadBean.getCurrentThreadCpuTime();
					
				if(results != null)	{
					numRecords += results.size();
				}
					
			} catch (OutOfMemoryError oome){
				System.out.println("Out of memory");
				outOfMemory = true;
				break;
			}		
			times[i] = endTime - startTime;
			minutes = (int) (times[i] / (1000 * 60));
			seconds = (int) ((times[i] / 1000) % 60);
			totalTimes += times[i];
			
			cpuTimes[i] = (cpuEnd - cpuStart)/1000000;
			cpuMin = (int) (cpuTimes[i] / (1000 * 60));
			cpuSec = (int) ((cpuTimes[i] / 1000) % 60);
			cpuTotalTimes += cpuTimes[i];
		}
		
		System.out.println();
		
		if(!outOfMemory){
			long avgTime = totalTimes / iterations;
			long cpuAvg = cpuTotalTimes / iterations;
			minutes = (int) (avgTime / (1000 * 60));
			seconds = (int) ((avgTime / 1000) % 60);
			cpuMin = (int) (cpuAvg / (1000 * 60));
			cpuSec = (int) ((cpuAvg / 1000) % 60);

			//			#Processors, LoadBefore, LoadAfter, #V
			System.out.println("Average #records: " + (numRecords / iterations) + " with total =  " + env.TOTAL_VERTICES);
			System.out.println(String.format("Average Time: %d ms or (%d min, %d sec)\n", avgTime, minutes, seconds)); 
	
//			printToFile(", " + cpuAfter + ", " + (numRecords / iterations));
//			printToFile(", " + avgTime + ", " + minutes + ", " + seconds + "\n"); 
			printToFile(", " + (numRecords / iterations));
			printToFile(", " + avgTime + ", " + minutes + ", " + seconds);
			
			printToFile(", " + cpuAvg + ", " + cpuMin + ", " + cpuSec + "\n");
		}
		try {
			out.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return !outOfMemory;
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
	
	
	private String stringQueryType(QueryType t){
		String type = "";
		
		if(t == QueryType.TRAVERSE) type = "TRAVERSE";
		else if(t == QueryType.DIFFERENCE) type = "DIFFERENCE";
		else if(t == QueryType.INTERSECTION) type = "INTERSECTION";
		else if(t == QueryType.UNION) type = "UNION";
		else if(t == QueryType.SYMMETRIC_DIFFERENCE) type = "SYMMETRIC_DIFFERENCE";
		
		return type;
	}
	

	
	private HashMap<Long, Object> traverseJava_recursive(Long root, int level, int depth) throws OutOfMemoryError{
		String sql;
		boolean foundLeaf = false;

//		System.out.println("ROOT: " + root + ", LEVEL : " + level + ", DEPTH: " + depth);
		HashMap<Long, Object> graph = null;
		Long id; 
		if(level <= depth){
			sql = "Select edgeOUT from tempEdges where edgeIN = " + root;
//			System.out.println(sql);
			try {
				Statement stmt2 = db.createStatement();
				ResultSet resultSet = stmt2.executeQuery(sql);
				HashMap<Long, Object> subgraph = null;
				while(resultSet.next()){
					if(graph == null){
						graph = new HashMap<Long, Object>();
					}
	
					id = resultSet.getLong("edgeOUT");
					subgraph = traverseJava_recursive(id, level+1, depth);
					graph.put(id,  subgraph);
				}
				
				resultSet.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(level == 0){
			HashMap<Long, Object> top = new HashMap<Long, Object>();
			top.put(root, graph);
			return top;
		}
		
		return graph;
	}
	
	private int countNodes(HashMap<Long, Object> tree){
		int count = 0;
		if(tree != null){
			for (Long key : tree.keySet()) {
				HashMap<Long, Object> t = (HashMap<Long, Object>) tree.get(key);
				count += countNodes(t);
				count++;
			}
		}
		return count;
	}
	

	private void printNodes(HashMap<Long, Object> tree, int numSpaces){
		if(tree!=null){
			for(Long key:tree.keySet()){
				printSpaces(numSpaces);
				System.out.println(key);
				HashMap<Long, Object> t = (HashMap<Long, Object>) tree.get(key);
				printNodes(t, numSpaces+1);
			}
		}
	}
	
	private void printSpaces(int numSpaces){
		for(int i=0; i < numSpaces; i++){
			System.out.print(" ");
		}
	}
	
	private void createEdgeTable(){
		String sql;
		ResultSet rs;
		try {
//			stmt = db.createStatement();
			sql = "DROP TABLE IF EXISTS tempEdges";
			System.out.println(sql);
			
			Statement stmt2 = db.createStatement();
			
			stmt2.execute(sql);
			
//			sql = "show tables like 'tempEdges'";
//			rs = stmt.executeQuery(sql);
//			if(rs.next()){
//				// Do nothing, table exists.
//			}
//			else {
				sql = "CREATE TABLE tempEdges ";
				sql += " (edgeID BIGINT NOT NULL, edgeIN BIGINT NOT NULL, edgeOUT BIGINT NOT NULL)"; 
				
				System.out.println(sql);
				stmt2.execute(sql);
			
				
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
				stmt2.executeUpdate(sql);
				sql = "insert into tempEdges (edgeID, edgeIN, edgeOUT) select Edge_1_ID, edgeIN, edgeOUT from Edge_1";
				System.out.println(sql);
				stmt2.executeUpdate(sql);
				
				System.out.println("Creating index for tempEdges");
				sql = "create index idx_tempEdges on tempEdges (edgeIN, edgeOUT)";
				stmt2.executeUpdate(sql);
//				rs.close();
//			}			
//			rs.close();
			stmt2.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	
}
