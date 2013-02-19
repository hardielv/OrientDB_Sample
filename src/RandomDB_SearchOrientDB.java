import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;


public class RandomDB_SearchOrientDB {
	OGraphDatabase db;
	RandomDB_Environment env;
	int iterations, depth;
	String databaseDirectory, fileDirectory;
	String [] randomRID_list;
	String DB_NAME;
	
	String performancePath = "/home/m113216/scratch/";
	String performanceFile = "orientDB_results.txt";
	File performance;
	FileWriter fstream;
	BufferedWriter out;

	public enum QueryType {TRAVERSE, UNION, INTERSECTION, DIFFERENCE, MINUS};
	
	public static void main(String[] args) {
		int iterations = 1;
		int minDepth = 10, maxDepth = 10;
		boolean print = false;
		boolean storeEdges = false;
				
		// small, medium, large, huge
//		String [] size = {"small", "medium", "large", "huge"}; 
		String [] size = {"huge"}; 
//		QueryType [] queryList = {QueryType.TRAVERSE, QueryType.MINUS, QueryType.INTERSECTION, QueryType.UNION, QueryType.DIFFERENCE};
		QueryType [] queryList = {QueryType.UNION, QueryType.DIFFERENCE};
//		String [] size = {"huge"}; 
		int sizeIndex = 0;
		
		RandomDB_SearchOrientDB searchDB; 
		long memory = Runtime.getRuntime().totalMemory();


		List<OIdentifiable> records = null;
		try{
			for(int i=0; i < size.length; i++){
				System.out.println("-----------------------------");
				System.out.println("Connecting to " + size[i]);
				boolean completed = true;
				for(int k=0; k < queryList.length; k++){
					for(int depth=minDepth; completed && depth <= maxDepth; depth++){
						searchDB = new RandomDB_SearchOrientDB(iterations, depth, size[i]);
						
						System.out.print("Iterations = " + iterations + ", depth = " + depth);
						System.out.println(", Database: " + searchDB.env.DB_PATH);
						searchDB.printToFile(memory + ", " + searchDB.DB_NAME + ", " + searchDB.stringQueryType(queryList[k]) + ", " + iterations + ", " + depth);
						
						searchDB.openDatabase();
						searchDB.collectRandomRIDs();
						completed = searchDB.timePerformance(queryList[k]);
	
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
	public RandomDB_SearchOrientDB(int i, int d, String size){
		iterations = i;
		depth = d;
		databaseDirectory = "/home/m113216/orient/databases/randomDB_" + size;
		fileDirectory = "/home/m113216/orient/datafiles/randomDB_" + size + "/";
		env = new RandomDB_Environment(fileDirectory);
		DB_NAME = env.DB_NAME_PREFIX + size;
		boolean addHeaders  = true;
		performance = new File(performancePath + performanceFile);
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
		File dir = new File(env.DB_PATH);
		if(!dir.exists()){
			System.out.println("Database does not exist");
			System.exit(1);
		}
		
		db = new OGraphDatabase("local:" + env.DB_PATH);
		db.open("admin",  "admin");
	}
	
	public void closeDatabase(){
		db.close();
	}
	
	private void collectRandomRIDs(){
		// Add #iterations plus one for the set operations to have two subgraphs per iteration
		int numRIDs = iterations + 1;
		randomRID_list = new String[numRIDs];
		String randomClusterName;
		int clusterID, randomPosition;
		
		// Collect #iterations of random RID's
		for(int i=0; i < numRIDs; i++){
			randomClusterName = env.VERTEX_PREFIX + (int) (Math.random() * env.NUM_VERTEX_TYPE);
			clusterID = db.getClusterIdByName(randomClusterName); 
			OClusterPosition [] range = db.getStorage().getClusterDataRange(clusterID);
			
			randomPosition = (int) (Math.random() * range[1].intValue()) + range[0].intValue();
			randomRID_list[i] = "#" + clusterID + ":" + randomPosition;
		}
		
	}

	private void setRandomRID(int i, String value){
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



	private void traverse_Iterator(){
		int numRecords = 0, records = 0;
		long [] times = new long[iterations];		
		long totalTimes = 0;
		long startTime, endTime;
		int minutes, seconds;
//		String rid = "#11:272915";
//		depth = 8;
		OIdentifiable id;
		System.out.println("Timing Iterator of " + iterations + " traversals of graph with " + env.TOTAL_VERTICES + " vertices");
		System.out.println("With depth = " + depth);
		
		for(int i=0; i < iterations; i++){
			records = 0;
//			System.out.println("Working with: " + rid);
			System.out.println("Working with: " + randomRID_list[i]);
			startTime = System.currentTimeMillis();
			Iterator<? extends OIdentifiable> g = new OTraverse().target(new ORecordId(randomRID_list[i])).fields("V.out", "V.in", "E.out").predicate(new OSQLPredicate("$depth <= " + depth));
//			OTraverse graph = new OTraverse().target(new ORecordId(rid)).fields("V.out", "V.in", "E.out").predicate(new OSQLPredicate("$depth <= " + depth));
//			OTraverse graph = new OTraverse().target(new ORecordId(rid)).fields("out", "in").predicate(new OSQLPredicate("$depth <= " + depth));
//			Iterator<? extends OIdentifiable> g = graph;
//			System.out.println("First ID: " + g.next());
//			graph.execute();

			while(g.hasNext()){
				id=g.next();
			System.out.println("ID: " + id);
			}
			
//			for(int j=0; j < 10 && g.hasNext(); j++){
//				id=g.next();
////				System.out.println("ID: " + id);
//			}
			//			for(OIdentifiable id : graph){
//				records++;
//			}
//			for (OIdentifiable id : new OTraverse()
//									.target(new ORecordId(randomRID_list[i]))
//									.fields("out", "in")
//									.predicate( new OSQLPredicate("$depth <= " + depth))) {
//	
//				System.out.println( id);
//				records++;
//			}				
			endTime = System.currentTimeMillis();
			
			numRecords += records;			
			times[i] = endTime - startTime;
			minutes = (int) (times[i] / (1000 * 60));
			seconds = (int) ((times[i] / 1000) % 60);
			totalTimes += times[i];
		}
		
		long avgTime = totalTimes / iterations;
		minutes = (int) (avgTime / (1000 * 60));
		seconds = (int) ((avgTime / 1000) % 60);
		
		System.out.println(iterations + " iterations, " + depth + " depth");
		System.out.println("Average #records: " + (numRecords / iterations) + " of " + env.TOTAL_VERTICES);
		System.out.println(String.format("Average Time: %d ms or (%d min, %d sec)", avgTime, minutes, seconds)); 
		System.out.println();					
	}

	
	private boolean timePerformance(QueryType query){
		List<OIdentifiable> results = null;
		
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
//			System.out.println(sql);

			try{
				// Time query
				startTime = System.currentTimeMillis();
				results = db.query(new OSQLSynchQuery<OIdentifiable>(sql));
				endTime = System.currentTimeMillis();
	
				if(results != null)			numRecords += results.size();
				
				
			} catch (OutOfMemoryError oome){
				System.out.println("Out of memory");
				outOfMemory = true;
				break;
			}
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
			System.out.println(String.format("Average Time: %d ms or (%d min, %d sec)", avgTime, minutes, seconds)); 

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

	
	private void printRecords(List<OIdentifiable> records){	
		System.out.println("Printing records: ");
		for(OIdentifiable record : records) {
			System.out.println(record.toString());
		}
		System.out.println("Done\n\n");
	}

	private String createSQL_Traverse(String id){
//		sql = "select from " + randomRID_list[i] + " where any() traverse(0," + depth + ") (@class like 'Vertex%')";			
//		sql = "select @RID from (traverse in, out from " + randomRID_list[i] + " WHILE $depth <= " + depth + ") where @class like 'Vertex%'";			
//		sql = "traverse V.in, E.out from " + randomRID_list[i] + " WHILE $depth <= " + depth;			
//		sql = "traverse V.out, E.in from " + randomRID_list[i] + " WHILE $depth <= 10";
//		sql = "Select V.out, V.in from (traverse V.out, V.in, E.out, E.in from " + randomRID_list[i] + " WHILE $depth <= " + depth + ")";

		String sql;
		
		sql =  "SELECT @RID AS rid1, label, $depth FROM ";
		sql +=     "(TRAVERSE V.in, V.out, E.out FROM " + id + " WHILE $depth <= " + depth + ") ";
		sql += " WHERE label LIKE 'Vertex%'";
		return sql;
	}
	
	private String createSQL_Query(QueryType operation, String id1, String id2){
		String traverse1, traverse2, sql = "";
		traverse1 = createSQL_Traverse(id1);
		traverse2 = createSQL_Traverse(id2);
		if(operation == QueryType.TRAVERSE)
			return traverse1;
		
		if(operation == QueryType.INTERSECTION)	// DONE		
			sql = "SELECT FROM (" + traverse1 + ") WHERE rid1 IN (" + traverse2 + ")";
		else if(operation == QueryType.MINUS) // DONE
			sql = "SELECT FROM (" + traverse1 + ") WHERE rid1 NOT IN (" + traverse2 + ")";
		else if(operation == QueryType.UNION) { 
			sql = "SELECT @RID as rid1 FROM V WHERE @RID IN (" + traverse1 + ") OR @RID IN (" + traverse2 + ")";
		}
		else if(operation == QueryType.DIFFERENCE){
			String union = createSQL_Query(QueryType.UNION, id1, id2);
			String intersect = createSQL_Query(QueryType.INTERSECTION, id1, id2);
			sql = "SELECT FROM (" + union + ") WHERE rid1 NOT IN (" + intersect + ")";
		}
		
		return sql;
	}
	
	private String stringQueryType(QueryType t){
		String type = "";
		
		if(t == QueryType.TRAVERSE) type = "TRAVERSE";
		else if(t == QueryType.DIFFERENCE) type = "DIFFERENCE";
		else if(t == QueryType.INTERSECTION) type = "INTERSECTION";
		else if(t == QueryType.MINUS) type = "MINUS";
		else if(t == QueryType.UNION) type = "UNION";
		
		return type;
	}
	
	private void testQuery(QueryType query, int id1, int id2, boolean print){
		String sql;
		List<OIdentifiable> records = null;

		sql = createSQL_Query(query, randomRID_list[id1], randomRID_list[id2]);
		sql += " ORDER BY rid1";
		if(print) System.out.println(sql);
			
		records = db.query(new OSQLSynchQuery<OIdentifiable>(sql));
		System.out.println("# Records = " + records.size());
		if(print) printRecords(records);
	}
}
