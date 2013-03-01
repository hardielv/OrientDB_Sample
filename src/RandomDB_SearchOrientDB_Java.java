import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;


public class RandomDB_SearchOrientDB_Java {
	// DATABASE TYPE SPECIFIC
	String DB_TYPE = "orientdb";
	OGraphDatabase db;
	//  Database credentials
	static final String USER = "admin";
	static final String PASS = "admin";

	
	// GENERAL
	String DB_NAME;
	
	String performanceFile = DB_TYPE + "_results.txt";
	String performancePath = "/home/m113216/scratch/";

	RandomDB_Environment env;
	String databaseDirectory, fileDirectory;
	String [] randomRID_list;
	int iterations, depth;

	File performance;
	FileWriter fstream;
	BufferedWriter out;

	
	public enum QueryType {TRAVERSE, UNION, INTERSECTION, DIFFERENCE, SYMMETRIC_DIFFERENCE};
	
	public static void main(String[] args) {
		int iterations = 100;
		int minDepth = 8, maxDepth = 10;
				
//		String [] size = {"small", "medium", "large", "huge"}; 
		String [] size = {"large", "huge", "gigantic"}; 
		QueryType [] queryList = {QueryType.TRAVERSE, QueryType.INTERSECTION, QueryType.UNION, QueryType.DIFFERENCE};
//		QueryType [] queryList = {QueryType.TRAVERSE};
		
		RandomDB_SearchOrientDB_Java searchDB; 
		long memory = Runtime.getRuntime().totalMemory();

		try{
			for(int i=0; i < size.length; i++){
				System.out.println("-----------------------------");
				System.out.println("Connecting to " + size[i]);
				boolean completed = true;
				for(int k=0; k < queryList.length; k++){
					for(int depth=minDepth; completed && depth <= maxDepth; depth++){
						searchDB = new RandomDB_SearchOrientDB_Java(iterations, depth, size[i]);
						
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
	public RandomDB_SearchOrientDB_Java(int i, int d, String size){
		iterations = i;
		depth = d;
		databaseDirectory = "/home/m113216/orient/databases/randomDB_" + size;
		fileDirectory = "/home/m113216/datafiles.HOLD/randomDB_" + size + "/";
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



	
	private HashSet<String> traverseJava(String root) throws OutOfMemoryError{
		String sql;
		ArrayList<ODocument> results = null;
		HashSet<String> graph = null;
		String id;
		sql = createSQL_Traverse(root);
		results = db.query(new OSQLSynchQuery<OIdentifiable>(sql));

		graph = new HashSet<String>();
		for (int i = 0; i < results.size(); i++) {
			// System.out.println(results.get(i));
			id = ((ODocument) results.get(i).field("rid1")).getIdentity()
					.toString();
			// System.out.println(doc);
			// String id = doc.getIdentity().toString();
			// System.out.println(id);
			// System.out.println();
			graph.add(id);
		}
		
		return graph;
	}
	
	private HashSet<String> runQuery(QueryType query, int index) throws OutOfMemoryError{
		HashSet<String> graph1 = traverseJava(randomRID_list[index]);

		if(query != QueryType.TRAVERSE){
			HashSet<String> graph2 = traverseJava(randomRID_list[index + 1]);
			if(query == QueryType.UNION){
				graph1.addAll(graph2);
			}
			else if(query == QueryType.DIFFERENCE){
				graph1.removeAll(graph2);
			}
			else if(query == QueryType.INTERSECTION){
				graph1.retainAll(graph2);				
			}
			else if(query == QueryType.SYMMETRIC_DIFFERENCE){
				Set<String> symmetricDiff = new HashSet<String>(graph1);
				symmetricDiff.addAll(graph2);
				Set<String> tmp = new HashSet<String>(graph1);
				tmp.retainAll(graph2);
				symmetricDiff.removeAll(tmp);
				graph1 = (HashSet<String>) symmetricDiff;
			}
		}
		
		return graph1;
	}
	
	private boolean timePerformance(QueryType query){
		HashSet<String> results = null;
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
			try{
				// Time query
				cpuStart = threadBean.getCurrentThreadCpuTime();
				startTime = System.currentTimeMillis();
//				results = db.query(new OSQLSynchQuery<OIdentifiable>(sql));
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
			
			System.out.println("Average #records: " + (numRecords / iterations) + " of " + env.TOTAL_VERTICES);
			System.out.println(String.format("Average Time: %d ms or (%d min, %d sec)", avgTime, minutes, seconds)); 

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

	
	private String createSQL_Traverse(String id){
		String sql;
		
		sql =  "SELECT @RID AS rid1, label, $depth FROM ";
		sql +=     "(TRAVERSE V.in, V.out, E.out FROM " + id + " WHILE $depth <= " + depth + ") ";
		sql += " WHERE label LIKE 'Vertex%'";
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
	
	private void printRecords(List<OIdentifiable> records){	
		System.out.println("Printing records: ");
		for(OIdentifiable record : records) {
			System.out.println(record.toString());
		}
		System.out.println("Done\n\n");
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


}
