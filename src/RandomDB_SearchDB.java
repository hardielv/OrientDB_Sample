import java.io.File;
import java.util.Iterator;
import java.util.List;

import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

// ODocument vs OIdentifiable
// OSQLSyncQuery vs OSQLAsynchQuery vs OCommand



public class RandomDB_SearchDB {
	OGraphDatabase db;
	RandomDB_Environment env;
	int iterations, depth;
	String databaseDirectory, fileDirectory;
	String [] randomRID_list;

	public enum QueryType {TRAVERSE, UNION, INTERSECTION, DIFFERENCE, MINUS};
	
	public static void main(String[] args) {
		int iterations = 1;
		int depth = 10;
		QueryType query;
		String size = "huge"; // small, medium, large, huge
		RandomDB_SearchDB searchDB = new RandomDB_SearchDB(iterations, depth, size);
		
		System.out.print("Iterations = " + iterations + ", depth = " + depth);
		System.out.println(", Database: " + searchDB.env.DB_PATH + "\n\n");
		searchDB.openDatabase();
		searchDB.collectRandomRIDs();

		List<OIdentifiable> records = null;
		try{
			
//			boolean print = true;
//			searchDB.setRandomRID(0, "#11:37");
//			searchDB.setRandomRID(1, "#11:0");
//			searchDB.testQuery(QueryType.TRAVERSE, 0, 1, print);
//			searchDB.testQuery(QueryType.TRAVERSE, 1, 0, print);
//			
//			QueryType q;
//			
//			q = QueryType.UNION;
//			System.out.println(q);
//			searchDB.testQuery(q, 1, 0, print);
//
//			q = QueryType.INTERSECTION;
//			System.out.println(q);
//			searchDB.testQuery(q, 1, 0, print);
//
//			q = QueryType.DIFFERENCE;
//			System.out.println(q);
//			searchDB.testQuery(q, 1, 0, print);

			query = QueryType.UNION;
			records = searchDB.timePerformance(query);

			query = QueryType.DIFFERENCE;
			records = searchDB.timePerformance(query);
			
			
//			if(iterations == 1 && size.equals("small")) searchDB.printRecords(records);

//			searchDB.traverse_Iterator();			
			
		} catch(Exception e){
			System.out.println("Some sort of error occurred.");
			e.printStackTrace();
		} finally {
			searchDB.closeDatabase();
		}
	}

	// Constructor
	public RandomDB_SearchDB(int i, int d, String size){
		iterations = i;
		depth = d;
		databaseDirectory = "C:/scratch/OrientDB/orientdb-graphed/databases/randomDB_" + size;
		fileDirectory = "C:/scratch/OrientDB/dataFiles/randomDB_" + size + "/";
		env = new RandomDB_Environment(fileDirectory);
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

	
	private List<OIdentifiable> timePerformance(QueryType query){
		List<OIdentifiable> results = null;
		
		long [] times = new long[iterations];
		int numRecords = 0;
		
		String sql, sql_traverse1, sql_traverse2;
		long startTime, endTime;
		int minutes, seconds;
		
		System.out.println("Timing " + stringQueryType(query) + " of graph with " + env.TOTAL_VERTICES + " vertices");
		long totalTimes = 0;
		for(int i=0; i < iterations; i++){
			sql = createSQL_Query(query, randomRID_list[i], randomRID_list[i+1]);
//			System.out.println(sql);
			
			// Time query
			startTime = System.currentTimeMillis();
			results = db.query(new OSQLSynchQuery<OIdentifiable>(sql));
			endTime = System.currentTimeMillis();

			if(results != null)			numRecords += results.size();
			
			times[i] = endTime - startTime;
			minutes = (int) (times[i] / (1000 * 60));
			seconds = (int) ((times[i] / 1000) % 60);
			totalTimes += times[i];
		}
		
		long avgTime = totalTimes / iterations;
		minutes = (int) (avgTime / (1000 * 60));
		seconds = (int) ((avgTime / 1000) % 60);
		
		System.out.println("Average #records: " + (numRecords / iterations) + " of " + env.TOTAL_VERTICES);
		System.out.println(String.format("Average Time: %d ms or (%d min, %d sec)", avgTime, minutes, seconds)); 
		System.out.println();
					
		return results;
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
			String rids = "[" + id2 + "," + id1 + "]";
//			traverse1 = createSQL_Traverse(rids);
//			sql = "SELECT rid1 FROM (" + traverse1 + ")";
//			sql = traverse1;
	
			sql = "SELECT FROM (" + traverse1 + ") WHERE rid1 IN (" + traverse2 + ") OR rid1 IN (" + traverse1 + ")";
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
