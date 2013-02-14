import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;

import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

public class RandomDB_ImportDataFilesToOrientDB {
	OGraphDatabase db;
	HashMap<Integer, ArrayList<String>> mapVertexFieldNames;
	HashMap<Integer, ArrayList<String>> mapEdgeFieldNames;
	HashMap<Integer, ArrayList<String>> mapVertexRIDs;
	RandomDB_Environment env;
	
	
	public static void main(String[] args) {
		String fileDirectory = "C:/scratch/dataFiles/randomDB_huge/";
		RandomDB_ImportDataFilesToOrientDB randomDB = new RandomDB_ImportDataFilesToOrientDB(fileDirectory); 
		randomDB.createDatabase();
		
		File vFile = new File(randomDB.env.VERTEX_FILE);
		File eFile = new File(randomDB.env.EDGE_PATH);
		File vfFile = new File(randomDB.env.VERTEX_FIELDS_PATH);
		File efFile = new File(randomDB.env.EDGE_FIELDS_PATH);
		
		// Open Database
		System.out.println("Storing Metadata");
		randomDB.storeMetaDataToDatabase(vfFile, efFile);
		
		// Save data to database
		System.out.println("Storing Vertices");
		randomDB.storeVerticesToDatabase(vFile);
		System.out.println("Storing Edges");
//		randomDB.storeEdgesToDatabase(eFile);
		
//		randomDB.printEdges();
		randomDB.closeDatabase();
		randomDB.storeEdgesSQLtoFile(eFile);
		System.out.println("Finished");
		
	}	

	public RandomDB_ImportDataFilesToOrientDB(String envPath){
		env = new RandomDB_Environment(envPath);
		mapVertexRIDs = new HashMap<Integer, ArrayList<String>>();
	}

	public void deleteDatabase(){
		File dir = new File(env.DB_PATH);
		if(dir.exists()){ 
			try{
				FileUtils.deleteDirectory(dir);
			}catch(Exception e){
				System.out.println("Unable to delete old version of database: " + env.DB_PATH);
			}
		}		
	}
	
	public void createDatabase(){
		deleteDatabase();
		db = new OGraphDatabase("local:" + env.DB_PATH);
		db.create();
	}
	
	
	public void openDatabase(){
		db = new OGraphDatabase("local:" + env.DB_PATH);
		db.open("admin",  "admin");
		db.declareIntent(new OIntentMassiveInsert());
	}
	
	public void storeMetaDataToDatabase(File vertexFile, File edgeFile){
		
		try{
			Scanner scanner = new Scanner(vertexFile);
			mapVertexFieldNames = new HashMap<Integer, ArrayList<String>>();
			
			while(scanner.hasNextLine()){
				String line = scanner.nextLine();
				Scanner lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				int typeID = lineScan.nextInt();
				mapVertexFieldNames.put(typeID, new ArrayList<String>());
				while(lineScan.hasNext()){
					String value = lineScan.next();
					mapVertexFieldNames.get(typeID).add(value);
				}
			}
			
			scanner.close();
		} catch(FileNotFoundException e){
			System.err.println("Error: " + e.getMessage());
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}

		
		try{
			Scanner scanner = new Scanner(edgeFile);			
			mapEdgeFieldNames = new HashMap<Integer, ArrayList<String>>();
			
			while(scanner.hasNextLine()){
				String line = scanner.nextLine();
				Scanner lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				int typeID = lineScan.nextInt();
				mapEdgeFieldNames.put(typeID, new ArrayList<String>());
				while(lineScan.hasNext()){
					String value = lineScan.next();
					mapEdgeFieldNames.get(typeID).add(value);
				}
			}
			
			scanner.close();
		} catch(FileNotFoundException e){
			System.err.println("Error: " + e.getMessage());
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}
		
		//Create Types in database
		
		OSchema schema = db.getMetadata().getSchema();
		
		String vType, vType_IndexField;
		
		for(int i=0; i < mapVertexFieldNames.size(); i++){
			vType = env.VERTEX_PREFIX + i;
			vType_IndexField = vType + env.ID_PREFIX;
			db.createVertexType(vType);
			OClass oClass = schema.getClass(vType);
			ArrayList<String> vFieldNames =  mapVertexFieldNames.get(i);
			
			// Create ID field with unique index
			oClass.createProperty(vType_IndexField, OType.STRING).setMandatory(true).setNotNull(true);
			oClass.createIndex(vType + env.IDx_PREFIX, OClass.INDEX_TYPE.UNIQUE, vType_IndexField);
			oClass.createProperty("label", OType.STRING);
			// Create other fields
			for(int j=0; j < vFieldNames.size(); j++){
				oClass.createProperty(vFieldNames.get(j), OType.STRING);
			}
			schema.save();
		}
			
		String eType, eType_IndexField;
		for(int i=0; i < mapEdgeFieldNames.size(); i++){
			eType = env.EDGE_PREFIX + i;
			eType_IndexField = eType + env.ID_PREFIX;
			db.createEdgeType(env.EDGE_PREFIX + i);
			OClass oClass = schema.getClass(env.EDGE_PREFIX + i);
			ArrayList<String> eFieldNames =  mapEdgeFieldNames.get(i);

			// Create ID field with unique index
			oClass.createProperty(eType_IndexField, OType.STRING);
//			oClass.createProperty(eType_IndexField, OType.STRING).setMandatory(true).setNotNull(true);
//			oClass.createIndex(eType + env.IDx_PREFIX, OClass.INDEX_TYPE.UNIQUE, eType_IndexField);
			oClass.createProperty("label", OType.STRING);
			for(int j=0; j < eFieldNames.size(); j++){
				oClass.createProperty(eFieldNames.get(j), OType.STRING);
			}
			schema.save();
		}
	}
	
	public void storeVerticesToDatabase(File file){
		
		for(int i=0; i < mapVertexFieldNames.size(); i++){
			mapVertexRIDs.put(i, new ArrayList<String>());
		}
		try{
			Scanner scanner = new Scanner(file);
			int recordCount = 0;
			ODocument tDoc = db.createVertex(env.VERTEX_PREFIX + 0);
			String vertexType, idField, id;
			long vertexID0 = 0;
			long vertexID1 = 10;
			String vertexID;
			while(scanner.hasNextLine()){
				if(((recordCount++ + 1) % 10000) == 0){
					System.out.println("Working on the " + recordCount + "th vertex");
					db.commit();
					System.out.println("Commited records");
				}
				String line = scanner.nextLine();
				Scanner lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				int typeID = lineScan.nextInt();
				
				vertexType = env.VERTEX_PREFIX + typeID;
				tDoc.reset();
				tDoc.setClassName(vertexType);
				tDoc.getIdentity().reset();
				
				// Check for overflow
				if(vertexID1 == 0) { 
					vertexID1 = 10;
					vertexID0++;
				}
				
				vertexID = vertexID0 + "_" + vertexID1++;
				idField = env.VERTEX_PREFIX + typeID + env.ID_PREFIX;
				tDoc.field(idField, vertexID);
				tDoc.field("label", vertexType);
				
				// Store fields from file...
				ArrayList<String> fieldNames = mapVertexFieldNames.get(typeID);
				for(String fieldName:fieldNames){
					String value = lineScan.next();
					tDoc.field(fieldName, value);
				}
				
				tDoc.save();
				id = tDoc.getIdentity().toString();
				mapVertexRIDs.get(typeID).add(id);
			}
						
			scanner.close();
		} catch(FileNotFoundException e){
			System.err.println("(storeVertices) Error: " + e.getMessage());
		} catch(Exception e){
			System.err.println("(storeVertices) Error: " + e.getMessage());
		}

	}
	
	
	public void storeEdgesToDatabase(File file){
		String connectDB = "connect local:" + env.DB_PATH + " admin admin;\n";
		try{
			Scanner scanner = new Scanner(file);
			int recordCount = 0;
			
			ODocument doc_in, doc_out;
			ODocument tDoc = db.createEdge(new ODocument(), new ODocument(), env.EDGE_PREFIX + 0);
			int typeID, in_type, in_index, out_type, out_index;
			
			long edgeID0 = 0;
			long edgeID1 = 10;
			ArrayList<ODocument> r1;
			ArrayList<ODocument> r2;
			
			String edgeID, line, id_in, id_out, idField;
			Scanner lineScan;
			String edgeType, edge_sql, comma, value; 

			while(scanner.hasNextLine()){
				recordCount++;
				line = scanner.nextLine();
				lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				typeID = lineScan.nextInt();
				in_type = lineScan.nextInt();
				in_index = lineScan.nextInt();
				out_type = lineScan.nextInt();
				out_index = lineScan.nextInt();
				id_in = mapVertexRIDs.get(in_type).get(in_index);
				id_out = mapVertexRIDs.get(out_type).get(out_index);
				doc_in = ((ArrayList<ODocument>) db.query(new OSQLSynchQuery<ODocument>("Select from " + id_in))).get(0);
				doc_out = ((ArrayList<ODocument>) db.query(new OSQLSynchQuery<ODocument>("Select from " + id_out))).get(0);
				
				tDoc.reset();
				tDoc.setClassName(env.EDGE_PREFIX + typeID);
				tDoc.getIdentity().reset();
				
				tDoc.field("in", doc_in);
				tDoc.field("out", doc_out);
				
				// Store ID field first
				edgeType = env.EDGE_PREFIX + typeID;
				idField = edgeType + env.ID_PREFIX;	
				if(edgeID1 == 0) { 
					edgeID1 = 10;
					edgeID0++;
				}
				
				edgeID = edgeID0 + "_" + edgeID1++;
				tDoc.field(idField, edgeID);

				if((recordCount % 1000) == 0) db.commit();
				if((recordCount % 10000) == 0){
					System.out.println("Working on the " + recordCount + "th edge");
				}

				for(String fieldName:mapEdgeFieldNames.get(typeID)){
					value = lineScan.next();
					tDoc.field(fieldName, value);
				}
				
				
				tDoc.save();
				String sql = "update " + doc_in.getIdentity() + " add out = " + tDoc.getIdentity();
				db.command(new OCommandSQL(sql)).execute();
				sql = "update " + doc_out.getIdentity() + " add in = " + tDoc.getIdentity();
				db.command(new OCommandSQL(sql)).execute();
			}
	
			scanner.close();
				
		} catch(FileNotFoundException e){
			System.err.println("(storeEdge) Error: " + e.getMessage());
		} catch(Exception e){
			System.err.println("(storeEdge) Error: " + e.getMessage());		
		}		
	}

	// -------------------------------
	
	public void storeEdgesSQLtoFile(File datain_file){
		String edgeSQL_file = env.EXPORT_DIR + "/" + env.ORIENT_EDGE_FILE;
		String connectDB = "connect local:" + env.DB_PATH + " admin admin;\n";
		try{
			Scanner scanner = new Scanner(datain_file);
			int recordCount = 0;
			
			File outFile = new File(edgeSQL_file);
			if(outFile.exists()) outFile.delete();
			FileWriter fstream = new FileWriter(outFile);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write(connectDB);

			int typeID, in_type, in_index, out_type, out_index;
			
			long edgeID0 = 0;
			long edgeID1 = 10;
			ArrayList<ODocument> r1;
			ArrayList<ODocument> r2;
			
			String edgeID, line, id_in, id_out, idField;
			Scanner lineScan;
			String edgeType, edge_sql, comma, value; 
			while(scanner.hasNextLine()){
				recordCount++;
				line = scanner.nextLine();
				lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				typeID = lineScan.nextInt();
				in_type = lineScan.nextInt();
				in_index = lineScan.nextInt();
				out_type = lineScan.nextInt();
				out_index = lineScan.nextInt();
				id_in = mapVertexRIDs.get(in_type).get(in_index);
//				id_in = id_in.substring(1, id_in.length());
				id_out = mapVertexRIDs.get(out_type).get(out_index);
//				id_out = id_out.substring(1, id_out.length());
				
				// Store ID field first
				edgeType = env.EDGE_PREFIX + typeID;
				idField = edgeType + env.ID_PREFIX;	
				if(edgeID1 == 0) { 
					edgeID1 = 10;
					edgeID0++;
				}
				
				edgeID = edgeID0 + "_" + edgeID1++;
				edge_sql = "create edge " + edgeType + " from " + id_in + " to " + id_out;
				edge_sql += " set " + idField + " = '" + edgeID + "', label = '" + edgeType + "'";
				comma = ", ";

				for(String fieldName:mapEdgeFieldNames.get(typeID)){
					value = lineScan.next();
					edge_sql += comma + fieldName + " = '" + value + "'";
				}
				
				if((recordCount % 10000) == 0){
					System.out.println("Working on the " + recordCount + "th edge");
					edge_sql = "commit;\n" + edge_sql;
				}
				
				out.write(edge_sql + ";\n");
				out.flush();				
			}
	
			out.close();
			scanner.close();
				
		} catch(FileNotFoundException e){
			System.err.println("(storeEdge) Error: " + e.getMessage());
		} catch(Exception e){
			System.err.println("(storeEdge) Error: " + e.getMessage());		
		}		
	}
	
	
	public void closeDatabase(){
		db.close();
	}

	public void printEdges(){
		List<ODocument> results = db.query(new OSQLSynchQuery("select from OGraphEdge")); 		 
		System.out.println("Printing Edges...");
		
		for(ODocument result:results) {
			System.out.println(result.toString());
		}

		System.out.println();
		System.out.println();
	}


}
