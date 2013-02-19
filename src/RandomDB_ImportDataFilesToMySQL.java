import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;


public class RandomDB_ImportDataFilesToMySQL {
	   	// JDBC driver name and database URL
	   	final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	   	final String DB_URL = "jdbc:mysql://localhost:3307/";
	   	Connection db;
	   	Statement stmt;
	   	String DB_NAME;
	   	
		HashMap<Integer, ArrayList<String>> mapVertexFieldNames;
		HashMap<Integer, ArrayList<String>> mapEdgeFieldNames;
		HashMap<Integer, ArrayList<Long>> mapVertexRIDs;
		RandomDB_Environment env;

		//  Database credentials
		static final String USER = "lexgrid";
		static final String PASS = "lexgrid";
	   
	   public static void main(String[] args) {
		   	Boolean importAll = true;
		   	Boolean importEdges = true;
		   	Boolean createKeys = true;
			String fileDirectory = "/home/m113216/orient/datafiles/randomDB_huge";
			File vFile, eFile, vfFile, efFile;
			
			RandomDB_ImportDataFilesToMySQL randomDB = new RandomDB_ImportDataFilesToMySQL(fileDirectory); 
			
			if(importAll){
				System.out.println("deleting database");
				randomDB.deleteDatabase();
				System.out.println("Re-creating database");
				randomDB.createDatabase();

				vFile = new File(randomDB.env.VERTEX_FILE);
				vfFile = new File(randomDB.env.VERTEX_FIELDS_PATH);
				efFile = new File(randomDB.env.EDGE_FIELDS_PATH);
				
				System.out.println("Storing Metadata");
				randomDB.storeMetaDataToDatabase(vfFile, efFile);
				// Save data to database
				System.out.println("Storing Vertices");
				randomDB.storeVerticesToDatabase(vFile);
			}
			else if(importEdges){
				System.out.println("Clearing out edge tables");
				randomDB.clearEdges();
			}
			
			if(importEdges){
				System.out.println("Storing Edges");
				eFile = new File(randomDB.env.EDGE_PATH);
				randomDB.storeEdgesToDatabase(eFile);
			}
			
			if(createKeys){
				System.out.println("Creating Primary Keys");
				randomDB.createPrimaryKeys();
				System.out.println("Creating Indices");
				randomDB.createIndices();
			}
			
			randomDB.closeDatabase();
					
			
//			randomDB.printEdges();
//			randomDB.storeEdgesSQLtoFile(eFile);
			System.out.println("Finished");
			
	   }	

	   public void createPrimaryKeys(){
		   String sql = "USE " + DB_NAME;
		   try {
			   stmt.executeUpdate(sql);
		   } catch (SQLException e) {
			   e.printStackTrace();
		   }
		   
		   createPrimaryKeys(env.VERTEX_PREFIX, env.NUM_VERTEX_TYPE);
		   createPrimaryKeys(env.EDGE_PREFIX, env.NUM_EDGE_TYPE);
	   }
	   
	   private void createPrimaryKeys(String table, int numTables){
		   String sql, tableName;
		   for(int i=0; i < numTables; i++){
			   tableName = table + i;
			   sql = "alter table " + tableName + " add primary key (" + tableName + "_ID)";
			   System.out.println(sql);
			   try {
				   stmt = db.createStatement();
				   stmt.executeUpdate(sql);
			   } catch (SQLException e) {
				   e.printStackTrace();
			   }
		   }
	   }
	   
	   public void createIndices(){
		   String sql = "USE " + DB_NAME;
		   try {
			   stmt.executeUpdate(sql);
		   } catch (SQLException e1) {
			   e1.printStackTrace();
		   }

		   String tableName;
		   for(int i=0; i < env.NUM_EDGE_TYPE; i++){
			   tableName = env.EDGE_PREFIX + i;
//			   sql = "alter table " + tableName + " add unique idx_" + tableName + " (edgeIN, edgeOUT)";
			   sql = "create index idx_" + tableName + " on " + tableName + " (edgeIN, edgeOUT)";
			   System.out.println(sql);
			   try {
				   stmt = db.createStatement();
				   stmt.executeUpdate(sql);
			   } catch (SQLException e) {
				   e.printStackTrace();
			   }
		   }
	   }
	   
	   public void clearEdges(){
		   String sql;
		   for(int i=0; i < env.NUM_EDGE_TYPE; i++){
			   sql = "Delete from " + env.EDGE_PREFIX + i;
			   try {
				   stmt = db.createStatement();
				   stmt.executeUpdate(sql);
			   } catch (SQLException e) {
				   e.printStackTrace();
			   }
		   }
	   }
	   
	   public RandomDB_ImportDataFilesToMySQL(String envPath){
		   env = new RandomDB_Environment(envPath);
		   mapVertexRIDs = new HashMap<Integer, ArrayList<Long>>();
		   DB_NAME = env.DB_NAME_PREFIX + env.DB_SIZE;
		   try{
			   Class.forName("com.mysql.jdbc.Driver");
			   System.out.println("Connecting to database...");
			   db = DriverManager.getConnection(DB_URL, USER, PASS);
			   stmt = db.createStatement();
		   }catch(SQLException se){
			   se.printStackTrace();
		   }catch(ClassNotFoundException ce){
			   ce.printStackTrace();
		   }		   
	   }
		  
		
		public void deleteDatabase(){
			   try{
				   String sql = "DROP DATABASE " + DB_NAME;
				   System.out.println(sql);
				   stmt.executeUpdate(sql);
			   }catch(SQLException se){
				   se.printStackTrace();
			   }catch(Exception e){
				   e.printStackTrace();
			   }
		}

		public void createDatabase(){
		   try{
			   String sql = "CREATE DATABASE " + DB_NAME;
			   stmt.executeUpdate(sql);
			   sql = "USE " + DB_NAME;
			   stmt.executeUpdate(sql);
		   }catch(SQLException se){
			   se.printStackTrace();
		   }catch(Exception e){
			   e.printStackTrace();
		   }
	   	}
	
		public void closeDatabase(){
			   try{
				   stmt.close();
				   db.close();
			   }catch(SQLException se){
				   se.printStackTrace();
			   }catch(Exception e){
				   e.printStackTrace();
			   }
		}

		public void storeMetaDataToDatabase(File vertexFile, File edgeFile){
			
			// Read in vertex records from file
			try{
				String sql = "USE " + DB_NAME;
				stmt.executeUpdate(sql);

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

			
			// Read in edge records from file
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
			
			//Create Tables in database			
			String vType, vType_IndexField, sql;			
			
			for(int i=0; i < mapVertexFieldNames.size(); i++){
				vType = env.VERTEX_PREFIX + i;
				vType_IndexField = vType + env.ID_PREFIX;
				
				ArrayList<String> vFieldNames =  mapVertexFieldNames.get(i);
				sql = "CREATE TABLE " + vType;
				// Create ID field with unique index
//				sql += " (" + vType_IndexField + " VARCHAR(255) NOT NULL PRIMARY KEY, label VARCHAR(255)"; 
				sql += " (" + vType_IndexField + " BIGINT NOT NULL, label VARCHAR(255)"; 
				// TODO: Create index
				// Create other fields
				for(int j=0; j < vFieldNames.size(); j++){
					sql += ", " + vFieldNames.get(j) + " VARCHAR(255)"; 
				}
				sql += ")";
				System.out.println(sql);
				try{
				   stmt = db.createStatement();
					stmt.executeUpdate(sql);
				}catch(SQLException se){
					se.printStackTrace();
				}
			}
				
			String eType, eType_IndexField;
			for(int i=0; i < mapEdgeFieldNames.size(); i++){
				eType = env.EDGE_PREFIX + i;
				eType_IndexField = eType + env.ID_PREFIX;
				
				ArrayList<String> eFieldNames =  mapEdgeFieldNames.get(i);
				sql = "CREATE TABLE " + eType;
				// Create ID field with unique index
//				sql += " (" + eType_IndexField + " VARCHAR(255) NOT NULL PRIMARY KEY, label VARCHAR(255), edgeIN VARCHAR(255), edgeOUT VARCHAR(255)"; 
				sql += " (" + eType_IndexField + " BIGINT NOT NULL, label VARCHAR(255), edgeIN VARCHAR(255), edgeOUT VARCHAR(255)"; 
				// TODO: Create index
				// Create other fields
				for(int j=0; j < eFieldNames.size(); j++){
					sql += ", " + eFieldNames.get(j) + " VARCHAR(255)"; 
				}
				sql += ")";
				System.out.println(sql);
				try{
				   stmt = db.createStatement();
					stmt.executeUpdate(sql);
				}catch(SQLException se){
					se.printStackTrace();
				}
			}
		}

		public void storeVerticesToDatabase(File file){
			String sql, sqlFields, sqlValues;
			
			for(int i=0; i < mapVertexFieldNames.size(); i++){
				mapVertexRIDs.put(i, new ArrayList<Long>());
			}
			try{
				sql = "USE " + DB_NAME;
				stmt.executeUpdate(sql);

				
				Scanner scanner = new Scanner(file);
				int recordCount = 0, typeID, prevTypeID = 0;
//				ODocument tDoc = db.createVertex(env.VERTEX_PREFIX + 0);
				String vertexType, idField, id;
				long vertexID0 = 0;
				long vertexID1 = 10;
				long vertexID;
				while(scanner.hasNextLine()){
					if(((recordCount++ + 1) % 10000) == 0){
						System.out.println("Working on the " + recordCount + "th vertex");
					}
					String line = scanner.nextLine();
					Scanner lineScan = new Scanner(line);
					lineScan.useDelimiter(",");
					typeID = lineScan.nextInt();
					
					vertexType = env.VERTEX_PREFIX + typeID;
					
					// Check for overflow
					if(vertexID1 == 0) { 
						vertexID1 = 10;
						vertexID0++;
					}
					
//					vertexID = vertexID0 + "_" + vertexID1++;
					vertexID = vertexID1++;
					idField = env.VERTEX_PREFIX + typeID + env.ID_PREFIX;
//					tDoc.field(idField, vertexID);
//					tDoc.field("label", vertexType);
					
					sqlFields = idField + ", label";
					sqlValues = "'" + vertexID + "', '" + vertexType + "'";
					// Store fields from file...
					ArrayList<String> fieldNames = mapVertexFieldNames.get(typeID);
					for(String fieldName:fieldNames){
						String value = lineScan.next();
//						tDoc.field(fieldName, value);
						sqlFields += ", " + fieldName;
						sqlValues += ", '" + value + "'";
					}
					
					sql = "INSERT INTO " + vertexType + " (" + sqlFields + ") VALUES (" + sqlValues + ")";
					stmt = db.createStatement();
					stmt.execute(sql);
//					tDoc.save();
//					id = tDoc.getIdentity().toString();
//					mapVertexRIDs.get(typeID).add(id);
					mapVertexRIDs.get(typeID).add(vertexID);
				}
							
				scanner.close();
			} catch(FileNotFoundException e){
				System.err.println("(storeVertices) Error: " + e.getMessage());
			} catch(Exception e){
				System.err.println("(storeVertices) Error: " + e.getMessage());
			}

		}
		
		public void storeEdgesToDatabase(File file){
			int typeID, in_type, in_index, out_type, out_index;
			long edgeID = 10, id_in, id_out;			
			String idField, line, edgeType, sql, sqlFields, sqlValues;
			Scanner lineScan;
			
			PreparedStatement [] pStatements = new PreparedStatement[env.NUM_EDGE_TYPE];
			int [] recordCount = new int[env.NUM_EDGE_TYPE];
			long totalRecords = 0;

			sql = "USE " + DB_NAME;
			try {
				stmt.executeUpdate(sql);
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
			// Create sql statements and save in prepared statements array
			for(int i=0; i < pStatements.length; i++){
				recordCount[i] = 0;
				edgeType = env.EDGE_PREFIX + i;
				idField = edgeType + env.ID_PREFIX;	
				// Store fields from file...
				sqlFields = idField + ", label, edgeIN, edgeOUT";
				sqlValues = "?, ?, ?, ?";

				for(String fieldName:mapEdgeFieldNames.get(i)){
					sqlFields += ", " + fieldName;
					sqlValues += ", ?";
				}
				
				sql = "INSERT INTO " + edgeType + " (" + sqlFields + ") VALUES (" + sqlValues + ")";
				System.out.println(sql);
				try {
					pStatements[i] = db.prepareStatement(sql);
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			
			// Pull data from import file and add to prepared statement batch method.
			try{
				Scanner scanner = new Scanner(file);
				
				while(scanner.hasNextLine()){
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
					
					edgeType = env.EDGE_PREFIX + typeID;
					edgeID++;
					int index = 1;
					totalRecords++;
					recordCount[typeID]++;
					pStatements[typeID].setLong(index++, edgeID);	 		// ID
					pStatements[typeID].setString(index++, edgeType);		// label
					pStatements[typeID].setLong(index++, id_in);			// edgeIN
					pStatements[typeID].setLong(index++, id_out);			// edgeOUT
					
					// Store fields from file...
					int numValues = mapEdgeFieldNames.get(typeID).size();
					for(int i=0; i < numValues; i++){
						String value2 = lineScan.next();
						pStatements[typeID].setString(index++, value2);		// fieldValue
					}
					
					pStatements[typeID].addBatch();
//					System.out.println(sql);
//					stmt = db.createStatement();
//					stmt.execute(sql);
//									
					// execute batch update to database every 1,000 records
					// Notify user progress every 10,000 records
					for(int i=0; i < recordCount.length; i++){
						if((recordCount[i] + 1) % 1000 == 0){
//								System.out.println(totalRecords + " -- Commiting records to " + env.EDGE_PREFIX + i + ", " + recordCount[i]);						
								pStatements[i].executeBatch();
						}	
						if((totalRecords + 1) % 100000 == 0){
							System.out.println(totalRecords + 1);						
						}
					}
				}
				
				System.out.println("Executing final batches...");
				for(int i=0; i < pStatements.length; i++){
					pStatements[i].executeBatch();
				}
				scanner.close();		
			} catch(FileNotFoundException e){
				System.err.println("(storeEdge) Error: " + e.getMessage());
			} catch(Exception e){
				System.err.println("(storeEdge) Error: " + e.getMessage());		
			}		
		}

		/*	
	
	
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
	
	
	public void printEdges(){
		List<ODocument> results = db.query(new OSQLSynchQuery("select from OGraphEdge")); 		 
		System.out.println("Printing Edges...");
		
		for(ODocument result:results) {
			System.out.println(result.toString());
		}

		System.out.println();
		System.out.println();
	}

*/
}
