import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Scanner;


public class RandomDB_Environment {
	public String DEFAULT_DIR = "C:/scratch/";
	public String DEFAULT_GRAPHDB = "OrientDB";
	public String DEFAULT_DATAFILE_DIR = DEFAULT_GRAPHDB + "/dataFiles";
	public String DEFAULT_DB_DIR = DEFAULT_GRAPHDB + "/databases";
	public String DB_NAME_PREFIX = "randomDB_";
	public String DB_SIZE, DB_PATH; 
	public String EXPORT_DIR; 
	public String ENV_FILE = "envData.txt";
	public String ORIENT_EDGE_FILE = "OrientDB_Edges.sql";
	
	public String VERTEX_FILE, EDGE_PATH;
	public String VERTEX_FIELDS_PATH, EDGE_FIELDS_PATH;
	
	public int NUM_VERTEX_TYPE, NUM_EDGE_TYPE;
	public int MAX_VERTEX_FIELDS, MAX_EDGE_FIELDS;
	public int TOTAL_VERTICES, TOTAL_EDGES;	
	
	public final String VERTEX_PREFIX = "Vertex_";
	public final String EDGE_PREFIX = "Edge_";
	public final String ID_PREFIX = "_ID";
	public final String IDx_PREFIX = "_IDx";

	public int VALUE_MAX = 999;

	public RandomDB_Environment(String envFileDir){
		File envFile = new File(envFileDir + "/" + ENV_FILE);
		try{
			String line, field;
			Scanner scanner = new Scanner(envFile);
			while(scanner.hasNextLine()){
				line = scanner.nextLine();
				Scanner lineScan = new Scanner(line);
				lineScan.useDelimiter(",");
				field = lineScan.next();
				
				if(field.equals("DB_NAME_PREFIX")) DB_NAME_PREFIX = lineScan.next();
				else if(field.equals("DB_SIZE")) DB_SIZE = lineScan.next();
				else if(field.equals("DB_PATH")) DB_PATH = lineScan.next();
				else if(field.equals("EXPORT_DIR")) EXPORT_DIR = lineScan.next();
				else if(field.equals("ENV_FILE")) ENV_FILE = lineScan.next();
				else if(field.equals("ORIENT_EDGE_FILE")) ORIENT_EDGE_FILE = lineScan.next();
				else if(field.equals("VERTEX_FILE")) VERTEX_FILE = lineScan.next();
				else if(field.equals("EDGE_PATH")) EDGE_PATH = lineScan.next();
				else if(field.equals("VERTEX_FIELDS_PATH")) VERTEX_FIELDS_PATH = lineScan.next();
				else if(field.equals("EDGE_FIELDS_PATH")) EDGE_FIELDS_PATH = lineScan.next();
				else if(field.equals("NUM_VERTEX_TYPE")) NUM_VERTEX_TYPE = lineScan.nextInt();
				else if(field.equals("NUM_EDGE_TYPE")) NUM_EDGE_TYPE = lineScan.nextInt();
				else if(field.equals("MAX_VERTEX_FIELDS")) MAX_VERTEX_FIELDS = lineScan.nextInt();
				else if(field.equals("MAX_EDGE_FIELDS")) MAX_EDGE_FIELDS = lineScan.nextInt();
				else if(field.equals("TOTAL_VERTICES")) TOTAL_VERTICES = lineScan.nextInt();
				else if(field.equals("TOTAL_EDGES")) TOTAL_EDGES = lineScan.nextInt();	
				else if(field.equals("VALUE_MAX")) VALUE_MAX = lineScan.nextInt();
				lineScan.close();
			}			
			scanner.close();
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}
	}
	
	public RandomDB_Environment(int numVertex){
		int numEdges = numVertex * 5;
		createEnvironment(numVertex, 2, 3, numEdges, 2, 2, DEFAULT_DIR);
	}
	
	public RandomDB_Environment(int numVertex, int numVertexTypes, int maxV_fields,
								int numEdges, int numEdgeTypes, int maxE_fields,
								String dbDirectory){
		createEnvironment(numVertex, numVertexTypes, maxV_fields, numEdges, numEdgeTypes, maxE_fields, dbDirectory);
	}
	
	public void createEnvironment(int numVertex, int numVertexTypes, int maxV_fields,
			int numEdges, int numEdgeTypes, int maxE_fields,
			String dbDirectory){
		int total = numVertex + numEdges;
		DB_SIZE = "small";
		
		if     (total >= 1000000) DB_SIZE = "huge";
		else if(total >= 10000) DB_SIZE = "large";
		else if(total >= 1000) DB_SIZE = "medium";
		else DB_SIZE = "small";

		DB_PATH = dbDirectory.substring(2) + "/orientDB/"+ DB_NAME_PREFIX + DB_SIZE;

		EXPORT_DIR = dbDirectory + "/dataFiles/" + DB_NAME_PREFIX + DB_SIZE;
		VERTEX_FILE = EXPORT_DIR + "/vertexData.odb";
		EDGE_PATH = EXPORT_DIR + "/edgeData.odb";
		VERTEX_FIELDS_PATH = EXPORT_DIR + "/vertexfieldData.odb";
		EDGE_FIELDS_PATH = EXPORT_DIR + "/edgefieldData.odb";

		TOTAL_VERTICES = numVertex;
		TOTAL_EDGES    = numEdges;	

		NUM_VERTEX_TYPE = numVertexTypes;
		NUM_EDGE_TYPE = numEdgeTypes;
		
		MAX_VERTEX_FIELDS = maxV_fields;
		MAX_EDGE_FIELDS = maxE_fields;		
	}
	
	public void saveEnvironment(){
		File envFile = new File(EXPORT_DIR + "/" + ENV_FILE);
		if(envFile.exists()) envFile.delete();
		try{
			FileWriter fstream = new FileWriter(envFile);
			BufferedWriter out = new BufferedWriter(fstream);
			out.write("DB_NAME_PREFIX," + DB_NAME_PREFIX + "\n");
			out.write("DB_SIZE," + DB_SIZE + "\n");
			out.write("DB_PATH," + DB_PATH + "\n");
			out.write("EXPORT_DIR," + EXPORT_DIR + "\n");
			out.write("ENV_FILE," + ENV_FILE + "\n");
			out.write("ORIENT_EDGE_FILE," + ORIENT_EDGE_FILE + "\n");
			
			out.write("VERTEX_FILE," + VERTEX_FILE + "\n");
			out.write("EDGE_PATH," + EDGE_PATH + "\n");
			out.write("VERTEX_FIELDS_PATH," + VERTEX_FIELDS_PATH + "\n");;
			out.write("EDGE_FIELDS_PATH," + EDGE_FIELDS_PATH + "\n");
			
			out.write("NUM_VERTEX_TYPE," + NUM_VERTEX_TYPE + "\n");
			out.write("NUM_EDGE_TYPE," + NUM_EDGE_TYPE + "\n");
			out.write("MAX_VERTEX_FIELDS," + MAX_VERTEX_FIELDS + "\n");
			out.write("MAX_EDGE_FIELDS," + MAX_EDGE_FIELDS + "\n");
			out.write("TOTAL_VERTICES," + TOTAL_VERTICES + "\n");
			out.write("TOTAL_EDGES," + TOTAL_EDGES + "\n");	

			out.write("VALUE_MAX," + VALUE_MAX + "\n");
			
			out.flush();
			out.close();
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}

	}
}
