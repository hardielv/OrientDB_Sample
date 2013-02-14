import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;


public class RandomDB_GenerateRandomDataFiles {
	String [][] listVertexFieldNames;
	String [][] listEdgeFieldNames;
	HashMap<Integer, ArrayList<RandomDB_Vertex>> mapVertices;
	HashMap<Integer, ArrayList<RandomDB_Edge>> mapEdges;
	Scanner s; 
	RandomDB_Environment env;

	public static void main(String[] args) {
//		boolean print = false;
		int [] sizes = {100};
//		int [] sizes = {100, 1000, 10000}; //, 1000000};
		RandomDB_GenerateRandomDataFiles randomDB;
		
		for(int i=0; i < sizes.length; i++){
			System.out.println("Genering datafiles for " + sizes[i] + " vertices...");
			randomDB = new RandomDB_GenerateRandomDataFiles(sizes[i]);
			randomDB.createVertices();
			randomDB.storeVertices();
			randomDB.createEdges();
			randomDB.storeEdges();
			randomDB.env.saveEnvironment();
		}
		
//		if(print){
//			randomDB.printMetaData();
//			System.out.println();	
//			System.out.println("Press enter to continue..");
//			randomDB.nextLine();
//			
//			randomDB.printVertices();
//			System.out.println("Press enter to continue..");
//			randomDB.nextLine();
//	
//			
//			randomDB.printEdges();
//			System.out.println("Press enter to continue..");
//			randomDB.nextLine();
//		}
		
	}	

	public RandomDB_GenerateRandomDataFiles(int size){
		env = new RandomDB_Environment(size);
		s = new Scanner(System.in);
		mapVertices = new HashMap<Integer, ArrayList<RandomDB_Vertex>>();
		mapEdges = new HashMap<Integer, ArrayList<RandomDB_Edge>>();
	
		for(int i=0; i < env.NUM_VERTEX_TYPE; i++){
			mapVertices.put(i, new ArrayList<RandomDB_Vertex>());
		}
		for(int i=0; i < env.NUM_EDGE_TYPE; i++){
			mapEdges.put(i, new ArrayList<RandomDB_Edge>());
		}
		
		listVertexFieldNames = new String[env.NUM_VERTEX_TYPE][];
		listEdgeFieldNames = new String[env.NUM_EDGE_TYPE][];
		
		run();
	}
	
	private void run(){
		// Configure random environment to create Vertices and Edges
		createFields("vertex", listVertexFieldNames, env.MAX_VERTEX_FIELDS);
		createFields("edge", listEdgeFieldNames, env.MAX_EDGE_FIELDS);
	}
	
	private void createFields(String type, String[][] groups, int max){
		for(int i=0; i < groups.length; i++){
			int size = (int) (Math.random() * max) + 1;
			groups[i] = new String[size];
			for(int j=0; j < size; j++){
				groups[i][j] = type + i + "_field" + j;
			}			
		}		
	}
	

	public void createVertices(){
		System.out.println("Creating " + env.TOTAL_VERTICES + " vertices");
		for(int i=0; i < env.TOTAL_VERTICES; i++){
//			if((i % 1000) == 0) System.out.println("Created " + i + " nodes of " + TOTAL_VERTICES);
			
			Integer vertexTypeID = (int) (Math.random() * env.NUM_VERTEX_TYPE);
			RandomDB_Vertex v = new RandomDB_Vertex(env.VERTEX_PREFIX + vertexTypeID, listVertexFieldNames[vertexTypeID].length);
			
			// Generate random values, one for each field in given vertex
			for(int j=0; j < v.getSize(); j++){
				int value = (int) (Math.random() * env.VALUE_MAX);
				v.addValue(j, "value_" + value);
			}
			// Insert into list of vertices
			mapVertices.get(vertexTypeID).add(v);
		}
	}

	public void createEdges(){
		System.out.println("Creating " + env.TOTAL_EDGES + " edges");
		for(int i = 0; i < env.TOTAL_EDGES; i++){
			if((i % 1000) == 0) System.out.println("Created " + i + " edges of " + env.TOTAL_EDGES);

			int vType1 = (int) (Math.random() * env.NUM_VERTEX_TYPE);
			int vType2 = (int) (Math.random() * env.NUM_VERTEX_TYPE);
			
			int index1 = (int) (Math.random() * mapVertices.get(vType1).size());
			int index2 = (int) (Math.random() * mapVertices.get(vType2).size()); 
			
			// Infinite loop if one vertexType and one Vertex
			while((vType1 == vType2) && (index1 == index2)){
				vType2 = (int) (Math.random() * env.NUM_VERTEX_TYPE);
				index2 = (int) (Math.random() * mapVertices.get(vType2).size());				
			}
	
			Integer edgeTypeID = (int)(Math.random() * env.NUM_EDGE_TYPE);
			RandomDB_Edge e = new RandomDB_Edge(env.EDGE_PREFIX + edgeTypeID, listEdgeFieldNames[edgeTypeID].length);
			int [] v_in = {vType1, index1};
			int [] v_out = {vType2, index2};
			e.connect(v_in, v_out);
			
			for(int j=0; j < e.getSize(); j++){
				int value = (int) (Math.random() * env.VALUE_MAX);
				e.addValue(j, "value_" + value);
			}			
			
			mapEdges.get(edgeTypeID).add(e);
		}		
	}
	
	public void printVertices(){
		System.out.println("Vertices...");
		for(int i=0; i < env.NUM_VERTEX_TYPE; i++){
			ArrayList<RandomDB_Vertex> vertices = mapVertices.get(i);
			int vertexIndex=0;
			for(RandomDB_Vertex v:vertices){
				System.out.print(v.getType() + "_" + vertexIndex++ + ":: ");
				for(int j=0; j < listVertexFieldNames[i].length; j++){
					System.out.print(listVertexFieldNames[i][j] + "(" + v.getValue(j) + "), ");
				}
				System.out.println();
			}
			s.nextLine();
		}	
		System.out.println();
		System.out.println();
	}

	public void printEdges(){ 		 
		System.out.println("Edges...");
		for(int i=0; i < env.NUM_EDGE_TYPE; i++){
			ArrayList<RandomDB_Edge> edges = mapEdges.get(i);
			int edgeIndex = 0;
			for(RandomDB_Edge e:edges){
				System.out.print(e.getType() + "_" + edgeIndex + ":: ");
				for(int j=0; j < listVertexFieldNames[i].length; j++){
					System.out.print(listEdgeFieldNames[i][j] + "(" + e.getValue(j) + "), ");
				}
				System.out.println();
				int [] v1 = e.getVertex_In();
				int [] v2 = e.getVertex_Out();
				System.out.println("CONNECTS: {(" + v1[0] + ", " + v1[1] + "), (" + v2[0] + ", " + v2[1] + ")}\n");
			}
			s.nextLine();
		}	
		System.out.println();
		System.out.println();
	}

	private void printMetaData(String type, String[][] fieldNames){
		String separator = "";
		type = type.toUpperCase() + ": ";
		
		for(int i=0; i < fieldNames.length; i++) {
			System.out.print(type);
			separator = "";
			for(int j=0; j < fieldNames[i].length; j++){
				System.out.print(separator + fieldNames[i][j]);
				separator = ", ";
			}
			System.out.println();
		}

		System.out.println();
	}

	public void printMetaData(){
		printMetaData("vertex", listVertexFieldNames);
		printMetaData("edge", listEdgeFieldNames);		
	}
	
	public String nextLine(){
		return s.nextLine();
	}
	
	public void storeVertices(){
		File vFile = new File(env.VERTEX_FILE);
		File vfFile = new File(env.VERTEX_FIELDS_PATH);

		System.out.println("Deleting existing files");
		if(vFile.exists()) vFile.delete();
		if(vfFile.exists()) vfFile.delete();
		
		System.out.println("Saving mapVertices to file");
		try{
			FileWriter fstream = new FileWriter(vFile);
			BufferedWriter out = new BufferedWriter(fstream);
			int vertexType = 0;
			for(Integer key:mapVertices.keySet()){
				for(RandomDB_Vertex v:mapVertices.get(key)){
					out.write(vertexType + "");
					int size = v.getSize();
					for(int i=0; i < size; i++){
						out.write("," + v.getValue(i));
					}
					out.write("\n");
				}
				vertexType++;
			}
			out.flush();
			out.close();
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}

//		mapVertices = null;
		
		System.out.println("Saving listVertexFieldNames to file");
		try{
			FileWriter fstream = new FileWriter(vfFile);
			BufferedWriter out = new BufferedWriter(fstream);
			String seperator = "";
			for(int i=0; i < listVertexFieldNames.length; i++){
				seperator = "";
				out.write(i + ",");
				for(int j=0; j < listVertexFieldNames[i].length; j++){
					out.write(seperator + listVertexFieldNames[i][j]);
					seperator = ",";
				}
				out.write("\n");
			}
			out.flush();
			out.close();
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}		
		
//		listVertexFieldNames = null;
	}

	public void storeEdges(){
		File eFile = new File(env.EDGE_PATH);
		File efFile = new File(env.EDGE_FIELDS_PATH);

		System.out.println("Deleting existing files");
		if(eFile.exists()) eFile.delete();
		if(efFile.exists()) efFile.delete();
		
		System.out.println("Saving mapEdges to file");
		try{
			FileWriter fstream = new FileWriter(eFile);
			BufferedWriter out = new BufferedWriter(fstream);
			
			for(Integer key:mapEdges.keySet()){
				for(RandomDB_Edge e:mapEdges.get(key)){
					out.write(key + ",");
					int [] edgeIn = e.getVertex_In();
					int [] edgeOut = e.getVertex_Out();
					out.write(edgeIn[0] + "," + edgeIn[1] + "," + edgeOut[0] + "," + edgeOut[1]);

					int size = e.getSize();
					for(int i=0; i < size; i++){
						out.write("," + e.getValue(i));
					}
					out.write("\n");
				}
			}
			out.flush();
			out.close();
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}
		
		mapEdges = null;
		
		System.out.println("Saving listEdgeFieldNames to file");
		try{
			FileWriter fstream = new FileWriter(efFile);
			BufferedWriter out = new BufferedWriter(fstream);
			String seperator = "";
			for(int i=0; i < listEdgeFieldNames.length; i++){
				seperator = "";
				out.write(i + ",");
				for(int j=0; j < listEdgeFieldNames[i].length; j++){
					out.write(seperator + listEdgeFieldNames[i][j]);
					seperator = ",";
				}
				out.write("\n");
			}
			out.flush();
			out.close();
		} catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}
		listEdgeFieldNames = null;
	}
	
}
