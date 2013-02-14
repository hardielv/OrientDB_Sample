
public class RandomDB_Vertex {
	protected String type;
	protected String [] fieldValues;
	protected int size;
	
	public RandomDB_Vertex(String t, int s){
		type = t;
		size = s;
		fieldValues = new String[size];
	}
	
	public String getType() { return type; }
	public int getSize(){ return size; }
	public String getValue(int i){ return fieldValues[i]; }
	
	public void addValue(int i, String value){
		fieldValues[i] = value;
	}
	

}
