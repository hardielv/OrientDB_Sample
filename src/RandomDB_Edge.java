public class RandomDB_Edge extends RandomDB_Vertex{
		int vertexType_in, vertexType_out;
		int vertexIndex_in, vertexIndex_out;
		
		public RandomDB_Edge(String t, int s){
			super(t, s);
		}
		
		public void connect(int[] v_in, int[] v_out){
			vertexType_in = v_in[0];
			vertexIndex_in = v_in[1];
			vertexType_out = v_out[0];
			vertexIndex_out = v_out[1];
		}
		
		
		public int[] getVertex_In(){
			int[] v = {vertexType_in, vertexIndex_in};
			return v;
		}
		
		public int[] getVertex_Out(){
			int[] v = {vertexType_out, vertexIndex_out};
			return v;
		}
	}
	

