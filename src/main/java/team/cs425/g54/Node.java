package team.cs425.g54;

public class Node {
	public int nodeID = 0;
	public String nodeAddr = "";
	public int nodePort = 0;
	
	public Node(int ID, String addr, int port) {
		nodeID=ID;
		nodeAddr=addr;
		nodePort=port;
	}
	
	public Node() {
		
	}
//	@Override
	public boolean equals(Object obj){
		if(this == obj){
			return true;
		}
		if(obj == null){
			return false;
		}
		if(!(obj instanceof Node)){
			return false;
		}
		final Node node = (Node)obj;
		if(node.nodeID ==this.nodeID && node.nodeAddr.equals(this.nodeAddr) && node.nodePort==this.nodePort)
			return true;
		else
			return false;
	}

	public String nodeInfoToString() {
		return ""+this.nodeID+";"+this.nodeAddr+";"+this.nodePort;

	public int hashCode() {
		return 17+31*nodeAddr.hashCode()+31*nodeID+31*nodePort;
	}
}
