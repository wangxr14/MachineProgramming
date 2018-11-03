package team.cs425.g54;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;

import java.util.logging.Logger;



public class Detector {
	public Node myNode;
	// Set a contactor(fixed)
	public Node introducer = new Node(1,"172.22.158.178", 12345);
	// Membership List & Group
	// Make sure they are thread-safe
	public static CopyOnWriteArrayList<Node> membershipList = new CopyOnWriteArrayList<Node>();
	public static CopyOnWriteArrayList<Node> groupList = new CopyOnWriteArrayList<Node>();
	// Listen & Ping
	public Listener listener;
	public SDFSListener sdfsListener;
	public Pinger pinger;
	public int pingerPort = 12333;
	public int nodePort = 12345;
	public int toNodesPort = 12002;
	public String configFile="mp.config";
	public static Node master;
    public static MasterInfo masterInfo;
    public static StoreInfo storeInfo;

	Logger logger = Logger.getLogger("main.java.team.cs425.g54.Detector");

	Socket clientToNodes;
	final int TIMEOUT = 5000;

	public void setConfig() {
		// Read From File to Know the ID of this VM
    	try {
    		BufferedReader in=new BufferedReader(new FileReader(configFile));
    		String line=in.readLine();
    		int count=1;
    		int myID=0;
    		if(line!=null) {
    			myID=Integer.parseInt(line);
    			line=in.readLine();
    		}
    		while(line!=null) {
    			String[] splites=line.split(";");
    			int id= Integer.parseInt(splites[0]);
    			String addr = splites[1];
    			int port = Integer.parseInt(splites[2]);
    			
    			if(id==myID) {
    				myNode=new Node(id,addr,port);
    				return;
    			}
    			line=in.readLine();
    		}
    		
    	}catch(IOException e){
    		e.printStackTrace();
    	}
	}

	public void init() {
		setConfig();
		pinger = new Pinger(myNode, pingerPort, membershipList, groupList);
		pinger.start();

		listener = new Listener(myNode, membershipList, groupList, myNode.nodeID==introducer.nodeID);
		listener.start();
		sdfsListener = new SDFSListener(myNode,toNodesPort);
		sdfsListener.start();
		storeInfo.initFileLists(myNode);
	}


	
	public int findPositionToInsert(Node node, CopyOnWriteArrayList<Node> nodeList) {
		if (nodeList.size()==0)
			return 0;
		for (int i=0;i<nodeList.size();i++) {
			if(node.nodeID<nodeList.get(i).nodeID) {
				return i;
			}
		}
		return nodeList.size();
	}
	
	private void joinNodeIntoGroup() {
		int pos = findPositionToInsert(myNode, groupList);
		if (pos==groupList.size()) {
			groupList.add(myNode);
		}
		else {
			groupList.add(pos,myNode);
		}
	}
	
	private void removeNodeFromGroup() {
		for (int i=0;i<groupList.size();i++) {
			Node node = groupList.get(i);
            if (node.nodeID==myNode.nodeID && node.nodeAddr.equals(myNode.nodeAddr) && node.nodePort==myNode.nodePort) {
                groupList.remove(i);
                return;
            }
        }
	}
	
	public void broadcastToAll(String type) {
		try {
			DatagramSocket ds = new DatagramSocket();
		
			for(Node node : groupList) {
	            JSONObject message = new JSONObject();
	            message.put("type", type);
	            message.put("nodeID", node.nodeID);
	            message.put("nodeAddr", node.nodeAddr);
	            message.put("nodePort", node.nodePort);
	            InetAddress address = InetAddress.getByName(node.nodeAddr);
	            DatagramPacket send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, node.nodePort);
	            ds.send(send_message);
	            ds.close();
			}
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e){
			e.printStackTrace();
		} 
		
	}
	
	public void sendMsgToIntroducer(String type) {
		try {
			DatagramSocket ds = new DatagramSocket();
		
			JSONObject message = new JSONObject();
	        message.put("type", type);
	        message.put("nodeID", myNode.nodeID);
	        message.put("nodeAddr", myNode.nodeAddr);
	        message.put("nodePort", myNode.nodePort);
	        InetAddress address = InetAddress.getByName(introducer.nodeAddr);
	        // logger.info("Send join to introducer bytes: "+message.toString().getBytes().length);
	        DatagramPacket send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, introducer.nodePort);
			ds.send(send_message);
			ds.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e){
			e.printStackTrace();
		} 
	}
	
	public void joinGroup() {
		pinger.restartPinger();

		listener.restartListen();
		// If this node is Introducer
		if(myNode.nodeID==introducer.nodeID) {
			System.out.println("I am Introducer");
			joinNodeIntoGroup();
			//broadcastToAll("join");
		}
		else {
			sendMsgToIntroducer("join");
		}
	}


	public void removeNodeFromGroupList(Node node){
		for (int i=0;i<groupList.size();i++){
			Node tmpNode=groupList.get(i);
			if(node.nodeID==tmpNode.nodeID && node.nodeAddr.equals(tmpNode.nodeAddr) && node.nodePort==tmpNode.nodePort){
				groupList.remove(i);
				return;
			}
		}
	}

	
	public void sendLeaveMsg() {

		removeNodeFromGroupList(myNode);
		pinger.stopPinger();
		listener.stopListen();

		try {
			DatagramSocket ds = new DatagramSocket();
		
			for(Node node : membershipList) {
	            JSONObject message = new JSONObject();
	            message.put("type", "leave");
	            message.put("nodeID", myNode.nodeID);
	            message.put("nodeAddr", myNode.nodeAddr);
	            message.put("nodePort", myNode.nodePort);
	            InetAddress address = InetAddress.getByName(node.nodeAddr);
	            // logger.info("Send leave message bytes length: "+ message.toString().getBytes().length);
	            DatagramPacket send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, node.nodePort);
	            ds.send(send_message);
			}
			ds.close();
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e){
			e.printStackTrace();
		} 
	}
	
	public void leaveGroup() {
		if(myNode.nodeID==introducer.nodeID) {
			removeNodeFromGroup();
			broadcastToAll("leave");
		}
		else {
			sendLeaveMsg();
		}
	}
	
	public void showID() {
		System.out.println("This is VM"+myNode.nodeID+", Node Address:"+myNode.nodeAddr+", Node Port:"+myNode.nodePort);
	}
	
	public void showMembershipList() {
		System.out.println("Number of Members:"+membershipList.size());
		for (int i=0;i<membershipList.size();i++) {
			Node node=membershipList.get(i);
			System.out.println("Member"+(i+1)+" :");
			System.out.println("Node ID:"+node.nodeID+", Node Address:"+node.nodeAddr+", Node Port:"+node.nodePort);
		}
	}
	
	public void showGroupList() {
		System.out.println("Size of group:"+groupList.size());
		for (int i=0;i<groupList.size();i++) {
			Node node=groupList.get(i);
			System.out.println("Member"+(i+1)+" :");
			System.out.println("Node ID:"+node.nodeID+", Node Address:"+node.nodeAddr+", Node Port:"+node.nodePort);
		}
	}
	
	public void showMaster() {
        System.out.println("Current master is:");
        if (master != null) {
            System.out.println("Node " + master.nodeID);
        } else {
            System.out.println("Nobody");
        }
    }
    // get jsondata into nodelist
	public ArrayList<Node> getNodeList(String str){
		ArrayList<Node> nodes = new ArrayList<>();
		try {
			JSONObject obj = new JSONObject(str);
			JSONArray objArray = new JSONArray(obj);
			for (int i = 0; i < objArray.length(); i++) {
				Node node = new Node();
				JSONObject jsonNode = objArray.getJSONObject(i);
				node.nodeAddr = jsonNode.get("nodeAddr").toString();
				node.nodeID = Integer.parseInt(jsonNode.get("nodeID").toString());
				node.nodePort = Integer.parseInt(jsonNode.get("nodePort").toString());
				nodes.add(node);
			}

		} catch (JSONException e) {
			e.printStackTrace();
		}
		return nodes;
	}

	public void broadcastMasterMsgToAll() {
		String type = "master";
		try {
			DatagramSocket ds = new DatagramSocket();
		
			for(Node node : groupList) {
	            JSONObject message = new JSONObject();
	            message.put("type", type);
	            message.put("nodeID", master.nodeID);
	            message.put("nodeAddr", master.nodeAddr);
	            message.put("nodePort", master.nodePort);
	            InetAddress address = InetAddress.getByName(node.nodeAddr);
	            DatagramPacket send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, node.nodePort);
	            ds.send(send_message);
			}
			ds.close();
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e){
			e.printStackTrace();
		}
	}
	
	public void setMaster(){
		master=myNode;
		//Broadcast this message to all 
		broadcastMasterMsgToAll();
	}

	public void putCommand(String cmdInput){
		String[] command = cmdInput.split(" ");
		if(command.length<3)
			return;
		try {
			DatagramSocket ds = new DatagramSocket();
			ds.setSoTimeout(TIMEOUT);

			String local = command[1];
			String sdfs = command[2];
			String timestamp = String.valueOf(System.currentTimeMillis());
			// send msg to master
			JSONObject obj = new JSONObject();
			obj.put("type","toMaster");
			obj.put("command","put");
			obj.put("sdfsName",sdfs);
			obj.put("timestamp",timestamp);
			obj.put("nodeID", myNode.nodeID);
			obj.put("nodeAddr", myNode.nodeAddr);
			obj.put("nodePort", myNode.nodePort);
			String msgToMaster = obj.toString();
			InetAddress address = InetAddress.getByName(master.nodeAddr);
			DatagramPacket dpSent= new DatagramPacket(msgToMaster.getBytes(),msgToMaster.length(),address,master.nodePort);
			// get nodeList from master
			byte[] data = new byte[2048];
			DatagramPacket dpReceived = new DatagramPacket(data, 2048);
			ds.send(dpSent);
			ds.receive(dpReceived);

			String dpRecivedData = new String(dpReceived.getData());
			ArrayList<Node> nodes = getNodeList(dpRecivedData);

			for(Node node:nodes){
				clientToNodes = new Socket(node.nodeAddr,toNodesPort);
				JSONObject obj2 = new JSONObject();
				obj2.put("type","put");
				obj2.put("sdfsName",sdfs);
				DataOutputStream outputStream = new DataOutputStream(clientToNodes.getOutputStream());
				outputStream.writeUTF(obj2.toString()); // send the put command to the node first
				FileInputStream fis = new FileInputStream(local);
				IOUtils.copy(fis,outputStream);
				outputStream.flush();
				clientToNodes.close();
			}

			ds.close();
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}

	}
	public void getCommand(String cmdInput){
		String[] command = cmdInput.split(" ");
		if(command.length<3)
			return;
		try {
			DatagramSocket ds = new DatagramSocket();
			ds.setSoTimeout(TIMEOUT);

			String local = command[2];
			String sdfs = command[1];

			// send msg to master
			JSONObject obj = new JSONObject();
			obj.put("type","toMaster");
			obj.put("command","get");
			obj.put("sdfsName",sdfs);

			String msgToMaster = obj.toString();
			InetAddress address = InetAddress.getByName(master.nodeAddr);
			DatagramPacket dpSent= new DatagramPacket(msgToMaster.getBytes(),msgToMaster.length(),address,master.nodePort);

			// get nodeList from master
			byte[] data = new byte[2048];
			DatagramPacket dpReceived = new DatagramPacket(data, 2048);
			ds.send(dpSent);
			ds.receive(dpReceived);

			String dpRecivedData = new String(dpReceived.getData());
			ArrayList<Node> nodes = getNodeList(dpRecivedData);

			for(Node node:nodes){
				JSONObject obj2 = new JSONObject();
				obj2.put("type","get");
				obj2.put("sdfsName",sdfs);
				clientToNodes = new Socket(node.nodeAddr,toNodesPort);
				DataOutputStream output = new DataOutputStream(clientToNodes.getOutputStream());
				// send get request to get the data
				output.writeUTF(obj2.toString());
				// get the file data
				DataInputStream input =  new DataInputStream(clientToNodes.getInputStream());

				FileOutputStream fos = new FileOutputStream(local);
				IOUtils.copy(input,fos);
				fos.flush();
				clientToNodes.close();
			}

		} catch (SocketException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteCommand(String cmdInput){
		String[] command = cmdInput.split(" ");
		if(command.length<2)
			return;
		try {
			DatagramSocket ds = new DatagramSocket();
			ds.setSoTimeout(TIMEOUT);

			String sdfs = command[1];

			// send msg to master
			JSONObject obj = new JSONObject();
			obj.put("type","toMaster");
			obj.put("command","delete");
			obj.put("sdfsName",sdfs);

			String msgToMaster = obj.toString();
			InetAddress address = InetAddress.getByName(master.nodeAddr);
			DatagramPacket dpSent= new DatagramPacket(msgToMaster.getBytes(),msgToMaster.length(),address,master.nodePort);

			// get nodeList from master
			byte[] data = new byte[2048];
			DatagramPacket dpReceived = new DatagramPacket(data, 2048);
			ds.send(dpSent);
			ds.receive(dpReceived);

			String dpRecivedData = new String(dpReceived.getData());
			ArrayList<Node> nodes = getNodeList(dpRecivedData);

			for(Node node:nodes){
				JSONObject obj2 = new JSONObject();
				obj2.put("type","delete");
				obj2.put("sdfsName",sdfs);
				clientToNodes = new Socket(node.nodeAddr,toNodesPort);
				DataOutputStream output = new DataOutputStream(clientToNodes.getOutputStream());
				output.writeUTF(obj2.toString());
				clientToNodes.close();
			}

		} catch (SocketException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void getVersionCommand(String cmdInput){
		String[] command = cmdInput.split(" ");
		if(command.length<4)
			return;
		String sdfsfile = command[1];
		String versionNum = command[2];
		String local = command[3];


		try {
			DatagramSocket ds = new DatagramSocket();
			ds.setSoTimeout(TIMEOUT);
			// send msg to master
			JSONObject obj = new JSONObject();
			obj.put("type","toMaster");
			obj.put("command","get_version");
			obj.put("sdfsName",sdfsfile);
			obj.put("versionNum",versionNum);

			String msgToMaster = obj.toString();
			InetAddress address = InetAddress.getByName(master.nodeAddr);
			DatagramPacket dpSent= new DatagramPacket(msgToMaster.getBytes(),msgToMaster.length(),address,master.nodePort);

			// get nodeList from master
			byte[] data = new byte[2048];
			DatagramPacket dpReceived = new DatagramPacket(data, 2048);
			ds.send(dpSent);
			ds.receive(dpReceived);

			String dpRecivedData = new String(dpReceived.getData());
			ArrayList<Node> nodes = getNodeList(dpRecivedData);

			for(Node node:nodes){
				JSONObject obj2 = new JSONObject();
				obj2.put("type","get_version");
				obj2.put("sdfsName",sdfsfile);
				obj2.put("versionNum",versionNum);
				clientToNodes = new Socket(node.nodeAddr,toNodesPort);
				DataOutputStream output = new DataOutputStream(clientToNodes.getOutputStream());
				output.writeUTF(obj2.toString());
				DataInputStream input =  new DataInputStream(clientToNodes.getInputStream());
				FileOutputStream fos = new FileOutputStream(local);
				IOUtils.copy(input,fos);
				fos.flush();
				clientToNodes.close();
			}

		} catch (JSONException e) {
			e.printStackTrace();
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	public void lsCommand(String cmdInput){
		String[] command = cmdInput.split(" ");
		if(command.length<2)
			return;
		String sdfsName = command[1];

		try {
			DatagramSocket ds  = new DatagramSocket();
			ds.setSoTimeout(TIMEOUT);
			// send msg to master
			JSONObject obj = new JSONObject();
			obj.put("type","toMaster");
			obj.put("command","ls");
			obj.put("sdfsName",sdfsName);

			String msgToMaster = obj.toString();
			InetAddress address = InetAddress.getByName(master.nodeAddr);
			DatagramPacket dpSent= new DatagramPacket(msgToMaster.getBytes(),msgToMaster.length(),address,master.nodePort);

			// get nodeList from master
			byte[] data = new byte[2048];
			DatagramPacket dpReceived = new DatagramPacket(data, 2048);
			ds.send(dpSent);
			ds.receive(dpReceived);

			String dpRecivedData = new String(dpReceived.getData());
			ArrayList<Node> nodes = getNodeList(dpRecivedData);
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	public static void main(String[] args) {  
		
		Detector mp = new Detector();
		mp.init();
		// User input

		InputStreamReader is_reader = new InputStreamReader(System.in);
		while(true) {
			try {

				System.out.println("Input Your Command:");
				String cmdInput = new BufferedReader(is_reader).readLine();
				System.out.println("Get Input:"+cmdInput);
				// Quit this program
				if(cmdInput.toLowerCase().equals("quit")) {
					break;
				}
				
				// Join command
				if(cmdInput.toLowerCase().equals("join")) {
					mp.storeInfo.deleteAllSDFSFilesOnDisk();
					mp.joinGroup();
					mp.showGroupList();
				}
				
				if(cmdInput.toLowerCase().equals("leave")) {
					mp.leaveGroup();
					mp.storeInfo.deleteAllSDFSFilesOnDisk();
				}
				if(cmdInput.toLowerCase().equals("show")) {
					mp.showID();
					mp.showMembershipList();
					mp.showGroupList();
					mp.showMaster();
				}
				if(cmdInput.toLowerCase().equals("master")) {
					mp.setMaster();
				if(cmdInput.toLowerCase().equals("store")) {
				    storeInfo.showFiles();
				}
				if(cmdInput.toLowerCase().contains("put")){
					mp.putCommand(cmdInput);
				}
				if(cmdInput.toLowerCase().contains("get")) {
					mp.getCommand(cmdInput);
				}
					if(cmdInput.toLowerCase().contains("delete")) {
						mp.deleteCommand(cmdInput);
					}
				if(cmdInput.toLowerCase().contains("get_version")) {
						mp.getVersionCommand(cmdInput);
				}
				if(cmdInput.toLowerCase().contains("ls")){
						mp.lsCommand(cmdInput);
				}
			  }
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		System.out.println("Program End");
		
		
	}
	
	
}
