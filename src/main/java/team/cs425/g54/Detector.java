package team.cs425.g54;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.IOUtils;
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
	public int testnode = 12001;
	public String configFile="mp.config";
	public static Node master;
	
	Logger logger = Logger.getLogger("main.java.team.cs425.g54.Detector");

	Socket client;

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
		sdfsListener = new SDFSListener(myNode,testnode);
		sdfsListener.start();
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
		if (master!=null) {
			System.out.println("Node "+master.nodeID);
		}else {
			System.out.println("Nobody");
		}
		
	public void store(){
		File dict = new File(""); // get all local file
		File[] fileArray = dict.listFiles();
		if(fileArray==null)
			return;
		System.out.println("Current store file");
		for(int i=0;i<fileArray.length;i++){
			if(fileArray[i].isFile()){
				System.out.println(fileArray[i].getName());
			}
		}
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
	
	public void setMaster() {
		master=myNode;
		//Broadcast this message to all 
		broadcastMasterMsgToAll();
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
					mp.joinGroup();
					mp.showGroupList();
				}
				
				if(cmdInput.toLowerCase().equals("leave")) {
					mp.leaveGroup();
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
					mp.store();
					mp.client = new Socket(mp.introducer.nodeAddr,mp.testnode);
					mp.client.setSoTimeout(200000); //
					DataOutputStream outputStream = new DataOutputStream(mp.client.getOutputStream());
					JSONObject obj = new JSONObject();
					obj.put("type","store");

					outputStream.writeUTF(obj.toString());
				}
				if(cmdInput.toLowerCase().contains("put")){
					mp.client = new Socket(mp.introducer.nodeAddr,mp.testnode);
					mp.client.setSoTimeout(200000);
					String filename = "L19-20.FA18.pdf";
					String sdfs = "sdfs";
					String timestamp = String.valueOf(System.currentTimeMillis());
					JSONObject obj = new JSONObject();
					obj.put("type","put");
					obj.put("sdfsName",sdfs);
					obj.put("timestamp",timestamp);
					DataOutputStream outputStream = new DataOutputStream(mp.client.getOutputStream());
					outputStream.writeUTF(obj.toString());

					FileInputStream fis = new FileInputStream(filename);
					IOUtils.copy(fis,outputStream);
					outputStream.flush();
				}
				if(cmdInput.toLowerCase().equals("get")) {
					mp.client = new Socket(mp.introducer.nodeAddr,mp.testnode);
					mp.client.setSoTimeout(200000);
					String local = "testfile1";
					String sdfs = "sdfs";
					JSONObject obj = new JSONObject();
					obj.put("type","get");
					obj.put("sdfsName",sdfs);
					DataOutputStream outputStream = new DataOutputStream(mp.client.getOutputStream());
					outputStream.writeUTF(obj.toString());
					DataInputStream input =  new DataInputStream(mp.client.getInputStream());
					FileOutputStream fos = new FileOutputStream(local);
					IOUtils.copy(input,fos);
					fos.flush();

				}
				if(cmdInput.toLowerCase().equals("get_version")) {
					mp.client = new Socket(mp.introducer.nodeAddr,mp.testnode);
					mp.client.setSoTimeout(200000);
					String local = "testfile2";
					String sdfs = "sdfs";
					JSONObject obj = new JSONObject();
					obj.put("type","get_version");
					obj.put("sdfsName",sdfs);
					obj.put("versionNum",2);
					DataOutputStream outputStream = new DataOutputStream(mp.client.getOutputStream());
					outputStream.writeUTF(obj.toString());
					DataInputStream input =  new DataInputStream(mp.client.getInputStream());
					FileOutputStream fos = new FileOutputStream(local);
					IOUtils.copy(input,fos);
					fos.flush();

				}
			  }
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}

		}

		System.out.println("Program End");
		
		
	}
	
	
}
