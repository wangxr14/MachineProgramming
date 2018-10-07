package team.cs425.g54;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.json.JSONObject;
import org.json.JSONException;
import java.util.logging.Logger;


public class Pinger extends Thread{
	
	public CopyOnWriteArrayList<Node> memberList = new CopyOnWriteArrayList<Node>();
	public CopyOnWriteArrayList<Node> groupList = new CopyOnWriteArrayList<Node>();
	
	public int myPort = 0;
	private static final int TIMEOUT = 5000;
	private boolean stopped = false;
	static Logger logger = Logger.getLogger("main.java.team.cs425.g54.Pinger");
	
	public Pinger(int port, CopyOnWriteArrayList<Node> memberList, CopyOnWriteArrayList<Node> groupList ) {
		this.memberList = memberList;
		this.groupList = groupList;
		stopped = false;
		myPort = port;
	}
	
	public void showMembershipList() {
		for (int i=0;i<memberList.size();i++) {
			Node node=memberList.get(i);
			System.out.println("Member"+(i+1)+" :");
			System.out.println("Node ID:"+node.nodeID+", Node Address:"+node.nodeAddr+", Node Port:"+node.nodePort);
		}
	}

	
	private String packPingMsg() {
		JSONObject jsonObj = new JSONObject();
		try{
			jsonObj.put("type", "ping");	
		} catch (JSONException e){
			e.printStackTrace();
		}
		return jsonObj.toString();	
	}
	
	private String packDeleteMsg(Node node) {
		JSONObject jsonObj = new JSONObject();
		try{
			
			jsonObj.put("type", "delete");
			jsonObj.put("nodeID", node.nodeID);
			jsonObj.put("nodeAddr", node.nodeAddr);
			jsonObj.put("nodePort", node.nodePort);
		} catch (JSONException e){
			e.printStackTrace();
		}
		return jsonObj.toString();	
	}
	
	public void removeNodeFromMemberList(Node node){
		for (int i=0;i<memberList.size();i++){
			Node tmpNode=memberList.get(i);
			if(node.nodeID==tmpNode.nodeID && node.nodeAddr.equals(tmpNode.nodeAddr) && node.nodePort==tmpNode.nodePort){
				memberList.remove(i);
				return;
			}
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

	public void removeNode(Node node) {
		removeNodeFromMemberList(node);
		removeNodeFromGroupList(node);

		for (Node member : memberList) {
			try {
				DatagramSocket ds = new DatagramSocket();
				byte[] data = new byte[1024];
				String deleteMsg = packDeleteMsg(node);
				
				InetAddress address = InetAddress.getByName(member.nodeAddr);
				
				DatagramPacket dpSent= new DatagramPacket(deleteMsg.getBytes(),deleteMsg.length(),address,member.nodePort);	
				
				ds.send(dpSent);
				
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} 
			
		}
	}

	
	public void stopPinger() {
		stopped = true;
	}
	
	private void ping(Node node) throws IOException {
		boolean receivedResponse = false;
		try {
			DatagramSocket ds = new DatagramSocket();
			ds.setSoTimeout(TIMEOUT);
			byte[] data = new byte[1024];
			
			String pingMsg = packPingMsg();
			InetAddress address = InetAddress.getByName(node.nodeAddr);
			
			
			
			DatagramPacket dpSent= new DatagramPacket(pingMsg.getBytes(),pingMsg.length(),address,node.nodePort);	
			DatagramPacket dpReceived = new DatagramPacket(data, 1024);
			
			ds.send(dpSent);
			
			ds.receive(dpReceived);
			receivedResponse = true;
			
			if(!receivedResponse) {
				//updater.removeNode(node);
				
				// Update my memberList
				// Or wait for next round to update?
			}
			
			
		} catch(SocketTimeoutException e){
			logger.warning("Node "+node.nodeID+"Fails!");
			receivedResponse = false;
			removeNode(node);
		}catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void run() {
		
		while(!Thread.currentThread().isInterrupted() && !stopped) {
			int memPointer=0;
			if(memberList.size()>0){
				Node node = memberList.get(memPointer);
				try {
					ping(node);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				memPointer = (memPointer+1)%memberList.size();
			}
		}
	}
}
