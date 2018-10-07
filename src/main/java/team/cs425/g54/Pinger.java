package team.cs425.g54;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.json.JSONObject;
import org.json.JSONException;


public class Pinger extends Thread{
	
	public CopyOnWriteArrayList<Node> memberList = new CopyOnWriteArrayList<Node>();
	public int myPort = 0;
	private static final int TIMEOUT = 5000;
	private boolean stopped = false;
	
	public Pinger(int port, CopyOnWriteArrayList<Node> memberList) {
		this.memberList = memberList;
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
	
	public void removeNode(Node node) {
		for (Node member : memberList) {
			try {
				DatagramSocket ds = new DatagramSocket(myPort);
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
		try {
			DatagramSocket ds = new DatagramSocket(myPort);
			ds.setSoTimeout(TIMEOUT);
			byte[] data = new byte[1024];
			
			String pingMsg = packPingMsg();
			InetAddress address = InetAddress.getByName(node.nodeAddr);
			
			boolean receivedResponse = false;
			
			DatagramPacket dpSent= new DatagramPacket(pingMsg.getBytes(),pingMsg.length(),address,node.nodePort);	
			DatagramPacket dpReceived = new DatagramPacket(data, 1024);
			
			ds.send(dpSent);
			
			ds.receive(dpReceived);
			receivedResponse = true;
			
			if(!receivedResponse) {
				//updater.removeNode(node);
				removeNode(node);
				// Update my memberList
				// Or wait for next round to update?
			}
			
			
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void run() {
		int memPointer = 0;
		
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
