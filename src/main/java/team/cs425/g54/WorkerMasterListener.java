package main.java.team.cs425.g54;

import java.awt.List;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;

public class WorkerMasterListener extends Thread {
	private DatagramSocket socket;
    

    public WorkerMasterListener() throws IOException {
        socket=new DatagramSocket(Detector.sendTaskPort);
        
    }

    @Override
    public void run() {
        System.out.println("[Worker] Worker Started");
        
        while(!Thread.currentThread().isInterrupted() ){ // running
            boolean receivedTask = false;     //mark whether the data is received
            byte[] receivedData = new byte[2048];
            DatagramPacket receivedPacket = new DatagramPacket(receivedData,receivedData.length); // receive package
            try{
                socket.receive(receivedPacket);
                receivedTask = true;
                handleMessage(new String(receivedPacket.getData()));
            } catch (IOException e) {
//                e.printStackTrace();
                continue;  // packet has not come yet
            }
        }
    }

    private void handleMessage(String receivedData) {
        try {
        	JSONObject jsonData = new JSONObject(receivedData);
        	String workerType = jsonData.get("workerType").toString();
        	if(workerType.equals("spout")) {
        		String appType = jsonData.get("appType").toString();
        		String filename = jsonData.get("filename").toString();
        		JSONArray arr = jsonData.getJSONArray("children");
        		CopyOnWriteArrayList<Node> childrenList = new CopyOnWriteArrayList<Node>();
        		for(int i=0;i<arr.length();i++){
                    Node tmp_node = new Node(0,"",0);
                    tmp_node.nodeID = Integer.parseInt(arr.getJSONObject(i).get("nodeID").toString());
                    tmp_node.nodeAddr = arr.getJSONObject(i).get("nodeAddr").toString();
                    tmp_node.nodePort = Detector.workerPort;
 
                    childrenList.add(tmp_node);
                }
        		Spout spout = new Spout(appType, filename, childrenList);
        		spout.open();
        	}
        	if(workerType.equals("bolt")) {
        		String appType = jsonData.get("appType").toString();
        		JSONArray arr = jsonData.getJSONArray("children");
        		CopyOnWriteArrayList<Node> childrenList = new CopyOnWriteArrayList<Node>();
        		for(int i=0;i<arr.length();i++){
                    Node tmp_node = new Node(0,"",0);
                    tmp_node.nodeID = Integer.parseInt(arr.getJSONObject(i).get("nodeID").toString());
                    tmp_node.nodeAddr = arr.getJSONObject(i).get("nodeAddr").toString();
                    tmp_node.nodePort = Detector.workerPort;
 
                    childrenList.add(tmp_node);
                }
        		Bolt bolt = new Bolt(appType, childrenList);
        		if(appType.equals("filter")) {
        			bolt.filterWord=jsonData.get("filterWord").toString();
        		}
        	}
        	
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } 
    }

    
}
