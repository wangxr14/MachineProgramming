package team.cs425.g54;

import java.awt.List;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class WorkerMasterListener extends Thread {
	private DatagramSocket socket;
	public SpoutThread workingSpout = null;
	public BoltThread workingBolt = null;
    private DatagramSocket workerSocket= null;
    static Logger logger = Logger.getLogger("main.java.team.cs425.g54.WorkerMasterListener");

    public WorkerMasterListener(int port) throws IOException {
        socket=new DatagramSocket(port);
        // For spout or bolt
        //workerSocket=new DatagramSocket(Detector.workerPort);
        workingSpout = null;
    	workingBolt = null;
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
        
        System.out.println("Worker master thread stops");
    }

    private void handleMessage(String receivedData) {
        try {
        	JSONObject jsonData = new JSONObject(receivedData);
            logger.info("json received: " + jsonData.toString());
        	String workerType = jsonData.get("workerType").toString();
        	logger.info("WorkerType received is "+workerType);
        	// Stop the current worker if there is any
        	logger.info("current spout is: "+workingSpout);
        	logger.info("current bolt is: "+workingBolt);
        	
        	if (workingSpout != null) {
        		logger.info("current spout id is: "+workingSpout.getId());
        		while(workingSpout.isAlive()) {
        			logger.info("working spout "+workingSpout.getId()+" is still alive");
        			workingSpout.stopThread();
        			logger.info("working spout "+workingSpout.getId()+" alive? "+workingSpout.isAlive());
        			sleep(1000);
        		}
        		logger.info("Previous spout stopped");
        		workingSpout = null;
        	}
        	if (workingBolt != null ) {
        		logger.info("current bolt id is: "+workingBolt.getId());
        		while(workingBolt.isAlive()) {
        			logger.info("working bolt "+workingBolt.getId()+" is still alive");
        			workingBolt.stopThread();
        			logger.info("working bolt "+workingBolt.getId()+" alive? "+workingBolt.isAlive());
        			sleep(1000);
        			logger.info("after sleep, working bolt "+workingBolt.getId()+" alive? "+workingBolt.isAlive());
        		}
        		logger.info("Previous bolt stopped");
        		workingBolt = null;
        	}
        	logger.info("Now we rest a little");
        	//sleep(10000);
        	logger.info("start new work now");
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
        		logger.info("children received");
        		for(Node node:childrenList) {
        			logger.info("children "+node.nodeID);
        		}
        		//SpoutThread spout = new SpoutThread(filename, appType, childrenList);
        		//workingSpout = spout;
        		workingSpout = new SpoutThread(filename, appType, childrenList);
        		workingSpout.start();
        		//spout.start();
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
        		logger.info("children received");
        		String info=jsonData.get("info").toString();
        		//BoltThread bolt = new BoltThread(appType, childrenList);
        		workingBolt = new BoltThread(appType, childrenList, info);
        		//if(appType.equals("filter")) {
        		//}
        		if(appType.equals("wordCount")) {
        			BoltThread.wordCounter.clear();
        			logger.info("current word count size:"+workingBolt.wordCounter.size());
        		}
        		//workingBolt = bolt;
        		workingBolt.start();
        	}
        	
        }  catch (JSONException e) {
            e.printStackTrace();
        } catch(InterruptedException e) {
        	e.printStackTrace();
        }
    }

    
}
