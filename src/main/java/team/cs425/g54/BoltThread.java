package team.cs425.g54;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class BoltThread extends Thread {
    public String appType;
    public CopyOnWriteArrayList<Node> children;
    int pointer;
    int port;
    private DatagramSocket socket;
    String info="";
    private final int BYTE_LEN=10000;
    final int TIMEOUT = 5000;
    boolean stopped_sign = false;
    
    // File for working
    String workingFilepath = "files/tmpBolt";
    // For word count
    HashMap<String,Integer> wordCounter=new HashMap<String, Integer>();
    
    // For streaming to write a file
    long lastWriteTime;
    long timeToSend = 5000;

    public BoltThread(String appType,CopyOnWriteArrayList<Node> children){
        this.appType = appType;
        this.children = children;
        pointer=0;
        lastWriteTime=System.currentTimeMillis();
        port=Detector.workerPort;
        try {
			socket=new DatagramSocket(port);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
    }
    
    
    @Override
    public void run() {
    	System.out.println("Bolt started");
    	// Delete the previous working file
    	File tmpFile = new File(workingFilepath);
    	tmpFile.delete();
    	
    	// Start listening
        byte [] receiveData=new byte[BYTE_LEN];
        while(!Thread.currentThread().isInterrupted() && !stopped_sign) {
            try {
                DatagramPacket pack=new DatagramPacket(receiveData,receiveData.length);
                socket.receive(pack);
                ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(receiveData));
                HashMap<String,String> in = (HashMap<String,String>) is.readObject();
                is.close();

//              // Deal
                dealWithData(in);
                
                // For stream to check if the file need to be written
                checkWriteDownTime(false);
                
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        checkWriteDownTime(true);
    }
	
	public void dealWithData(HashMap<String,String> inData){
		System.out.println("Data received: "+inData.values().toString());
		HashMap<String,String> outData = new HashMap<String,String>();
		
		if(appType.equals("filter")) {
			if(children.size()==0) {
				System.out.println("Write to file");
				BufferedWriter bufferedWriter;
				try {
					bufferedWriter = new BufferedWriter(new FileWriter(workingFilepath, true));
					for (Entry<String, String> entry : inData.entrySet()) {
						bufferedWriter.write(entry.getValue());
						
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else {
				System.out.println("Send to children");
				for (Entry<String, String> entry : inData.entrySet()) {
					for(String s: entry.getValue().split(" ")) {
						if(s.equals(info)) {
							outData.put(entry.getKey(), entry.getValue());
							break;
						}
					}
				}
				// Send
	            sendTuple(outData);
			}
			
    	}
		if (appType.equals("wordCount")) {
			if(children.size()==0) {
				for (Entry<String, String> entry : inData.entrySet()) {
					String key = entry.getKey();
					if (!wordCounter.containsKey(key)) {  
						wordCounter.put(key, 1);
					}
					else {
						int count = wordCounter.get(key);
					    wordCounter.put(key, count + 1);
					}
				}
				System.out.println("Write to file");
				BufferedWriter bufferedWriter;
				try {
					bufferedWriter = new BufferedWriter(new FileWriter(workingFilepath));
					for (Entry<String, Integer> entry : wordCounter.entrySet()) {
						bufferedWriter.write(entry.getKey()+" "+entry.getValue());
	
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else {
				System.out.println("Send to children");
				for (Entry<String, String> entry : inData.entrySet()) {
					for(String s: entry.getValue().split(" ")) {
						outData.put(s, "1");
			            sendTuple(outData);
			            outData = new HashMap<String,String>();
					}
				}
				
			}
		}
		
    	
		
	}

	public void sendTuple(HashMap<String,String> tuple) {
    	if(children.size()>0) {
    		try {
	    		// Connect children[pointer]
	    		String address = children.get(pointer).nodeAddr; 
	    		int port = children.get(pointer).nodePort;
	    		// Send tuple
	    		ByteArrayOutputStream bo=new ByteArrayOutputStream(BYTE_LEN);
	            ObjectOutputStream os;
				os = new ObjectOutputStream(bo);
	            os.writeObject(tuple);
	            os.flush();
	            byte [] sendBytes=bo.toByteArray();
	            DatagramPacket dp=new DatagramPacket(sendBytes,sendBytes.length, InetAddress.getByName(address),port);
	            DatagramSocket dSock=new DatagramSocket();
	            dSock.send(dp);
	            os.close();
	            dSock.close();
    		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		pointer = (pointer + 1) % children.size();
    	}
    }
	
	public ArrayList<Node> getNodeList(String str){
		
		ArrayList<Node> nodes = new ArrayList<>();
		try {
			JSONArray objArray = new JSONArray(str);
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
	
	public void checkWriteDownTime(boolean forced) {
		// If filter, this bolt will put the file into sdfs every time unit
		if ((System.currentTimeMillis() - lastWriteTime) > timeToSend || forced) {
			try {
				DatagramSocket ds = new DatagramSocket();
				ds.setSoTimeout(TIMEOUT);

				String local = workingFilepath;
				String sdfs = appType;
				String timestamp = String.valueOf(System.currentTimeMillis());
				// send msg to master
				JSONObject obj = new JSONObject();
				obj.put("type","toMaster");
				obj.put("command","put");
				obj.put("sdfsName",sdfs);
				obj.put("timestamp",timestamp);
				obj.put("nodeID", Detector.myNode.nodeID);
				obj.put("nodeAddr", Detector.myNode.nodeAddr);
				obj.put("nodePort", Detector.myNode.nodePort);
				String msgToMaster = obj.toString();
				InetAddress address = InetAddress.getByName(Detector.master.nodeAddr);
				DatagramPacket dpSent= new DatagramPacket(msgToMaster.getBytes(),msgToMaster.length(),address,Detector.master.nodePort);
				byte[] data = new byte[2048];
				DatagramPacket dpReceived = new DatagramPacket(data, 2048);
				ds.send(dpSent);
				ds.receive(dpReceived);

				String dpRecivedData = new String(dpReceived.getData());
				System.out.println("Received "+dpRecivedData);
				ArrayList<Node> nodes = getNodeList(dpRecivedData);

				for(Node node:nodes){
					Socket clientToNodes = new Socket(node.nodeAddr,Detector.toNodesPort);
					JSONObject obj2 = new JSONObject();
					obj2.put("type","put");
					obj2.put("sdfsName",sdfs);
					obj2.put("timestamp",timestamp);
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
		
	}
	
	public void stopThread() {
		socket.close();
		stopped_sign = true;
		Thread.currentThread().interrupt();
	}
}
