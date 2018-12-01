package team.cs425.g54;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class SpoutThread extends Thread {
    public String spoutFile;
    public String appType;
    public CopyOnWriteArrayList<Node> children;
    int pointer;
    int port;
    boolean isFinished = false;
    ArrayList<Socket> childrenSocket;
    ArrayList<ObjectOutputStream> childrenOutputStream;
    
    private final int BYTE_LEN=10000;
    
    public SpoutThread(String spoutFile,String appType,CopyOnWriteArrayList<Node> children){
        this.spoutFile = spoutFile;
        this.appType = appType;
        this.children = children;
        pointer=0;
        port=Detector.workerPort;
        
    }
    
    public void connectToChildren() {
    	for(Node node:children) {
    		try {
	    		Socket socket = new Socket(node.nodeAddr, port);
	    		childrenSocket.add(socket);
	    		ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
	    		childrenOutputStream.add(os);
    		} catch (IOException e) {
				e.printStackTrace();
			} 
    	}
    }
    
    @Override
    public void run() {
    	System.out.println("Spout started");
    	//Connect to children
    	connectToChildren();
    	while(!Thread.currentThread().isInterrupted() && !isFinished) {
	    	//
	    	BufferedReader bufferedReader;
	    	int linenumber=0;
	    	try {
	    		bufferedReader = new BufferedReader(new FileReader(spoutFile));
	    		//System.out.println("Read file "+spoutFile);
				String line = bufferedReader.readLine();
				//System.out.println("Line is:"+line);
				while(line!=null) {
					//System.out.println("Line is:"+line);
					linenumber++;
					if(line.isEmpty()) {
						line = bufferedReader.readLine();
						continue;
					}
					HashMap<String,String> emit=new HashMap<String, String>();
					emit.put(Integer.toString(linenumber), line);
					sendTuple(emit);
					
					line = bufferedReader.readLine();
				}
				System.out.println("################linenumber "+linenumber+"#########");
				System.err.println("####################### FILE END ############################");
				isFinished = true;
				
			} catch (IOException e) {
				e.printStackTrace();
			} 
    	}
    }
    
    public void sendTuple(HashMap<String,String> tuple) {
    	if(children.size()>0) {
    		try {
    			childrenOutputStream.get(pointer).writeObject(tuple);
    			childrenOutputStream.get(pointer).flush();
	            System.out.println("tuple sent "+tuple.values().toString());
    		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		pointer = (pointer + 1) % children.size();
    	}
    }
    
    public void stopThread() {
    	// Close sockets and os
    	try {
	    	for(ObjectOutputStream os:childrenOutputStream) {
	    		os.close();
	    	}
	    	for(Socket socket:childrenSocket) {
	    		socket.close();
	    	}
    	}catch (IOException e) {
			e.printStackTrace();
		} 
    	isFinished = true;
    }

}
