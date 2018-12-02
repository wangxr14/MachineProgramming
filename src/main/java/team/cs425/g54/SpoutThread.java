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
import java.util.logging.Logger;

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
    boolean spoutOpen = false;
    CopyOnWriteArrayList<Socket> childrenSocket;
    CopyOnWriteArrayList<ObjectOutputStream> childrenOutputStream;
    
    static Logger logger = Logger.getLogger("main.java.team.cs425.g54.SpoutThread");
    
    private final int BYTE_LEN=10000;
    
    public SpoutThread(String spoutFile,String appType,CopyOnWriteArrayList<Node> children){
        this.spoutFile = spoutFile;
        this.appType = appType;
        this.children = children;
        pointer=0;
        port=Detector.workerPort;
        
        childrenSocket = new CopyOnWriteArrayList<Socket>();
        childrenOutputStream = new CopyOnWriteArrayList<ObjectOutputStream>();
        
        logger.info("new spout "+Thread.currentThread().getId());
        printChildren();
        
    }
    
    public void printChildren() {
    	for(Node node:children) {
    		logger.info("children :"+node.nodeID);
    	}
    }
    
    public void connectToChildren() {
    	printChildren();
    	ArrayList<Node> childrenToConnect = new ArrayList<Node>();
    	for(Node node:children) {
    		childrenToConnect.add(node);
    	}
    	while(childrenToConnect.size()>0) {
    		ArrayList<Node> tmp = new ArrayList<Node>();
	    	for(Node node:childrenToConnect) {
	    		try {
		    		Socket socket = new Socket(node.nodeAddr, port);
		    		childrenSocket.add(socket);
		    		ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
		    		childrenOutputStream.add(os);
	    		}catch (IOException e) {
	    			tmp.add(node);
	    			//System.out.println("Cannot connect to "+node.nodeID+" now, will try again");
				} 
	    	}
	    	childrenToConnect = tmp;
    	}
    }
    
    @Override
    public void run() {
    	logger.info("Spout started "+Thread.currentThread().getId());
    	//Connect to children
    	logger.info("begin to connect to children");
    	connectToChildren();
    	logger.info("children connected");
    	spoutOpen = true;
    	while(!Thread.currentThread().isInterrupted() && !isFinished) {
	    	//
    		if(spoutOpen) {
    			logger.info("start to read file");
    			BufferedReader bufferedReader;
    	    	int linenumber=0;
    	    	try {
    	    		bufferedReader = new BufferedReader(new FileReader(spoutFile));
    	    		//System.out.println("Read file "+spoutFile);
    				String line = bufferedReader.readLine();
    				//System.out.println("Line is:"+line);
    				while(line!=null && !Thread.currentThread().isInterrupted() && !isFinished) {
    					//System.out.println("Line is:"+line);
    					
    					if(line.isEmpty()) {
    						line = bufferedReader.readLine();
    						continue;
    					}
    					linenumber++;
    					HashMap<String,String> emit=new HashMap<String, String>();
    					emit.put(Integer.toString(linenumber), line);
    					sendTuple(emit);
    					
    					line = bufferedReader.readLine();
    			}
    				logger.info("################linenumber "+linenumber+"#########");
    				logger.info("####################### FILE END ############################");
				spoutOpen = false;
    				
    			} catch (IOException e) {
    				e.printStackTrace();
    			}
    		}
	    	 
    	}
    	logger.info("Spout ended "+Thread.currentThread().getId());
    }
    
    public void sendTuple(HashMap<String,String> tuple) {
    	if(children.size()>0) {
    		try {
    			//System.out.println("begin to send tuple");
    			childrenOutputStream.get(pointer).writeObject(tuple);
    			childrenOutputStream.get(pointer).flush();
	            //System.out.println("tuple sent "+tuple.values().toString());
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
    		
    		for(Socket socket:childrenSocket) {
	    		if(!socket.isClosed()) {
	    			socket.close();
	    		}
	    		childrenSocket.remove(socket);
	    	}
    		
	    	for(ObjectOutputStream os:childrenOutputStream) {
	    		try {
	    			os.close();
	    		}catch (IOException e) {
	    			e.printStackTrace();
	    			//continue;
	    		} 	
	    		childrenOutputStream.remove(os);
	    	}
	    	spoutOpen = false;
        	isFinished = true;
    	}catch (IOException e) {
			e.printStackTrace();
		} 
    	
    }

}
