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
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class BoltThread extends Thread {
    public String appType;
    // Output
    public CopyOnWriteArrayList<Node> children;
    public CopyOnWriteArrayList<Socket> childrenSocket;
    public CopyOnWriteArrayList<ObjectOutputStream> childrenOutputStream;

    // Input thread
    public CopyOnWriteArrayList<BoltDataHandlerThread> dataHandlerThreads;
    
    private ServerSocket serverSocket;
    
    int port;
    
    String info="";
    private final int BYTE_LEN=10000;
    final int TIMEOUT = 5000;
    boolean stopped_sign = false;
    
    // File for working
    String workingFilepath = "files/tmpBolt";
    // For word count
    public static ConcurrentHashMap<String,Integer> wordCounter=new ConcurrentHashMap<String, Integer>();
    
    // For upload File to sdfs
    FileUploader uploader;
    
    int sendCount=0;
    
    public static AtomicBoolean allThreadStop;
    
    static Logger logger = Logger.getLogger("main.java.team.cs425.g54.BoltThread");
    
    public BoltThread(String appType,CopyOnWriteArrayList<Node> children){
        this.appType = appType;
        this.children = children;
        port=Detector.workerPort;
        
        childrenSocket = new CopyOnWriteArrayList<Socket>();
        childrenOutputStream = new CopyOnWriteArrayList<ObjectOutputStream>();
        dataHandlerThreads = new CopyOnWriteArrayList<BoltDataHandlerThread>();
        
        allThreadStop = new AtomicBoolean(false);
        
        wordCounter=new ConcurrentHashMap<String, Integer>();
    }
    
    public void printChildren() {
    	for(Node node:children) {
    		System.out.println(node.nodeID);
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
	    			//System.out.println("connect "+node.nodeID);
		    		Socket socket = new Socket(node.nodeAddr, port);
		    		childrenSocket.add(socket);
		    		ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
		    		childrenOutputStream.add(os);
	    		}catch (IOException e) {
	    			tmp.add(node);
					//e.printStackTrace();
	    			//System.out.println("Cannot connect to "+node.nodeID+" now, will try again");
	    			
				} 
	    	}
	    	childrenToConnect = tmp;
    	}
    	System.out.println("children connected done");
    }
    
    @Override
    public void run() {
    	System.out.println("Bolt started "+Thread.currentThread().getId());
    	System.out.println("begin to connect to children");
    	connectToChildren();
    	System.out.println("children connected");
    	int count=0;
    	try {
    	// Start listening
    		System.out.println("Bolt start listening");
	    	serverSocket=new ServerSocket(port);
	        while(!Thread.currentThread().isInterrupted() && !stopped_sign) {	
            	Socket socket = serverSocket.accept();
            	BoltDataHandlerThread dataHandler = new BoltDataHandlerThread(appType, children, childrenOutputStream, socket, count, allThreadStop);
            	dataHandlerThreads.add(dataHandler);
            	dataHandler.start();   
                
            	count++;
	            
	        }
	        serverSocket.close();
    	} catch (IOException e) {
            e.printStackTrace();
        } 
    	
    	System.out.println("Bolt ended "+Thread.currentThread().getId());
    }
	
	
	public void stopThread() {
		try {
			//serverSocket.close();
			logger.info("started to stop handlers");
			for(BoltDataHandlerThread thread:dataHandlerThreads) {
				while(thread.isAlive()) {
					thread.stopThread();
					sleep(500);
				}
				
			}
			logger.info("set stop sign");
			allThreadStop.set(true);
			stopped_sign = true;
			
			logger.info("start to close sockets");
			for(Socket socket:childrenSocket) {
	    		if(!socket.isClosed()) {
	    			socket.close();
	    		}
	    		childrenSocket.remove(socket);
	    	}
			logger.info("start to close outputstream");
			for(ObjectOutputStream os:childrenOutputStream) {
				try {
					os.close();
				}catch (IOException e) {
		            e.printStackTrace();
		            //continue;
		        } 
	    		
	    		childrenOutputStream.remove(os);
	    	}
	    	
	    	
			
		} catch(InterruptedException e) {
        	e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } 
		
	}
}
