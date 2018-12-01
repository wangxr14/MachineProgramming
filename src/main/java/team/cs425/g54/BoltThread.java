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

    public BoltThread(String appType,CopyOnWriteArrayList<Node> children){
        this.appType = appType;
        this.children = children;
        port=Detector.workerPort;
        
        childrenSocket = new CopyOnWriteArrayList<Socket>();
        childrenOutputStream = new CopyOnWriteArrayList<ObjectOutputStream>();
        dataHandlerThreads = new CopyOnWriteArrayList<BoltDataHandlerThread>
    }
    
    public void connectToChildren() {
    	ArrayList<Node> childrenToConnect = new ArrayList<Node>();
    	for(Node node:children) {
    		childrenToConnect.add(node);
    	}
    	while(childrenToConnect.size()>0) {
    		ArrayList<Node> tmp = new ArrayList<Node>();
	    	for(Node node:childrenToConnect) {
	    		try {
	    			System.out.println("connect "+node.nodeID);
		    		Socket socket = new Socket(node.nodeAddr, port);
		    		childrenSocket.add(socket);
		    		ObjectOutputStream os = new ObjectOutputStream(socket.getOutputStream());
		    		childrenOutputStream.add(os);
	    		}catch (IOException e) {
	    			tmp.add(node);
					e.printStackTrace();
				} 
	    	}
	    	childrenToConnect = tmp;
    	}
    	System.out.println("children connected done");
    }
    
    @Override
    public void run() {
    	System.out.println("Bolt started");
        
    	connectToChildren();
    	int count=0;
        while(!Thread.currentThread().isInterrupted() && !stopped_sign) {
            try {
            	// Start listening
            	serverSocket=new ServerSocket(port);
            	Socket socket = serverSocket.accept();
            	BoltDataHandlerThread dataHandler = new BoltDataHandlerThread(appType, children, childrenOutputStream, socket, count);
            	dataHandlerThreads.add(dataHandler);
            	dataHandler.start();   
                
            	count++;
            } catch (IOException e) {
                e.printStackTrace();
            } 
        }
    }
	
	
	public void stopThread() {
		stopped_sign = true;
		for(BoltDataHandlerThread thread:dataHandlerThreads) {
			thread.stopThread();
		}
	}
}
