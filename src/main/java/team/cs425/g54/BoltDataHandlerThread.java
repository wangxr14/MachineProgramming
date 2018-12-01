package team.cs425.g54;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class BoltDataHandlerThread extends Thread {
	String appType;
	
	CopyOnWriteArrayList<Node> children;
	CopyOnWriteArrayList<ObjectOutputStream> childrenOutputStream;
	Socket socket;
	int threadID;
	
    int pointer;
    int port;
    
    String info="";
    private final int BYTE_LEN=10000;
    final int TIMEOUT = 5000;
    boolean stopped_sign = false;
    
    // File for working
    String workingFilepath;
 // For upload File to sdfs
    FileUploader uploader;
    
    // For word count
    public static ConcurrentHashMap<String,Integer> wordCounter=new ConcurrentHashMap<String, Integer>();
    
    int sendCount=0;
    
    public BoltDataHandlerThread(String appType, CopyOnWriteArrayList<Node> children, CopyOnWriteArrayList<ObjectOutputStream> childrenOutputStream, Socket inputSocket, int threadID) {
    	this.appType = appType;
    	this.children = children;
    	this.childrenOutputStream = childrenOutputStream;
    	this.socket = inputSocket;
    	this.threadID = threadID;
    	pointer=0;
    	workingFilepath = "files/tmpBolt"+this.threadID;
    	
    }
    
   
    @Override
    public void run() {
    	
    	// Delete the previous working file
    	File tmpFile = new File(workingFilepath);
    	tmpFile.delete();
    	uploader=new FileUploader(appType,workingFilepath);
    	uploader.setWordCounter(wordCounter);
    	uploader.start();
    	try {
	    	ObjectInputStream is = new ObjectInputStream(socket.getInputStream());
	    	int count=0;
	    	// Begin read data
	    	while(!Thread.currentThread().isInterrupted() && !stopped_sign) {
	    		HashMap<String,String> in = (HashMap<String,String>) is.readObject();
	          // Deal
	            dealWithData(in);
	            uploader.setFileChanged();
	            count++;
	            if(count%100==0) {
	            	System.out.println("Data received: "+count);
	            	System.out.println("Data sent: "+sendCount);
	            }
	            //if(count%1000==0 && appType.equals("wordCount")) {
	            //	wordcount_writeToLocalFile();
	            //}
	    	}
	    	is.close();
    	}catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    }
    
    public void wordcount_writeToLocalFile() {
    	BufferedWriter bufferedWriter;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(workingFilepath));
			for (Entry<String, Integer> entry : wordCounter.entrySet()) {
				bufferedWriter.write(entry.getKey()+" "+entry.getValue()+"\n");
				bufferedWriter.flush();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    public void dealWithData(HashMap<String,String> inData){
		//System.out.println("Data received: "+inData.values().toString());
		HashMap<String,String> outData = new HashMap<String,String>();
		//System.out.println("Apptype is "+appType+" "+appType.equals("filter"));
		if(appType.equals("filter")) {
			if(children.size()==0) {
				//System.out.println("Write to file");
				BufferedWriter bufferedWriter;
				try {
					bufferedWriter = new BufferedWriter(new FileWriter(workingFilepath, true));
					for (Entry<String, String> entry : inData.entrySet()) {
						bufferedWriter.write(entry.getValue()+"\n");
						bufferedWriter.flush();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else {
				//System.out.println("Send to children");
				for (Entry<String, String> entry : inData.entrySet()) {
					for(String s: entry.getValue().split(" ")) {
						if(s.equals(info)) {
							outData.put(entry.getKey(), entry.getValue());
				            sendTuple(outData);
				            outData=new HashMap<String, String>();
							break;
						}
					}
				}
				
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
				//System.out.println("Write to file");
				
			}
			else {
				//System.out.println("Send to children");
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
    			childrenOutputStream.get(pointer).writeObject(tuple);
    			childrenOutputStream.get(pointer).flush();
    		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		pointer = (pointer + 1) % children.size();
    		
    		// For debug
    		sendCount++;
    	}
    }
    
    public void stopThread() {
		stopped_sign = true;
	}
}
