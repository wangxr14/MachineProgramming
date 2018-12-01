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
    
    private DatagramSocket socket;
    String info="";
    private final int BYTE_LEN=10000;
    final int TIMEOUT = 5000;
    boolean stopped_sign = false;
    
    // File for working
    String workingFilepath = "files/tmpBolt";
    // For word count
    HashMap<String,Integer> wordCounter=new HashMap<String, Integer>();
    
    // For upload File to sdfs
    FileUploader uploader;

    public BoltThread(String appType,CopyOnWriteArrayList<Node> children, DatagramSocket workerSocket){
        this.appType = appType;
        this.children = children;
        pointer=0;
        socket=workerSocket;
        
    }
    
    
    @Override
    public void run() {
    	System.out.println("Bolt started");
    	// Delete the previous working file
    	File tmpFile = new File(workingFilepath);
    	tmpFile.delete();
    	uploader=new FileUploader(appType,workingFilepath);
    	if(children.size()==0) {
        	uploader.start();
    	}
    	
    	
    	// Start listening
        byte [] receiveData=new byte[BYTE_LEN];
        while(!Thread.currentThread().isInterrupted() && !stopped_sign) {
            try {
                DatagramPacket pack=new DatagramPacket(receiveData,receiveData.length);
                socket.receive(pack);
                ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(receiveData));
                HashMap<String,String> in = (HashMap<String,String>) is.readObject();
                is.close();

              // Deal
                dealWithData(in);
                uploader.setFileChanged();
                
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
	
	public void dealWithData(HashMap<String,String> inData){
		System.out.println("Data received: "+inData.values().toString());
		HashMap<String,String> outData = new HashMap<String,String>();
		System.out.println("Apptype is "+appType+" "+appType.equals("filter"));
		if(appType.equals("filter")) {
			if(children.size()==0) {
				System.out.println("Write to file");
				BufferedWriter bufferedWriter;
				try {
					bufferedWriter = new BufferedWriter(new FileWriter(workingFilepath, true));
					for (Entry<String, String> entry : inData.entrySet()) {
						System.out.println("entry "+entry.getValue()+"\n");
						bufferedWriter.write(entry.getValue());
						bufferedWriter.flush();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else {
				System.out.println("Send to children");
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
				System.out.println("Write to file");
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
	
	
	public void stopThread() {
		stopped_sign = true;
		Thread.currentThread().interrupt();
	}
}
