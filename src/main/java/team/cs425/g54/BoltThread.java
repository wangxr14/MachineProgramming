package team.cs425.g54;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
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

    public BoltThread(String appType,CopyOnWriteArrayList<Node> children){
        this.appType = appType;
        this.children = children;
        pointer=0;
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
        byte [] receiveData=new byte[BYTE_LEN];
        while(!Thread.currentThread().isInterrupted()) {
            try {
                DatagramPacket pack=new DatagramPacket(receiveData,receiveData.length);
                socket.receive(pack);
                ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(receiveData));
                HashMap<String,String> in = (HashMap<String,String>) is.readObject();
                is.close();

//              // Deal
                dealWithData(in);
                
                
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
		
		if(appType.equals("filter")) {
			if(children.size()==0) {
				System.out.println("Write to file");
				BufferedWriter bufferedWriter;
				try {
					bufferedWriter = new BufferedWriter(new FileWriter("sdfs_bolt", true));
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
					if(entry.getValue().equals(info)) {
						outData.put(entry.getKey(), entry.getValue());
					}
					
				}
				// Send
	            sendTuple(outData);
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
}
