package team.cs425.g54;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
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
    
    private final int BYTE_LEN=10000;
    
    public SpoutThread(String spoutFile,String appType,CopyOnWriteArrayList<Node> children){
        this.spoutFile = spoutFile;
        this.appType = appType;
        this.children = children;
        pointer=0;
        port=Detector.workerPort;
        
    }
    
    @Override
    public void run() {
    	System.out.println("Spout started");
    	while(!Thread.currentThread().isInterrupted() && !isFinished) {
	    	//
	    	BufferedReader bufferedReader;
	    	int linenumber=0;
	    	try {
	    		bufferedReader = new BufferedReader(new FileReader(spoutFile));
	    		System.out.println("Read file "+spoutFile);
				String line = bufferedReader.readLine();
				System.out.println("Line is:"+line);
				while(line!=null) {
					System.out.println("Line is:"+line);
					linenumber++;
					HashMap<String,String> emit=new HashMap<String, String>();
					emit.put("linenumber", Integer.toString(linenumber));
					emit.put("line", line);
					sendTuple(emit);
					line = bufferedReader.readLine();
				}
				
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
	    		// Tcp connect children[pointer]
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
	            System.out.println("tuple sent "+tuple.values().toString());
    		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		pointer = (pointer + 1) % children.size();
    	}
    }

}
