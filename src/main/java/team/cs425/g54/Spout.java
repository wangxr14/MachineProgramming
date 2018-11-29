package main.java.team.cs425.g54;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class Spout {
    public String spoutFile;
    public String appType;
    public CopyOnWriteArrayList<Node> children;
    int pointer;
    int port;
    
    private final int BYTE_LEN=10000;
    
    public Spout(String spoutFile,String appType,CopyOnWriteArrayList<Node> children){
        this.spoutFile = spoutFile;
        this.appType = appType;
        this.children = children;
        pointer=0;
        port=Detector.workerPort;
    }
    
    public void open() {
    	//
    	BufferedReader bufferedReader = new BufferedReader(new FileReader(spoutFile));
    	int linenumber=0;
    	try {
			String line = bufferedReader.readLine();
			if(line!=null){
				linenumber++;
				HashMap<String,String> emit=new HashMap<String, String>();
				emit.put("linenumber", Integer.toString(linenumber));
				emit.put("line", line);
				sendTuple(emit);
			}
			else{
				System.err.println("####################### FILE END ############################");
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch(InterruptedException e){
			e.printStackTrace();
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
    		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		pointer = (pointer + 1) % children.size();
    	}
    }

}
