package main.java.team.cs425.g54;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class WorkerTaskListener extends Thread{
	private DatagramSocket socket;
	public String appType;
    public CopyOnWriteArrayList<Node> children;
    int pointer;
    int port;
    private final int BYTE_LEN=10000;
    
	public WorkerTaskListener(String appType,CopyOnWriteArrayList<Node> children){
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
        byte [] receiveData=new byte[BYTE_LEN];
        while(true) {
            try {
//                System.err.println("[BOLT_TASK] Waiting for next packet "+taskID);
                DatagramPacket pack=new DatagramPacket(receiveData,receiveData.length);
                socket.receive(pack);
                ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(receiveData));
                HashMap<String,String> in = (HashMap<String,String>) is.readObject();
                is.close();

//              // Deal
                
                // Send
                
                
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
	
	public HashMap<String,String> dealWithData(HashMap<String,String> inData){
		
		
		HashMap<String,String> outData = new HashMap<String,String>();
		return outData;
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
