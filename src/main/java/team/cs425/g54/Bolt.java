package main.java.team.cs425.g54;

import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.CopyOnWriteArrayList;

public class Bolt {
    public String appType;
    public CopyOnWriteArrayList<Node> children;
    int pointer;
    int port;
    private DatagramSocket socket;
    WorkerTaskListener taskListener;

    public Bolt(String appType,CopyOnWriteArrayList<Node> children){
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
        taskListener = new WorkerTaskListener(appType, children);
    }
    
    public void open() {
    	if(appType == "filter") {
    		
    	}
    	if(appType == "combine") {
    		
    	}
    }
}
