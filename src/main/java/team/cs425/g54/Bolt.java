package main.java.team.cs425.g54;

import java.util.concurrent.CopyOnWriteArrayList;

public class Bolt {
    public String appType;
    public CopyOnWriteArrayList<Node> children;
    int pointer;
    int port;

    public Bolt(String appType,CopyOnWriteArrayList<Node> children){
        this.appType = appType;
        this.children = children;
        pointer=0;
        port=Detector.workerPort;
    }
    
    public void open() {
    	if(appType == "filter") {
    		
    	}
    }
}
