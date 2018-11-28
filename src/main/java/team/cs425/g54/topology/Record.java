package team.cs425.g54.topology;


import team.cs425.g54.Node;

import java.util.ArrayList;

public class Record {
    int ID;
    String workerType; // bolt or spout
    String appType;
    String ipAddr;
    ArrayList<Node> children;// next bolt
    public Record(int ID,String ipAddr, String appType, String workerType, ArrayList<Node> children){
        this.ID = ID;
        this.ipAddr = ipAddr;
        this.workerType = workerType;
        this.appType = appType;
        this.children = new ArrayList<>();
        for(Node node:children){
            this.children.add(node);
        }

    }
    public int getID(){
        return ID;
    }
    public String getWorkerType(){
        return workerType;
    }
    public String getAppType(){
        return appType;
    }
    public String getIpAddr(){
        return ipAddr;
    }
    public ArrayList<Node> getChildren(){
        return (ArrayList<Node>) children.clone();
    }

}
