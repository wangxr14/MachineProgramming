package team.cs425.g54;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;
import java.util.Hashtable;


public class MasterInfo {
    Logger logger = Logger.getLogger("main.java.team.cs425.g54.Detector");
    Hashtable<Node, CopyOnWriteArrayList<String>> nodeFiles;
    Hashtable<String,CopyOnWriteArrayList<String>> fileVersions;
    final int max_versions = 5;
    final int replicas = 4;
    public MasterInfo(){
        nodeFiles = new Hashtable<>();
        fileVersions = new Hashtable<>();
    }



    public void addNodeFile(Node node,String file){
        if(nodeFiles.containsKey(node)){
            if(!nodeFiles.get(node).contains(file)){
                nodeFiles.get(node).add(file);
                logger.info("add a file from list of master succeed");
                logger.info("existed add to master: node "+node.nodeID+" file "+file);
            }
        }
        else{
            CopyOnWriteArrayList<String> arr = new CopyOnWriteArrayList<>();
            arr.add(file);
            nodeFiles.put(node,arr);
            logger.info("add to master: node "+node.nodeID+" file "+file);
            logger.info("add a file from list of master succeed");
            logger.info("res " +nodeFiles.containsKey(node)+nodeFiles.get(node).get(0));
        }
    }
    public void deleteNodeFile(Node node,String file){
        if(nodeFiles.containsKey(node)){
            if(nodeFiles.get(node).contains(file)){
                nodeFiles.get(node).remove(file);
                logger.info("remove a file from list of master succeed");
            }
        }
    }
    public void deleteNodeAllFiles(Node node){
        CopyOnWriteArrayList<String> files = nodeFiles.get(node);

        if(nodeFiles.containsKey(node)){
            nodeFiles.remove(node);
            logger.info("remove a node to master succeed");
        }
        for(String file:files){
            int flag = 0;
            for(Map.Entry<Node, CopyOnWriteArrayList<String>> entry : nodeFiles.entrySet()){
                if(entry.getValue().contains(file)){
                    flag = 1;
                    break;
                }
            }
            if(flag==0){
                fileVersions.remove(file);
            }
        }

    }
    public void updateFileVersion(String file,String timestamp){
        if(fileVersions.containsKey(file)){
            fileVersions.get(file).add(timestamp);
            while(fileVersions.get(file).size()>max_versions){
                fileVersions.get(file).remove(0);
            }
            logger.info("update file version from master succeed");
        }
        else{
            CopyOnWriteArrayList<String> arr = new CopyOnWriteArrayList<>();
            arr.add(timestamp);
            fileVersions.put(file,arr);
            logger.info("update file version from master succeed");
        }
    }
    // get all the nodes that have the file
    public ArrayList<Node> hasFileNodes(String file){
        ArrayList<Node> nodeList = new ArrayList<>();
        for(Node node : nodeFiles.keySet()){
            if(nodeFiles.get(node).contains(file)){
                nodeList.add(node);
            }
        }
        return nodeList;
    }
    // return rereplicatList
    public ArrayList<Node> getrereplicaList(String file){
        ArrayList<Node> curNodes = hasFileNodes(file);
        ArrayList<Node> needRereplica = new ArrayList<>();
        // need to get more replicas;
        if(curNodes.size()<replicas){
            int needs = replicas - curNodes.size();
            needRereplica = reReplicaList(curNodes,needs);
        }
        return needRereplica;
    }
    // get nodes that needs for more replicas
    public ArrayList<Node> reReplicaList(ArrayList<Node> cur, int num){
        ArrayList<Node> result = new ArrayList<>();
        int maxx_id = 0;
        Node max_node = new Node();
        for(Node node:cur){
            if(node.nodeID>maxx_id){
                max_node = node;
                maxx_id = node.nodeID;
            }
        }
        int id = Detector.findNodeInGroupList(max_node);
        while(num>0){
            id = (id+1) % Detector.groupList.size();
            result.add(Detector.groupList.get(id));
            num--;
        }
        return result;
    }


    // for put
    public ArrayList<Node> getListToPut(Node node){ // request from node, type for command
         ArrayList<Node> nodeList = new ArrayList<>();
         int len = Detector.groupList.size();

         int origin = Detector.findNodeInGroupList(node);

         int index = (origin+1)%len;
         int count=0;
         //System.out.println(len+" "+index);
         while(index != origin && count<replicas){
             nodeList.add(Detector.groupList.get(index));
             count++;
             index = (index+1)%len;
         }
         return nodeList;

    }

    // get,get version and delete
    public ArrayList<Node> getNodeToGetFile(String file){
        ArrayList<Node> nodeList = new ArrayList<>();
        if(nodeFiles==null)
            return nodeList;
        for(Node node : nodeFiles.keySet()){
            if(nodeFiles.get(node).contains(file)){
                nodeList.add(node);
                return nodeList;
            }
        }
        logger.info("no node for file to get...");
        return null;
    }
    public ArrayList<Node> getNodesForLs(String file){
        ArrayList<Node> nodeList = new ArrayList<>();

        for(Node node : nodeFiles.keySet()){
            if(nodeFiles.get(node).contains(file)){
                nodeList.add(node);
                logger.info("node "+node.nodeID);
            }
        }
        logger.info("get file for ls done");
        return nodeList;
    }

    public CopyOnWriteArrayList<String> getNodeFiles(Node node){
        CopyOnWriteArrayList<String> result =  new CopyOnWriteArrayList<>();
        if(nodeFiles.containsKey(node))
            return nodeFiles.get(node);
        return result;
    }
    public int getNodeFilesSize(){
        return nodeFiles.size();
    }
    public ArrayList<String> getAllFiles(){
        ArrayList<String> fileList = new ArrayList<>();
        if(fileVersions==null)
            return fileList;
        for(String file : fileVersions.keySet()){
            fileList.add(file);
        }
        return fileList;
    }
    public void printMasterNode(){
        logger.info("master store node");
        for(Node file : nodeFiles.keySet()){
            System.out.println("Node "+file.nodeID);
        }
    }

}
