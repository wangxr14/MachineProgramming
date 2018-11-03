package team.cs425.g54;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;
import java.util.Hashtable;


public class MasterInfo {
    Logger logger = Logger.getLogger("main.java.team.cs425.g54.Detector");
    private Hashtable<Node, CopyOnWriteArrayList<String>> nodeFiles;
    private Hashtable<String,CopyOnWriteArrayList<String>> fileVersions;
    final int max_versions = 5;
    void addNodeFile(Node node,String file){
        if(nodeFiles.containsKey(node)){
            if(!nodeFiles.get(node).contains(file)){
                nodeFiles.get(node).add(file);
                logger.info("add a file from list of master succeed");
            }
        }
    }
    void deleteNodeFile(Node node,String file){
        if(nodeFiles.containsKey(node)){
            if(nodeFiles.get(node).contains(file)){
                nodeFiles.get(node).remove(file);
                logger.info("remove a file from list of master succeed");
            }
        }
    }
    void deleteNodeAllFiles(Node node){
        if(nodeFiles.containsKey(node)){
            nodeFiles.remove(node);
            logger.info("remove a node to master succeed");
        }
    }
    void updateFileVersion(String file,String timestamp){
        if(fileVersions.contains(file)){
            if(fileVersions.get(file).size()>=max_versions){
                fileVersions.get(file).remove(0);
                fileVersions.get(file).add(timestamp);
                logger.info("update file version from master succeed");
            }
        }
    }
    // for put
    ArrayList<Node> getListToPut(Node node){ // request from node, type for command
         ArrayList<Node> nodeList = new ArrayList<>();
         int len = Detector.groupList.size();
         int origin = Detector.groupList.indexOf(node);
         int index = (origin+1)%len;
         while(index != origin){
             nodeList.add(Detector.groupList.get(index));
             index = (index+1)%len;
         }
         return nodeList;

    }
    // get,get version and delete
    ArrayList<Node> getNodeToGetFile(String file){
        ArrayList<Node> nodeList = new ArrayList<>();
        for(Map.Entry<Node, CopyOnWriteArrayList<String>> entry : nodeFiles.entrySet()){
            Node res = entry.getKey();
            if(nodeFiles.get(res).contains(file)){
                nodeList.add(res);
                return nodeList;
            }
        }
        logger.info("no node for file to get...");
        return null;
    }
}
