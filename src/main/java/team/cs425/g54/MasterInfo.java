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
    private Hashtable<Node, CopyOnWriteArrayList<String>> nodeFiles;
    private Hashtable<String,CopyOnWriteArrayList<String>> fileVersions;
    final int max_versions = 5;
    final int replicas = 4;
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
    void updateFileVersion(String file,String timestamp){
        if(fileVersions.contains(file)){
            if(fileVersions.get(file).size()>=max_versions){
                fileVersions.get(file).remove(0);
                fileVersions.get(file).add(timestamp);
                logger.info("update file version from master succeed");
            }
        }
    }
    // get all the nodes that have the file
    ArrayList<Node> hasFileNodes(String file){
        ArrayList<Node> nodeList = new ArrayList<>();
        for(Map.Entry<Node, CopyOnWriteArrayList<String>> entry : nodeFiles.entrySet()){
            Node res = entry.getKey();
            if(nodeFiles.get(res).contains(file)){
                nodeList.add(res);
            }
        }
        return nodeList;
    }
    // return rereplicatList
    ArrayList<Node> getrereplicaList(String file){
        ArrayList<Node> curNodes = hasFileNodes(file);
        ArrayList<Node> needRereplica = new ArrayList<>();
        // need to get more replicas;
        if(curNodes.size()<replicas){
            int needs = replicas - curNodes.size();
            needRereplica = rereplicaList(curNodes,needs);
        }
        return needRereplica;
    }
    // get nodes that needs for more replicas
    ArrayList<Node> rereplicaList(ArrayList<Node> cur, int num){
        ArrayList<Node> result = new ArrayList<>();
        int maxx_id = 0;
        Node max_node = new Node();
        for(Node node:cur){
            if(node.nodeID>maxx_id){
                max_node = node;
                maxx_id = node.nodeID;
            }
        }
        int id = Detector.groupList.indexOf(max_node);
        while(num>0){
            id = (id+1) % Detector.groupList.size();
            result.add(Detector.groupList.get(id));
            num--;
        }
        return result;
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
    ArrayList<Node> getNodesForLs(String file){
        ArrayList<Node> nodeList = new ArrayList<>();
        for(Map.Entry<Node, CopyOnWriteArrayList<String>> entry : nodeFiles.entrySet()){
            Node res = entry.getKey();
            if(nodeFiles.get(res).contains(file)){
                nodeList.add(res);

            }
        }
        logger.info("get file for ls done");
        return nodeList;
    }
}
