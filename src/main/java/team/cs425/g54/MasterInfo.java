package team.cs425.g54;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;
import java.util.Hashtable;


public class MasterInfo {
    Logger logger = Logger.getLogger("main.java.team.cs425.g54.Detector");
    Hashtable<Node, CopyOnWriteArrayList<String>> nodeFiles;
    Hashtable<String,CopyOnWriteArrayList<Pair<Integer,String>>> fileVersions;
    final int max_versions = 20;
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
        if(fileVersions.containsKey(file))
            fileVersions.remove(file);

    }
    public void deleteNodeAllFiles(Node node){
    	logger.info("Now begins");
        CopyOnWriteArrayList<String> files = nodeFiles.get(node);
        logger.info("contains node? "+nodeFiles.containsKey(node));
        if(nodeFiles.containsKey(node)){
            nodeFiles.remove(node);
            logger.info("remove a node to master succeed");
        }
        if(files==null) {
        	return;
        }
        for(String file:files){
            int flag = 0;
            for(Pair<Integer,String> p:fileVersions.get(file)){
                if(p.getKey()==node.nodeID){
                    fileVersions.get(file).remove(p);
                }
            }
            if(fileVersions.get(file).size()==0){
                fileVersions.remove(file);
            }
        }

    }
    public void sendDeleteVersionMsg(Node node,String file, String timestamp){
        JSONObject obj = new JSONObject();
        try {
            obj.put("type","deleteVersion");
            obj.put("timestamp",timestamp);
            obj.put("sdfsName",file);
            String msg = obj.toString();
            DatagramSocket dp = new DatagramSocket();
            InetAddress address = InetAddress.getByName(node.nodeAddr);
            DatagramPacket send_message = new DatagramPacket(msg.getBytes(), msg.getBytes().length, address, node.nodePort);
            dp.send(send_message);

        } catch (JSONException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void updateFileVersion(Node node,String file,String timestamp){
        if(fileVersions.containsKey(file)){
            if(fileVersions.get(file).size()==max_versions){
                int num = replicas;
                while(num>0){
                    Node tmp = new Node();
                    tmp.nodeID = fileVersions.get(file).get(0).getKey();
                    tmp.nodeAddr = Detector.nodeAddrPortList.get(tmp.nodeID).getKey();
                    tmp.nodePort  = Detector.nodeAddrPortList.get(tmp.nodeID).getValue();
                    sendDeleteVersionMsg(tmp,file,fileVersions.get(file).get(0).getValue());
                    fileVersions.get(file).remove(0);
                    num--;
                }
            }
            Pair<Integer,String> p = new ImmutablePair<>(node.nodeID,timestamp);
            fileVersions.get(file).add(p);
            logger.info("update file version from master succeed");
        }
        else{
            CopyOnWriteArrayList<Pair<Integer,String>> arr = new CopyOnWriteArrayList<>();
            Pair<Integer,String> p = new ImmutablePair<>(node.nodeID,timestamp);
            arr.add(p);
            fileVersions.put(file,arr);
            logger.info("update file version from master succeed");
        }
    }
    // get all the nodes that have the file
    public ArrayList<Node> hasFileNodes(String file){
        logger.info("begin hasFileNode");
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
        logger.info("Begin get reReplicalist");
        ArrayList<Node> curNodes = hasFileNodes(file);
        ArrayList<Node> needRereplica = new ArrayList<>();
        // need to get more replicas;
        logger.info("cur num of node that have file " + file +": "+curNodes.size());
        if(curNodes.size()<replicas){
            int needs = replicas - curNodes.size();
            needRereplica = reReplicaList(curNodes,needs);
        }
        return needRereplica;
    }
    boolean inRelist(ArrayList<Node> cur,int id){
        for(Node node:cur){
            if(node.nodeID==id)
                return true;
        }
        return false;
    }
    // get nodes that needs for more replicas
    public ArrayList<Node> reReplicaList(ArrayList<Node> cur, int num){
        logger.info("Begin get reReplicalist");
        logger.info("num of need replica"+num);
        ArrayList<Node> result = new ArrayList<>();
        int maxx_id = 0;
        Node max_node = new Node();
        for(Node node:cur){
            if(node.nodeID>maxx_id){
                max_node = node;
                maxx_id = node.nodeID;
            }
        }
        int origin = Detector.findNodeInGroupList(max_node);
        int id = (origin+1) % Detector.groupList.size();
        logger.info("origin id"+origin + "initial id"+ id);
        while(num>0){
            logger.info("inRelist(cur,id) "+inRelist(cur,id) + "cur "+ id);
            if(inRelist(cur,Detector.groupList.get(id).nodeID)) {
                id=(id+1) % Detector.groupList.size();
                continue;
            }
            result.add(Detector.groupList.get(id));
            logger.info("need put replica "+Detector.groupList.get(id).nodeID);
            id = (id+1) % Detector.groupList.size();
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
        if(fileVersions.containsKey(file)){
            int index = fileVersions.get(file).size()-1;
            Node node = new Node();
            node.nodeID = fileVersions.get(file).get(index).getKey();
            node.nodeAddr = Detector.nodeAddrPortList.get(node.nodeID).getKey();
            node.nodePort  = Detector.nodeAddrPortList.get(node.nodeID).getValue();
            nodeList.add(node);
            return nodeList;
        }
        logger.info("no node for file to get...");
        return nodeList;
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
    public ArrayList<Pair<Node,String>> getKVersionsNode(String sdfsName,int k){
        ArrayList<Pair<Node,String>> nodeList = new ArrayList<>();
        if(fileVersions.containsKey(sdfsName)) {
	        for(int i=fileVersions.get(sdfsName).size()-1;i>=0;i--){
	            if(k==0)
	                break;
	            if(i<fileVersions.get(sdfsName).size()-1 && fileVersions.get(sdfsName).get(i).getValue().equals(fileVersions.get(sdfsName).get(i+1).getValue()))
	                continue;
	            Node node = new Node();
	            node.nodeID = fileVersions.get(sdfsName).get(i).getKey();
	            node.nodeAddr = Detector.nodeAddrPortList.get(node.nodeID).getKey();
	            node.nodePort  = Detector.nodeAddrPortList.get(node.nodeID).getValue();
	            Pair<Node,String> p = new ImmutablePair<>(node,fileVersions.get(sdfsName).get(i).getValue());
	            nodeList.add(p);
	            k--;
	        }
        }
        return nodeList;
    }
    public void printVersions(){
        logger.info("print all versions");
        for(String file:fileVersions.keySet()){
            System.out.println("File " +file);
            for(Pair<Integer,String> p:fileVersions.get(file)){
                System.out.println("Node "+p.getKey()+" "+p.getValue());
            }
            System.out.println();
        }
    }
}
