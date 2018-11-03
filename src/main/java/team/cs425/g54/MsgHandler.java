package team.cs425.g54;

//import main.java.team.cs425.g54.Node;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;



/*
MessageHandler is used to handle all different types of message includes ping,join,leave,stop
*/

public class MsgHandler extends Thread{
    Node serverNode;
    DatagramSocket server;
    DatagramPacket receivedPacket;
    CopyOnWriteArrayList<Node> memberList;
    CopyOnWriteArrayList<Node> totalMemberList;
    boolean isIntroducer;
    int memberListSize = 3;
    int cnt = 0;
    static Logger logger = Logger.getLogger("main.java.team.cs425.g54.MessageHandler");
    public MsgHandler(Node node, DatagramSocket server, DatagramPacket receivedPacket,boolean isIntroducer,CopyOnWriteArrayList<Node> totalMemberList,CopyOnWriteArrayList<Node> memberList){
        this.serverNode = new Node(node.nodeID,node.nodeAddr,node.nodePort);
        this.server = server;
        this.receivedPacket = receivedPacket;
        this.isIntroducer = isIntroducer;
        this.totalMemberList = totalMemberList;
        this.memberList = memberList;
        // this.cnt = cnt;
    }
    public int containsInstance(CopyOnWriteArrayList<Node> list, Node node) {
        for (int i=0;i<list.size();i++) {
            if (compareNode(node,list.get(i))) {
                return i;
            }
        }
        return -1;
    }
    public boolean compareNode(Node n1,Node n2){
        if (n1.nodeID==n2.nodeID && n1.nodeAddr.equals(n2.nodeAddr) && n1.nodePort==n2.nodePort)
            return true;
        else
            return false;
    }
    
    public JSONArray packTotalList() {
    	JSONArray totalListJson = new JSONArray();
    	try {
            for (Node member : totalMemberList) {
                JSONObject m = new JSONObject();
                m.put("type", "join");
                m.put("nodeID", member.nodeID);
                m.put("nodeAddr", member.nodeAddr);
                m.put("nodePort", member.nodePort);
                totalListJson.put(m);
            }
        
        } catch (JSONException e){
            e.printStackTrace();
        }
    	return totalListJson;
    }
    
    public JSONObject packJoinMsg(Node node, JSONArray totalListJson) {
    	JSONObject message = new JSONObject();
    	try {
    		message.put("type", "join");
            message.put("totalList",totalListJson);
            message.put("nodeID", node.nodeID);
            message.put("nodeAddr", node.nodeAddr);
            message.put("nodePort", node.nodePort);
            // Pack master, for the nodes joining in
            if (Detector.master!=null) {
            	message.put("hasmaster",1);
            	message.put("masterID", Detector.master.nodeID);
            	message.put("masterAddr", Detector.master.nodeAddr);
            	message.put("masterPort", Detector.master.nodePort);
            }else {
            	message.put("hasmaster",0);
            }
            
    	}catch (JSONException e) {
    		e.printStackTrace();
    	}
    	return message;
    }
    public JSONObject packDeleteMsg(String type, Node node) {
    	JSONObject message = new JSONObject();
    	try {
	    	message.put("type", type);
	        message.put("nodeID", node.nodeID);
	        message.put("nodeAddr", node.nodeAddr);
	        message.put("nodePort", node.nodePort);
	        
	        //Detector
	        
	        
    	}catch (JSONException e) {
    		e.printStackTrace();
    	}
    	return message;
    }
    public JSONObject packNodeInfo(){
        JSONObject jsonNodeInfo = new JSONObject();
        try {
            CopyOnWriteArrayList<String> nodeFiles = Detector.storeInfo.getAllFiles();
            Hashtable<String,CopyOnWriteArrayList<String>> versions = Detector.storeInfo.getAllVersions();
            JSONArray fileList = new JSONArray();
            for(String file:nodeFiles){
                fileList.put(file);
                JSONArray versionArr = new JSONArray();
                for(String version:versions.get(file)){
                    versionArr.put(version);
                }
                jsonNodeInfo.put(file,versionArr.toString());  // file -> versions list
            }
            jsonNodeInfo.put("fileList",fileList);  // file list on node
            jsonNodeInfo.put("type","toMaster");
            jsonNodeInfo.put("command","updateNodeInfo");
            jsonNodeInfo.put("nodeID", serverNode.nodeID);
            jsonNodeInfo.put("nodeAddr", serverNode.nodeAddr);
            jsonNodeInfo.put("nodePort", serverNode.nodePort);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonNodeInfo;
    }
    
    public boolean deleteMsgNeedToSend(Node failNode, Node detector) {
    	int nodeId = containsInstance(totalMemberList,failNode);
    	if(nodeId!=-1 || detector.nodeID != Detector.master.nodeID) {
    		return true;
    	}
    	else {		
    		return false;
    	}
    }

    String packNodesToJson(ArrayList<Node> nodeList){
        JSONObject obj = new JSONObject();
        JSONArray nodeListJson = new JSONArray();
        try {
            for (Node member : nodeList) {
                JSONObject m = new JSONObject();
                m.put("nodeID", member.nodeID);
                m.put("nodeAddr", member.nodeAddr);
                m.put("nodePort", member.nodePort);
                nodeListJson.put(m);
            }


        } catch (JSONException e){
            e.printStackTrace();
        }
        return nodeListJson.toString();
    }

    public void updateNodeInfo(JSONObject jsonData ){
        try {
            Node node = new Node();
            node.nodeID = Integer.parseInt(jsonData.get("nodeID").toString());
            node.nodeAddr = jsonData.get("nodeAddr").toString();
            node.nodePort = Integer.parseInt(jsonData.get("nodePort").toString());
            JSONArray jsonfiles = (JSONArray) jsonData.get("fileList");
            for(int i=0;i<jsonfiles.length();i++){
                String file = jsonfiles.getString(i);
                Detector.masterInfo.addNodeFile(node,file);
                JSONArray jsonVersions = (JSONArray) jsonData.get(file);
                for(int j=0;j<jsonVersions.length();j++){
                    Detector.masterInfo.updateFileVersion(file,jsonVersions.getString(i));
                }
            }
            if(Detector.masterInfo.getNodeFilesSize()==Detector.groupList.size()){

            }

        } catch (JSONException e) {
            e.printStackTrace();
        }

    }

    public void sendReReplicaRequest(){
        // check all file, see if replicas is enough
        try {
            ArrayList<String> files =  Detector.masterInfo.getAllFiles();
            for(String file:files){
                ArrayList<Node> replicas = Detector.masterInfo.hasFileNodes(file);
                ArrayList<Node> needReplicas = Detector.masterInfo.getrereplicaList(file);
                if(needReplicas.size()==0)
                    continue;
                Node replicaNode = replicas.get(0);
                JSONArray jsonArray = new JSONArray();
                JSONObject jsonMsg = new JSONObject();
                jsonMsg.put("type","reReplica");
                for(Node putReplica:needReplicas){
                    JSONObject obj = new JSONObject();
                    obj.put("nodeID",putReplica.nodeID);  // node that need to add replica
                    obj.put("nodeAddr",putReplica.nodeAddr);
                    obj.put("nodePort",putReplica.nodePort);
                    obj.put("sdfsName",file);
                    jsonArray.put(obj);
                }
                // send rereplica request can ask one or ask all
                jsonMsg.put("NodeArray",jsonArray);
                InetAddress address = InetAddress.getByName(replicaNode.nodeAddr);
                logger.info("Introducer send join to all bytes: "+jsonArray.toString().getBytes().length);
                DatagramPacket send_message = new DatagramPacket(jsonArray.toString().getBytes(), jsonArray.toString().getBytes().length, address, replicaNode.nodePort);
                server.send(send_message);

            }
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // request nodes that have the replica to put replica on new machine
    }

    public void dealReReplicaRequest(JSONObject jsonData){
        try {
            JSONArray jsonArray = (JSONArray) jsonData.get("NodeArray");
            for(int i=0;i<jsonArray.length();i++){
                JSONObject jsonNode = jsonArray.getJSONObject(i);
                String sdfsName = jsonNode.getString("sdfsName");
                Socket socket = new Socket(jsonNode.getString("noderAddr"),jsonNode.getInt("noderPort"));
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                for(String version:Detector.storeInfo.fileVersions.get(sdfsName)){
                    // send put command msg
                    JSONObject obj = new JSONObject();
                    obj.put("sdfsName",sdfsName);
                    obj.put("timestamp",version);
                    obj.put("type","put");
                    dos.writeUTF(obj.toString());
                    // then send file
                    String name = sdfsName+"_"+version;
                    FileInputStream fis = new FileInputStream(name);
                    IOUtils.copy(fis,dos);
                    dos.flush();
                }
                socket.close();
            }
        } catch (JSONException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    // TODO: Change the input. Use other functions to pack the messages
    public void broadcast(String messageType, Node node){
        // introducer broadcast join message to all nodes
        try {
            DatagramPacket send_message;
            if (messageType.equals("join")) {
                if (isIntroducer) {
                    logger.info("broadcasting join from introducer...");
                    JSONArray totalListJson = packTotalList();
                    for (Node member : totalMemberList) {
                        if(compareNode(member,serverNode))
                            continue;
                        JSONObject message=packJoinMsg(member,totalListJson);
                        InetAddress address = InetAddress.getByName(member.nodeAddr);
                        logger.info("Introducer send join to all bytes: "+message.toString().getBytes().length);
                        send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, member.nodePort);
                        server.send(send_message);
                    }
                }
            } else if (messageType.equals("leave")) {
                logger.info("broadcasting leave from "+node.nodeID+" ...");
                
                
            	for (Node member : memberList) {
                	JSONObject message = packDeleteMsg("leave", node);
                	InetAddress address = InetAddress.getByName(member.nodeAddr);
                    send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, member.nodePort);
                    server.send(send_message);
                }
            
                
                
            } else if (messageType.equals("delete")) {
                logger.info("broadcasting delete from "+node.nodeID+" ...");
                for (Node member : memberList) {
                    JSONObject message = packDeleteMsg("delete", node);
                    InetAddress address = InetAddress.getByName(member.nodeAddr);
                    
                    send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, member.nodePort);
                    server.send(send_message);
                }
            
            }
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }
    // renew membership list
    public void renewMemberList(){
        logger.info("renew member list...");
        int index = containsInstance(totalMemberList,serverNode);
        int selfIndex = index;
        memberList.clear();
        for(int i=0;i<memberListSize;i++){
            index = (index+1) % totalMemberList.size();
            if(containsInstance(memberList,totalMemberList.get(index))>=0 || selfIndex==index)
                break;
            else
                memberList.add(totalMemberList.get(index));
        }
        // logger.info("Node " +serverNode.nodeID+ "finishing renew membership list");
        // showMembershipList();
    }

    public void renewTotalList(Node node){
        if(totalMemberList.size()==0)
            totalMemberList.add(node);
        else{
            int i = 0;
            for(;i<totalMemberList.size();i++){
                if(totalMemberList.get(i).nodeID>node.nodeID){
                    totalMemberList.add(i,node);
                    break;
                }
            }
            if(i==totalMemberList.size())
                totalMemberList.add(node);

        }
        // showGroupList();
    }
    public boolean compareAndRenewTotalList(CopyOnWriteArrayList<Node> newTotalList){
        logger.info("compareAndRenewTotalList..");
        if(newTotalList.size()!=totalMemberList.size()){
            totalMemberList.clear();
            for(Node m:newTotalList){
                totalMemberList.add(m);
            }

            return false; // different and renew
        }
        for(int i=0;i<newTotalList.size();i++){
            if(!compareNode(newTotalList.get(i),totalMemberList.get(i))){
                totalMemberList.clear();
                for(Node m:newTotalList){
                    totalMemberList.add(m);
                }
                return false; // different and renew
            }
        }
        // showMembershipList();
        // showGroupList();
        return true;
    }
    public void showMembershipList() {
        logger.info("Number of Members:"+memberList.size());
        for (int i=0;i<memberList.size();i++) {
            Node node=memberList.get(i);
            logger.info("Member"+(i+1)+" :");
            logger.info("Node ID:"+node.nodeID+", Node Address:"+node.nodeAddr+", Node Port:"+node.nodePort);
        }
    }
    
    public void showGroupList() {
        logger.info("Size of group:"+totalMemberList.size());
        for (int i=0;i<totalMemberList.size();i++) {
            Node node=totalMemberList.get(i);
            logger.info("Member"+(i+1)+" :");
            logger.info("Node ID:"+node.nodeID+", Node Address:"+node.nodeAddr+", Node Port:"+node.nodePort);
        }
    }

    public void run(){
        //logger.info("messageHandle start...");
        String receivedData = new String(receivedPacket.getData());
        //logger.info("receivedData: "+ receivedData);
        try{
            JSONObject jsonData = new JSONObject(receivedData);
        
            String messageType = jsonData.get("type").toString();

                    // get new node information
            //Node node = new Node(0,"",0);  // join need not use node but the whole list membership 
            //if(!messageType.equals("ping")){
            //    node.nodeID = Integer.parseInt(jsonData.get("nodeID").toString());
            //    node.nodeAddr = jsonData.get("nodeAddr").toString();
            //    node.nodePort = Integer.parseInt(jsonData.get("nodePort").toString());
           // }
            
            if(messageType.equals("ping")){
                //measure bytes
                // int num = receivedData.getBytes().length;
                // logger.info("Ping message bytes: "+num);
                logger.info("handling ping situation...");
                String id = String.valueOf(serverNode.nodeID);
                Random random = new Random();
//                double con = random.nextDouble();
//                logger.info("random number: "+ con);

                DatagramPacket send_ack = new DatagramPacket(id.getBytes(),id.getBytes().length,receivedPacket.getAddress(),receivedPacket.getPort());

                String tmp2 = new String(send_ack.getData());
                int num2 = tmp2.getBytes().length;

                // logger.info("Ping message bytes: "+num2);
                Listener.cnt++;
                
                logger.info("cnt for false positive: "+Listener.cnt);
                try {
                    server.send(send_ack);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                
            }
            else if(messageType.equals("join")){
            	Node node = new Node(0,"",0);
            	node.nodeID = Integer.parseInt(jsonData.get("nodeID").toString());
                node.nodeAddr = jsonData.get("nodeAddr").toString();
                node.nodePort = Integer.parseInt(jsonData.get("nodePort").toString());
                
                logger.info("Node "+ node.nodeID +" is joining...");
                if(!isIntroducer){
                    JSONArray arr = jsonData.getJSONArray("totalList");
                    CopyOnWriteArrayList<Node> newTotalList = new CopyOnWriteArrayList<>();
                    for(int i=0;i<arr.length();i++){
                        Node tmp_node = new Node(0,"",0);
                        tmp_node.nodeID = Integer.parseInt(arr.getJSONObject(i).get("nodeID").toString());
                        tmp_node.nodeAddr = arr.getJSONObject(i).get("nodeAddr").toString();
                        tmp_node.nodePort = Integer.parseInt(arr.getJSONObject(i).get("nodePort").toString());
                        // System.out.println("tmp_node.nodeID "+tmp_node.nodeAddr);
                        newTotalList.add(tmp_node);
                    }
                    

                    if(!compareAndRenewTotalList(newTotalList)){
                        renewMemberList();
                    }
                    
                    // Update master
                    if(Detector.master==null && jsonData.get("hasmaster").toString()=="1") {
                    	
                    	Node master=new Node();
                    	master.nodeID=Integer.parseInt(jsonData.get("masterID").toString());
                    	master.nodeAddr=jsonData.get("nodeAddr").toString();
                    	master.nodePort=Integer.parseInt(jsonData.get("masterPort").toString());
                    	
                    	Detector.master=master;
                    }
                    
                }
                else{
                    renewTotalList(node);
                    renewMemberList();
                    broadcast(messageType,node);
                }
                

            }
            else if(messageType.equals("leave")){

            	Node failNode = new Node(0,"",0);
            	failNode.nodeID = Integer.parseInt(jsonData.get("nodeID").toString());
                failNode.nodeAddr = jsonData.get("nodeAddr").toString();
                failNode.nodePort = Integer.parseInt(jsonData.get("nodePort").toString());
                
                //Node detector = new Node(0,"",0);
                //detector.nodeID = Integer.parseInt(jsonData.get("nodeID").toString());
                //detector.nodeAddr = jsonData.get("nodeAddr").toString();
                //detector.nodePort = Integer.parseInt(jsonData.get("nodePort").toString());
                
                logger.info("Node "+failNode.nodeID+" is leaving...");
                
            	int nodeIndex = containsInstance(totalMemberList,failNode);
                if(nodeIndex>=0){
                    totalMemberList.remove(nodeIndex);
                    renewMemberList();
                  //update master
                    if(failNode.nodeID == Detector.master.nodeID) {
                    	Detector.master=totalMemberList.get(0);
                    }
                    if(Detector.master.nodeID==serverNode.nodeID){ // check if it needs to send rereplica
                        Detector.masterInfo.deleteNodeAllFiles(failNode);
                        sendReReplicaRequest();
                    }
                    broadcast(messageType,failNode);
                
                }
                

            }
            else if(messageType.equals("delete")){
            	Node node = new Node(0,"",0);
            	node.nodeID = Integer.parseInt(jsonData.get("nodeID").toString());
                node.nodeAddr = jsonData.get("nodeAddr").toString();
                node.nodePort = Integer.parseInt(jsonData.get("nodePort").toString());
                
                logger.info("Node "+node.nodeID+" is being deleted..."); 
                
                int nodeIndex = containsInstance(totalMemberList,node);
                if(nodeIndex>=0){
                    totalMemberList.remove(nodeIndex);
                    renewMemberList();
                    if(node.nodeID == Detector.master.nodeID) {
                        Detector.master=totalMemberList.get(0);
                    }
                    if(Detector.master.nodeID==serverNode.nodeID){ // check if it needs to send rereplica
                        Detector.masterInfo.deleteNodeAllFiles(node);
                        sendReReplicaRequest();
                    }
                    broadcast(messageType,node);
                }

            }
            else if(messageType.equals("master")) {
            	Node node = new Node(0,"",0);
            	node.nodeID = Integer.parseInt(jsonData.get("nodeID").toString());
                node.nodeAddr = jsonData.get("nodeAddr").toString();
                node.nodePort = Integer.parseInt(jsonData.get("nodePort").toString());
                
            	logger.info("Node "+node.nodeID+" is set as master");
            	Detector.master=node;
            }
            else if(messageType.equals("toMaster")){   // master receiving msg
                String command = jsonData.get("command").toString();
                if(command.equals("get") || command.equals("delete") || command.equals("get_version") ){
                    String sdfsFile = jsonData.get("sdfsName").toString();
                    ArrayList<Node> nodes = Detector.masterInfo.getNodeToGetFile(sdfsFile);
                    String msg = packNodesToJson(nodes);
                    DatagramPacket send_msg = new DatagramPacket(msg.getBytes(),msg.getBytes().length,receivedPacket.getAddress(),receivedPacket.getPort());
                    server.send(send_msg);
                }
                else if(command.equals("put")){
                    String sdfsFile = jsonData.get("sdfsName").toString();
                    Node node = new Node();
                    node.nodeID = Integer.parseInt(jsonData.get("nodeID").toString());
                    node.nodeAddr = jsonData.get("nodeAddr").toString();
                    node.nodePort = Integer.parseInt(jsonData.get("nodePort").toString());
                    ArrayList<Node> nodes = Detector.masterInfo.getListToPut(node);
                    String msg = packNodesToJson(nodes);
                    DatagramPacket send_msg = new DatagramPacket(msg.getBytes(),msg.getBytes().length,receivedPacket.getAddress(),receivedPacket.getPort());
                    server.send(send_msg);
                }
                else if(command.equals("ls")){
                    String sdfsFile = jsonData.get("sdfsName").toString();
                    ArrayList<Node> nodes = Detector.masterInfo.getNodesForLs(sdfsFile);
                    String msg = packNodesToJson(nodes);
                    DatagramPacket send_msg = new DatagramPacket(msg.getBytes(),msg.getBytes().length,receivedPacket.getAddress(),receivedPacket.getPort());
                    server.send(send_msg);
                }
                else if(command.equals("updateNodeInfo")){
                    updateNodeInfo(jsonData);
                }
            }
            else if(messageType.equals("requset")){ // send node info to new master
                String msg = packNodeInfo().toString();
                DatagramPacket send_msg = new DatagramPacket(msg.getBytes(),msg.getBytes().length,receivedPacket.getAddress(),receivedPacket.getPort());
                server.send(send_msg);
            }
            else if(messageType.equals("reReplica")){

            }



        }catch (JSONException e){
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
