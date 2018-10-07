package team.cs425.g54;

//import main.java.team.cs425.g54.Node;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONException;


import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.List;
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
    static Logger logger = Logger.getLogger("main.java.team.cs425.g54.MessageHandler");
    public MsgHandler(Node node, DatagramSocket server, DatagramPacket receivedPacket,boolean isIntroducer,CopyOnWriteArrayList<Node> totalMemberList,CopyOnWriteArrayList<Node> memberList){
        this.serverNode = new Node(node.nodeID,node.nodeAddr,node.nodePort);
        this.server = server;
        this.receivedPacket = receivedPacket;
        this.isIntroducer = isIntroducer;
        this.totalMemberList = totalMemberList;
        this.memberList = memberList;
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
    public void broadcast(String messageType,Node node ){
        // introducer broadcast join message to all nodes
        try {
            DatagramPacket send_message;
            if (messageType.equals("join")) {
                if (isIntroducer) {
                    logger.info("broadcasting join from introducer...");
                    JSONArray totalListJson = new JSONArray();
                    for (Node member : totalMemberList) {
                        JSONObject m = new JSONObject();
                        m.put("type", "join");
                        m.put("nodeID", member.nodeID);
                        m.put("nodeAddr", member.nodeAddr);
                        m.put("nodePort", member.nodePort);
                        totalListJson.put(m);
                    }
                    for (Node member : totalMemberList) {
                        if(compareNode(member,serverNode))
                            continue;
                        JSONObject message = new JSONObject();

                        message.put("type", "join");
                        message.put("totalList",totalListJson);

                        InetAddress address = InetAddress.getByName(member.nodeAddr);
                        send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, member.nodePort);
                        server.send(send_message);
                    }
                }
            } else if (messageType.equals("leave")) {
                logger.info("broadcasting leave from "+serverNode.nodeID+" ...");
                for (Node member : memberList) {
                    JSONObject message = new JSONObject();
                    message.put("type", "leave");
                    message.put("nodeID", node.nodeID);
                    message.put("nodeAddr", node.nodeAddr);
                    message.put("nodePort", node.nodePort);
                    InetAddress address = InetAddress.getByName(member.nodeAddr);
                    send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, member.nodePort);
                    server.send(send_message);
                }
            } else if (messageType.equals("delete")) {
                logger.info("broadcasting delete from "+serverNode.nodeID+" ...");
                for (Node member : memberList) {
                    JSONObject message = new JSONObject();
                    message.put("type", "delete");
                    message.put("nodeID", node.nodeID);
                    message.put("nodeAddr", node.nodeAddr);
                    message.put("nodePort", node.nodePort);
                    InetAddress address = InetAddress.getByName(member.nodeAddr);
                    send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, member.nodePort);
                    server.send(send_message);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e){
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
        showMembershipList();
        showGroupList();
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
            Node node = new Node(0,"",0);  // join need not use node but the whole list membership
            if((!jsonData.get("type").equals("join") && !jsonData.get("type").equals("ping")) || (jsonData.get("type").equals("join") && isIntroducer)){
                node.nodeID = Integer.parseInt(jsonData.get("nodeID").toString());
                node.nodeAddr = jsonData.get("nodeAddr").toString();
                node.nodePort = Integer.parseInt(jsonData.get("nodePort").toString());
            }
            if(messageType.equals("ping")){
                //logger.info("Handling ping situation...");
                String id = String.valueOf(serverNode.nodeID);
                DatagramPacket send_ack = new DatagramPacket(id.getBytes(),id.getBytes().length,receivedPacket.getAddress(),receivedPacket.getPort());
                try {
                    server.send(send_ack);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(messageType.equals("join")){
                logger.info("Handling join situation...");
                if(!isIntroducer){
                    JSONArray arr = jsonData.getJSONArray("totalList");
                    CopyOnWriteArrayList<Node> newTotalList = new CopyOnWriteArrayList<>();
                    for(int i=0;i<arr.length();i++){
                        Node tmp_node = new Node(0,"",0);
                        tmp_node.nodeID = Integer.parseInt(arr.getJSONObject(i).get("nodeID").toString());
                        tmp_node.nodeAddr = arr.getJSONObject(i).get("nodeAddr").toString();
                        tmp_node.nodePort = Integer.parseInt(arr.getJSONObject(i).get("nodePort").toString());
                        System.out.println("tmp_node.nodeID "+tmp_node.nodeAddr);
                        newTotalList.add(tmp_node);
                    }
                    logger.info("Size of newTotalList group:"+newTotalList.size());
                    for (int i=0;i<newTotalList.size();i++) {
                        Node n=newTotalList.get(i);
                        logger.info("Member"+(i+1)+" :");
                        logger.info("Node ID:"+n.nodeID+", Node Address:"+n.nodeAddr+", Node Port:"+n.nodePort);
                    }
                    logger.info("getting totalList from introducer..");
                    showGroupList();
                    showMembershipList();


                    if(!compareAndRenewTotalList(newTotalList)){
                        renewMemberList();
                    }
                    logger.info("After renewing membership list..");
                    showGroupList();
                    showMembershipList();
                }
                else{
                    renewTotalList(node);
                    renewMemberList();
                    broadcast(messageType,node);
                }
                

            }
            else if(messageType.equals("leave")){
                logger.info("Handling leave situation...");
                int nodeIndex = containsInstance(totalMemberList,node);
                if(nodeIndex>=0){
                    totalMemberList.remove(nodeIndex);
                    broadcast(messageType,node);
                    renewMemberList();
                }

            }
            else if(messageType.equals("delete")){
                logger.info("Handling delete situation...");
                int nodeIndex = containsInstance(totalMemberList,node);
                if(nodeIndex>=0){
                    totalMemberList.remove(nodeIndex);
                    broadcast(messageType,node);
                    renewMemberList();
                }

            }

        }catch (JSONException e){
            e.printStackTrace();
        }

    }
}
