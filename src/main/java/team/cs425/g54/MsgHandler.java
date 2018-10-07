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
        if(index>0)
            memberList.add(totalMemberList.get(index-1));
        for(int i=0;i<memberListSize;i++){
            index = (index+1) % totalMemberList.size();
            if(containsInstance(memberList,totalMemberList.get(index))>=0 || selfIndex!=index)
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
            if(i<totalMemberList.size())
                totalMemberList.add(node);
        }
    }
    public boolean compareAndRenewTotalList(CopyOnWriteArrayList<Node> newTotalList){
        if(newTotalList.size()!=totalMemberList.size()){
            totalMemberList.clear();
            totalMemberList = newTotalList;
            return false; // different and renew
        }

        for(int i=0;i<newTotalList.size();i++){
            if(!compareNode(newTotalList.get(i),totalMemberList.get(i))){
                totalMemberList.clear();
                totalMemberList = newTotalList;
                return false; // different and renew
            }
        }
        return true;
    }
    public void run(){
        logger.info("messageHandle start...");
        String receivedData = new String(receivedPacket.getData());
        
        try{
            JSONObject jsonData = new JSONObject(receivedData);
        
            String messageType = jsonData.get("type").toString();

                    // get new node information
            Node node = new Node(0,"",0);  // join need not use node but the whole list membership
            if(!jsonData.get("type").equals("join")){
                node.nodeID = Integer.parseInt(jsonData.get("nodeID").toString());
                node.nodeAddr = jsonData.get("nodeAddr").toString();
                node.nodePort = Integer.parseInt(jsonData.get("nodePort").toString());
            }
            if(messageType.equals("ping")){
                logger.info("Handling ping situation...");
                String id = String.valueOf(serverNode.nodeID);
                DatagramPacket send_ack = new DatagramPacket(id.getBytes(),id.getBytes().length,receivedPacket.getAddress(),serverNode.nodePort);
                try {
                    server.send(send_ack);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(messageType.equals("join")){
                logger.info("Handling join situation...");
                JSONArray arr = jsonData.getJSONArray("totalList");
                CopyOnWriteArrayList<Node> newTotalList = new CopyOnWriteArrayList<>();
                Node tmp_node = new Node(0,"",0);
                for(int i=0;i<arr.length();i++){
                    tmp_node.nodeID = Integer.parseInt(arr.getJSONObject(i).get("nodeID").toString());
                    tmp_node.nodeAddr = arr.getJSONObject(i).get("nodeAddr").toString();
                    tmp_node.nodePort = Integer.parseInt(arr.getJSONObject(i).get("nodePort").toString());
                    newTotalList.add(tmp_node);
                }
                if(!compareAndRenewTotalList(newTotalList)){
                    broadcast(messageType,node);
                    renewMemberList();
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
