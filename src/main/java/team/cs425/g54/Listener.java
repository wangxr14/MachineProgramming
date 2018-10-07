package team.cs425.g54;

import org.json.JSONObject;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


public class Listener extends Thread{
    Node serverNode;
    CopyOnWriteArrayList<Node> memberList;  // membership list, add element while traverse the list
    CopyOnWriteArrayList<Node> totalMemberList;  // membership list, add element while traverse the list
    DatagramSocket server;  // UDP connection
    boolean isFinished = false;  // whether stop instruction is received
    boolean isIntroducer;

    public Listener(Node node,CopyOnWriteArrayList<Node> memberList,CopyOnWriteArrayList<Node> totalMemberList,boolean isIntroducer){
        this.serverNode = new Node(node.nodeID,node.nodeAddr,node.nodePort);
        this.memberList = memberList;
        this.totalMemberList = totalMemberList;
        this.isIntroducer = isIntroducer;
    }

    public void stopListen(){
        this.isFinished = true;
    }

    public void run(){
        try {
            server = new DatagramSocket(this.serverNode.nodePort);
            server.setSoTimeout(500);  // the time of socket time out

            while(!Thread.currentThread().isInterrupted() && !isFinished){ // running
                boolean receivedResponse = false;     //mark whether the data is received
                byte[] receivedData = new byte[2048];
                DatagramPacket receivedPacket = new DatagramPacket(receivedData,receivedData.length); // receive package
                try{
                    server.receive(receivedPacket);
                    receivedResponse = true;
                } catch (IOException e) {
//                    e.printStackTrace();
                    continue;  // packet has not come yet
                }
                if(receivedResponse){
                    MsgHandler handler = new MsgHandler(serverNode,server,receivedPacket,isIntroducer,totalMemberList,memberList);
                    handler.start();
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }

    }
}
