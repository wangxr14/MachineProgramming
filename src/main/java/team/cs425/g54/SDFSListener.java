package team.cs425.g54;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

public class SDFSListener extends Thread{
    static final Logger logger = Logger.getLogger("main.java.team.cs425.g54.MessageHandler");
    boolean isFinished = false;  // whether stop instruction is received
    ServerSocket serverSocket;
    Node serverNode;
    SDFSListener(Node node,int testNode){
        this.serverNode = new Node(node.nodeID,node.nodeAddr,testNode);
        try {
            serverSocket = new ServerSocket(serverNode.nodePort);
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("sdfs listener not connected");
        }
    }
    public void stopListen(){
        this.isFinished = true;
    }

    public void restartListen(){
        this.isFinished = false;
    }

    public void run(){

        try {
            while(!Thread.currentThread().isInterrupted() ) { // running
//                System.out.println("listerning");
                Socket receivedSocket = serverSocket.accept(); // build tcp connection receivedSocket is a new socket
                SDFSMsgHandler sdfsMsgHandler = new SDFSMsgHandler(receivedSocket,serverNode);
                sdfsMsgHandler.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
