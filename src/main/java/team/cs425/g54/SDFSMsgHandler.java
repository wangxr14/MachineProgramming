package team.cs425.g54;

import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

import com.jcraft.jsch.Logger;

import java.io.*;
import java.lang.reflect.Array;
import java.net.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.CopyOnWriteArrayList;

import static team.cs425.g54.MsgHandler.logger;

public class SDFSMsgHandler extends Thread{
    SDFSMsgHandler(Socket socket,Node node){
        this.socket = socket;
        this.serverNode = node;
    }
    Node serverNode;
    DataInputStream dataInputStream;
    DataOutputStream dataOutputStream;
    FileInputStream fileInputStream;
    FileOutputStream fileOutputStream;
    Socket socket;

    String packSendAddMsg(String file,String timestamp){
        JSONObject obj = new JSONObject();
        try {
            obj.put("sdfsName",file);
            obj.put("timestamp",timestamp);
            obj.put("command","updateAddfile");
            obj.put("type","toMaster");
            obj.put("nodeID",serverNode.nodeID);
            obj.put("nodeAddr",serverNode.nodeAddr);
            obj.put("nodePort",serverNode.nodePort);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return obj.toString();
    }
    String packSendDeleteMsg(String file){
        JSONObject obj = new JSONObject();
        try {
            obj.put("sdfsName",file);
            obj.put("type","toMaster");
            obj.put("command","updateDeletefile");
            obj.put("nodeID",serverNode.nodeID);
            obj.put("nodeAddr",serverNode.nodeAddr);
            obj.put("nodePort",serverNode.nodePort);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return obj.toString();
    }
    public void sendPackageToMaster(String msg){
        try {
            InetAddress address = InetAddress.getByName(Detector.master.nodeAddr);
            DatagramPacket send_message = new DatagramPacket(msg.getBytes(), msg.getBytes().length, address, Detector.master.nodePort);
            DatagramSocket ds = new DatagramSocket();
            ds.send(send_message);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public void run(){

        try{
            dataInputStream = new DataInputStream(socket.getInputStream());
            dataOutputStream = new DataOutputStream(socket.getOutputStream());

            String receivedData = dataInputStream.readUTF();
            JSONObject jsonData = new JSONObject(receivedData);

            String messageType = jsonData.get("type").toString(); // the first message to get command
            logger.info("messagetype "+messageType);
            // write the file as the sdfsname
            logger.info(jsonData.toString());
            if(messageType.equals("put")){
                String sdfsName = jsonData.get("sdfsName").toString();
                String timestamp = jsonData.get("timestamp").toString();
                fileOutputStream = new FileOutputStream(Detector.SDFSPath+sdfsName+"_"+timestamp);
                IOUtils.copy(dataInputStream,fileOutputStream); // write file into the disk;
                fileOutputStream.flush();
                logger.info("file writtern ...");
                Detector.storeInfo.addFileUpdate(sdfsName,timestamp); // update the file list and clean old version on disk
                // update master info
                if(serverNode.nodeID == Detector.master.nodeID){
                    Detector.masterInfo.addNodeFile(serverNode,sdfsName);
                    Detector.masterInfo.updateFileVersion(serverNode,sdfsName,timestamp);

                }
                else{
                    String msg = packSendAddMsg(sdfsName,timestamp);
                    sendPackageToMaster(msg);
                }

            }

//            else if(messageType.equals("ls")){
//                String sdfsName = jsonData.get("sdfsName").toString();
//                String result = "true";
//                if(!Detector.storeInfo.hasFile(sdfsName))
//                    result = "false";
//                dataOutputStream.writeUTF(result);
//            }
            else if(messageType.equals("get_version")){
                String sdfsName = jsonData.get("sdfsName").toString();
                String timestamp = jsonData.get("timestamp").toString();
                
                for(String version:Detector.storeInfo.fileVersions.get(sdfsName)){
                    if(version.equals(timestamp)) {
                    	String filename = sdfsName+"_"+version;
                        dataOutputStream.writeUTF(filename);  // get file name
                        fileInputStream = new FileInputStream(Detector.SDFSPath+filename);
                        IOUtils.copy(fileInputStream,dataOutputStream);
                        dataOutputStream.flush();
                    }
                }
                System.out.println("get_version done");

            }
            else if(messageType.equals("get")){
                String sdfsName = jsonData.get("sdfsName").toString();
                String version = Detector.storeInfo.getLatestVersion(sdfsName);
                if(version.equals("NULL")) {
                    dataOutputStream.writeUTF("no such file");
                    logger.info("no such file to get");
                    return;
                }
                String fileName = sdfsName+"_"+version;
                fileInputStream = new FileInputStream(Detector.SDFSPath+fileName);
                IOUtils.copy(fileInputStream,dataOutputStream);
                dataOutputStream.flush();

            }
            else if(messageType.equals("delete")){
                String sdfsName = jsonData.get("sdfsName").toString();
                Detector.storeInfo.deleteFileUpdate(sdfsName); // update the list
                if(serverNode.nodeID == Detector.master.nodeID) {
                    Detector.masterInfo.deleteNodeFile(serverNode, sdfsName);
                }
                else{
                    String msg = packSendDeleteMsg(sdfsName);
                    sendPackageToMaster(msg);
                }
            }
            else if(messageType.equals("store")){  // checked
                Detector.storeInfo.showFiles();
            }


        } catch (JSONException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
