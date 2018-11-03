package team.cs425.g54;

import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

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
                fileOutputStream = new FileOutputStream(sdfsName+"_"+timestamp);
                IOUtils.copy(dataInputStream,fileOutputStream); // write file into the disk;
                fileOutputStream.flush();
                logger.info("file writtern ...");
                Detector.storeInfo.addFileUpdate(sdfsName,timestamp); // update the file list and clean old version on disk
                // update master info
                if(serverNode.nodeID == Detector.master.nodeID){
                    Detector.masterInfo.addNodeFile(serverNode,sdfsName);
                    Detector.masterInfo.updateFileVersion(sdfsName,timestamp);
                }
            }

            else if(messageType.equals("ls")){
                String sdfsName = jsonData.get("sdfsName").toString();
                String result = "true";
                if(!Detector.storeInfo.hasFile(sdfsName))
                    result = "false";
                dataOutputStream.writeUTF(result);
            }
            else if(messageType.equals("get_version")){
                String sdfsName = jsonData.get("sdfsName").toString();
                int num = Integer.parseInt(jsonData.get("versionNum").toString());
                ArrayList<String> versions = Detector.storeInfo.getKVersions(sdfsName,num);

                for(String version:versions){
                    String filename = sdfsName+"_"+version;
                    dataOutputStream.writeUTF(filename);  // get file name
                    fileInputStream = new FileInputStream(filename);
                    IOUtils.copy(fileInputStream,dataOutputStream);
                    dataOutputStream.flush();
                }

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
                fileInputStream = new FileInputStream(fileName);
                IOUtils.copy(fileInputStream,dataOutputStream);
                dataOutputStream.flush();

            }
            else if(messageType.equals("delete")){
                String sdfsName = jsonData.get("sdfsName").toString();
                Detector.storeInfo.deleteFileUpdate(sdfsName); // update the list
                Detector.masterInfo.deleteNodeFile(serverNode,sdfsName);
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
