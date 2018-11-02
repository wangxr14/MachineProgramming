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
    public void updateVersion(String sdfsName){
        File dict = new File(""); // get all local file
        File[] fileArray = dict.listFiles();
        if(fileArray==null)
            return;
        long min_timestamp = Long.MAX_VALUE;
        for(int i=0;i<fileArray.length;i++){
            if(fileArray[i].isFile() && fileArray[i].getName().contains(sdfsName)){
                String timestamp = fileArray[i].getName().split("_")[1];
                min_timestamp = Math.min(Long.parseLong(timestamp),min_timestamp);
            }
        }
        String deleteFileName = sdfsName+"_"+String.valueOf(min_timestamp);
        File deleteFile = new File(deleteFileName);  // delete old version
        if(deleteFile.delete()){
            logger.info("delete old version "+deleteFileName+" successfully..");
        }
        else
            logger.info("file delete failed...");
    }


    public void run(){

        try{
            dataInputStream = new DataInputStream(socket.getInputStream());
            dataOutputStream = new DataOutputStream(socket.getOutputStream());

            String receivedData = dataInputStream.readUTF();
            JSONObject jsonData = new JSONObject(receivedData);

            String messageType = jsonData.get("type").toString(); // the first message to get command

            // write the file as the sdfsname
            if(messageType.equals("put")){
                String sdfsName = jsonData.get("sdfsName").toString();
                String timestamp = jsonData.get("timestamp").toString();
                fileOutputStream = new FileOutputStream(sdfsName+"_"+timestamp);
                IOUtils.copy(dataInputStream,fileOutputStream); // write file into the disk;
                fileOutputStream.flush();
                logger.info("file writtern ...");
                updateVersion(sdfsName);  // delete the old version
            }

            else if(messageType.equals("ls")){
                String sdfsName = jsonData.get("sdfsName").toString();
                File dict = new File(""); // get all local file
                File[] fileArray = dict.listFiles();
                String result = "";
                int i;
                if(fileArray==null){
                    dataOutputStream.writeUTF("false");
                    return ;
                }
                for(i=0;i<fileArray.length;i++){
                    if(fileArray[i].isFile() && fileArray[i].getName().contains(sdfsName)){
                        result = "true";
                        break;
                    }
                }
                if(i<fileArray.length)
                    result = "false";
                dataOutputStream.writeUTF(result);
            }
            else if(messageType.equals("get_version")){
                String sdfsName = jsonData.get("sdfsName").toString();
                int num = Integer.parseInt(jsonData.get("versionNum").toString());
                File dict = new File(""); // get all local file
                File[] fileArray = dict.listFiles();
                String result = "";
                ArrayList<Long> versions = new ArrayList<>();

                if(fileArray==null){
                    result = "false";
                    dataOutputStream.writeUTF("no file existed");
                    return;
                }

                for(int i=0;i<fileArray.length;i++){
                    if(fileArray[i].isFile() && fileArray[i].getName().contains(sdfsName)){
                        Long timestamp = Long.parseLong(fileArray[i].getName().split("_")[1]);
                        versions.add(timestamp);
                    }
                }
                Collections.sort(versions, new Comparator<Long>() {
                    @Override
                    public int compare(Long o1, Long o2) {
                        return o1.compareTo(o2);
                    }
                });
                for(int i=0;i<num;i++){
                    if(i>=versions.size())
                        break;
                    String t = String.valueOf(versions.get(i));
                    String filename = sdfsName+"_"+t;
                    dataOutputStream.writeUTF(filename);  // get file name
                    fileInputStream = new FileInputStream(filename);
                    IOUtils.copy(fileInputStream,dataOutputStream);
                    dataOutputStream.flush();
                }

            }
            else if(messageType.equals("get")){
                String sdfsName = jsonData.get("sdfsName").toString();
                File dict = new File(""); // get all local file
                File[] fileArray = dict.listFiles();
                String result = "";
                int i;

                long max_timestamp = 0;
                if(fileArray==null){
                    dataOutputStream.writeUTF("no file existed");
                    return;
                }

                for(i=0;i<fileArray.length;i++){
                    if(fileArray[i].isFile() && fileArray[i].getName().contains(sdfsName)){
                        String timestamp = fileArray[i].getName().split("_")[1];
                        max_timestamp = Math.max(Long.parseLong(timestamp),max_timestamp);
                    }
                }
                String fileName = sdfsName+"_"+String.valueOf(max_timestamp);
                fileInputStream = new FileInputStream(fileName);
                IOUtils.copy(fileInputStream,dataOutputStream);
                dataOutputStream.flush();

            }
            else if(messageType.equals("delete")){
                String sdfsName = jsonData.get("sdfsName").toString();
                ArrayList<String> files = new ArrayList<>();
                File dict = new File(""); // get all local file
                File[] fileArray = dict.listFiles();
                if(fileArray==null)
                    return;
                long min_timestamp = Long.MAX_VALUE;
                for(int i=0;i<fileArray.length;i++){
                    if(fileArray[i].isFile() && fileArray[i].getName().contains(sdfsName)){
                        files.add(fileArray[i].getName());
                    }
                }
                for(int i=0;i<files.size();i++){
                    File d = new File(files.get(i));
                    if(d.delete())
                        logger.info(files.get(i)+" delete successfully");
                    else
                        logger.info(files.get(i)+" delete failed");
                }
            }
            else if(messageType.equals("store")){
                File dict = new File(""); // get all local file
                File[] fileArray = dict.listFiles();
                if(fileArray==null)
                    return;
                System.out.println("Current store file");
                for(int i=0;i<fileArray.length;i++){
                    if(fileArray[i].isFile()){
                        System.out.println(fileArray[i].getName());
                    }
                }
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
