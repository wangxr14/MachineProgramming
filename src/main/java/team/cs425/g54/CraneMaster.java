package team.cs425.g54;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import team.cs425.g54.topology.Bolt;
import team.cs425.g54.topology.Record;
import team.cs425.g54.topology.Spout;
import team.cs425.g54.topology.Topology;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.logging.Logger;

public class CraneMaster {
    Logger logger = Logger.getLogger("main.java.team.cs425.g54.Detector");
    public Topology curTopology;
    int myID;
    String myAddr;
    String fileSpout; // spout to read file data
    int totalWorker;
    Node spoutNode;
    ArrayList<Node> firstLevelWorkers;  // all workers
    ArrayList<Node> secondLevelWorker;  // all workers

    DatagramSocket server;
    public CraneMaster(String ipAddr,int id,String file,Node spoutNode){
        this.myID=id;
        this.myAddr=ipAddr;
        this.fileSpout = file;
        totalWorker = Detector.groupList.size()-1;
        firstLevelWorkers = new ArrayList<>();
        this.spoutNode = spoutNode;
        curTopology = new Topology();
        setStandByMaster();
    }

    // constructTopology according to application type
    void constructTopology(){
        firstLevelWorkers = new ArrayList<>();
        secondLevelWorker = new ArrayList<>();
        for(Node node:Detector.groupList){
            if(node.nodeID==spoutNode.nodeID || node.nodeID==myID || node.nodeID == Detector.standByMaster.nodeID)
                continue;
            firstLevelWorkers.add(node);
        }
        secondLevelWorker.add(firstLevelWorkers.get(0));
        firstLevelWorkers.remove(0);
        // initialize spout
        for(Spout spout:curTopology.spoutList){
            Record spoutRecord = new Record(spoutNode.nodeID,spoutNode.nodeAddr,spout.appType,"","spout",firstLevelWorkers);
            curTopology.addRecord(spoutRecord);
            logger.info("spout NodeID "+spoutNode.nodeID);
        }
        int i = 0;
        for(Bolt bolt:curTopology.boltList){
            if(i==curTopology.boltList.size()-1){
                ArrayList<Node> tmp = new ArrayList<>();// null arraylist
                Record bolt2 = new Record(secondLevelWorker.get(0).nodeID,secondLevelWorker.get(0).nodeAddr,
                        bolt.functionType,bolt.info,"bolt",tmp);
                curTopology.addRecord(bolt2);
                logger.info("bolt NodeID "+secondLevelWorker.get(0).nodeID+" function "+bolt.functionType);
            }
            else{
                for(Node worker:firstLevelWorkers){
                    Record bolt1 = new Record(worker.nodeID,worker.nodeAddr,bolt.functionType,bolt.info,"bolt",secondLevelWorker);
                    curTopology.addRecord(bolt1);
                    logger.info("bolt NodeID "+worker.nodeID+" function "+bolt.functionType+" info "+bolt.info);
                }
            }
            i++;
        }
        backUpStandByMaster();
    }
    void setStandByMaster(){
        for(Node node:Detector.groupList){
            if(node.nodeID!=Detector.craneMaster.nodeID){
                Detector.standByMaster = node;
                break;
            }
        }
        logger.info("set standby master as node "+Detector.standByMaster.nodeID);
        // broadcast standByMaster
        broadcastMasterMsgToAll("standByMaster",Detector.standByMaster);
    }
    // distribute task to each node in topology
    void sendTask() {
        try {
            server = new DatagramSocket();
            ArrayList<Record> records = curTopology.getRecordList();
            for (Record record : records) {
                JSONObject jsonMsg = new JSONObject();
                jsonMsg.put("workerType", record.getWorkerType());
                jsonMsg.put("appType", record.getAppType());
                jsonMsg.put("filename", fileSpout);
                jsonMsg.put("info",record.getInfo());
                JSONArray arr = new JSONArray();
                ArrayList<Node> children = record.getChildren();
                for (Node child : children) {
                    JSONObject obj = new JSONObject();
                    obj.put("nodeID", child.nodeID);
                    obj.put("nodeAddr", child.nodeAddr);
                    arr.put(obj);
                }
                jsonMsg.put("children", arr);
                InetAddress address = InetAddress.getByName(record.getIpAddr());
                DatagramPacket send_message = new DatagramPacket(jsonMsg.toString().getBytes(), jsonMsg.toString().getBytes().length, address,Detector.sendTaskPort);
                server.send(send_message);
            }


        }catch (JSONException e) {
            e.printStackTrace();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // send backup msg to standby master
    public void backUpStandByMaster(){
        try {
            server = new DatagramSocket();
            ArrayList<Record> records = curTopology.getRecordList();
            JSONObject jsonAllMsgs = new JSONObject();
            JSONArray arrRecord = new JSONArray();
            for (Record record : records) {
                JSONObject jsonMsg = new JSONObject();
                jsonMsg.put("workerType", record.getWorkerType());
                jsonMsg.put("appType", record.getAppType());
                jsonMsg.put("filename", fileSpout);
                jsonMsg.put("info",record.getInfo());
                JSONArray arr = new JSONArray();
                ArrayList<Node> children = record.getChildren();
                for (Node child : children) {
                    JSONObject obj = new JSONObject();
                    obj.put("nodeID", child.nodeID);
                    obj.put("nodeAddr", child.nodeAddr);
                    arr.put(obj);
                }
                jsonMsg.put("children", arr);
                arr.put(jsonMsg);
            }
            JSONArray spoutArr = new JSONArray();
            for(Spout spout:curTopology.spoutList){
                JSONObject obj = new JSONObject();
                obj.put("functionType",spout.appType);
                obj.put("spoutFile",spout.spoutFile);
                spoutArr.put(obj);
            }
            JSONArray boltArr = new JSONArray();
            for(Bolt bolt:curTopology.boltList){
                JSONObject obj = new JSONObject();
                obj.put("functionType",bolt.functionType);
                obj.put("info",bolt.info);
                boltArr.put(obj);
            }
            jsonAllMsgs.put("fileSpout",fileSpout); // for input file stream
            jsonAllMsgs.put("spoutID",spoutNode.nodeID);
            jsonAllMsgs.put("spoutAddr",spoutNode.nodeAddr);
            jsonAllMsgs.put("spoutArr",spoutArr);
            jsonAllMsgs.put("boltArr",boltArr);
            jsonAllMsgs.put("clone",arrRecord);
            InetAddress address = InetAddress.getByName(Detector.standByMaster.nodeAddr);
            DatagramPacket send_message = new DatagramPacket(jsonAllMsgs.toString().getBytes(), jsonAllMsgs.toString().getBytes().length, address,Detector.sendTaskPort);
            server.send(send_message);
            logger.info("sending backup standbymaster msg..");

        }catch (JSONException e) {
            e.printStackTrace();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void broadcastMasterMsgToAll(String type, Node toSend) {
        try {
            DatagramSocket ds = new DatagramSocket();

            for(Node node : Detector.groupList) {
                JSONObject message = new JSONObject();
                message.put("type", type);
                message.put("nodeID", toSend.nodeID);
                message.put("nodeAddr", toSend.nodeAddr);
                message.put("nodePort", toSend.nodePort);
                InetAddress address = InetAddress.getByName(node.nodeAddr);
                DatagramPacket send_message = new DatagramPacket(message.toString().getBytes(), message.toString().getBytes().length, address, node.nodePort);
                ds.send(send_message);
            }
            ds.close();
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e){
            e.printStackTrace();
        }
    }


}
