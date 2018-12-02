package team.cs425.g54;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class FileUploader extends Thread{
	public String appType;
    final int TIMEOUT = 5000;
	
	// For streaming to write a file
    long lastWriteTime;
    long timeToSend = 20000;
    
    String filepath;
    AtomicBoolean fileChanged;
    
    ConcurrentHashMap<String,Integer> wordCounter;
    
    boolean stop;
    
    public FileUploader(String appType, String filepath) {
    	this.appType=appType;
    	this.filepath=filepath;
    	this.fileChanged=new AtomicBoolean(false);
    	this.lastWriteTime=System.currentTimeMillis();
    }
    
    public void setWordCounter(ConcurrentHashMap<String,Integer> wordCounter) {
    	this.wordCounter=wordCounter;
    }
    @Override
    public void run() {
    	this.stop=false;
    	while(!stop) {
    		//checkWriteDownTime();
    		//wordcountToFile(filepath);
    		checkWriteToLocal();
    		try {
    			sleep(5000);
    		} catch(InterruptedException e) {
    			e.printStackTrace();
    		}
    	}
    }
    
    public void setFileChanged() {
    	fileChanged.set(true);;
    }
    
    public void checkWriteDownTime() {
		// If filter, this uploader will put the file into sdfs every time unit
		if ((System.currentTimeMillis() - lastWriteTime) > timeToSend && fileChanged.get()) {
			System.out.println("send file to sdfs");
			putFileToSDFS(filepath, appType);
			lastWriteTime=System.currentTimeMillis();
			fileChanged.set(false);
		}
		
	}
    
    public void checkWriteToLocal() {
    	if ((System.currentTimeMillis() - lastWriteTime) > timeToSend && fileChanged.get()) {
			System.out.println("write file to local and sdfs");
			//wordcountToFile(filepath);
			String filename = filepath + System.currentTimeMillis();
			String sdfsname = appType;
			writeFileToLocal(filename);
			putFileToSDFS(filename, sdfsname);
			lastWriteTime=System.currentTimeMillis();
			fileChanged.set(false);
		}
    }
    
    public String getFilename() {
    	if(appType.equals("wordCount")) {
    		return filepath + System.currentTimeMillis();
    	}else {
    		return filepath;
    	}
    }
    
    public void writeFileToLocal(String filename) {
    	if(appType.equals("wordCount")) {
    		wordcountToFile(filename);
    	}else {
    		
    	}
    }
    
    public void wordcountToFile(String filepath) {
    	BufferedWriter bufferedWriter;
		try {
			bufferedWriter = new BufferedWriter(new FileWriter(filepath+System.currentTimeMillis()));
			for (Entry<String, Integer> entry : wordCounter.entrySet()) {
				bufferedWriter.write(entry.getKey()+" "+entry.getValue()+"\n");
				bufferedWriter.flush();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public ArrayList<Node> getNodeList(String str){
		
		ArrayList<Node> nodes = new ArrayList<>();
		try {
			JSONArray objArray = new JSONArray(str);
			for (int i = 0; i < objArray.length(); i++) {
				Node node = new Node();
				JSONObject jsonNode = objArray.getJSONObject(i);
				node.nodeAddr = jsonNode.get("nodeAddr").toString();
				node.nodeID = Integer.parseInt(jsonNode.get("nodeID").toString());
				node.nodePort = Integer.parseInt(jsonNode.get("nodePort").toString());
				nodes.add(node);
			}

		} catch (JSONException e) {
			e.printStackTrace();
		}
		return nodes;
	}
    
    public void putFileToSDFS(String filename, String sdfsName) {
    	try {
			DatagramSocket ds = new DatagramSocket();
			ds.setSoTimeout(TIMEOUT);

			String local = filename;
			String sdfs = sdfsName;
			String timestamp = String.valueOf(System.currentTimeMillis());
			// send msg to master
			JSONObject obj = new JSONObject();
			obj.put("type","toMaster");
			obj.put("command","put");
			obj.put("sdfsName",sdfs);
			obj.put("timestamp",timestamp);
			obj.put("nodeID", Detector.myNode.nodeID);
			obj.put("nodeAddr", Detector.myNode.nodeAddr);
			obj.put("nodePort", Detector.myNode.nodePort);
			String msgToMaster = obj.toString();
			InetAddress address = InetAddress.getByName(Detector.master.nodeAddr);
			DatagramPacket dpSent= new DatagramPacket(msgToMaster.getBytes(),msgToMaster.length(),address,Detector.master.nodePort);
			byte[] data = new byte[2048];
			DatagramPacket dpReceived = new DatagramPacket(data, 2048);
			ds.send(dpSent);
			ds.receive(dpReceived);

			String dpRecivedData = new String(dpReceived.getData());
			System.out.println("Received "+dpRecivedData);
			ArrayList<Node> nodes = getNodeList(dpRecivedData);

			for(Node node:nodes){
				Socket clientToNodes = new Socket(node.nodeAddr,Detector.toNodesPort);
				JSONObject obj2 = new JSONObject();
				obj2.put("type","put");
				obj2.put("sdfsName",sdfs);
				obj2.put("timestamp",timestamp);
				DataOutputStream outputStream = new DataOutputStream(clientToNodes.getOutputStream());
				outputStream.writeUTF(obj2.toString()); // send the put command to the node first
				FileInputStream fis = new FileInputStream(local);
				IOUtils.copy(fis,outputStream);
				outputStream.flush();
				clientToNodes.close();
			}

			ds.close();
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
    }
    
    public void stopUploader() {
    	this.stop=true;
    }
}
