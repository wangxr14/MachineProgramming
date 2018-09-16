package team.cs425.g54;
 
import org.grep4j.core.result.GrepResults;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Client {

	public static List<ClientThread> threadList = new ArrayList<ClientThread>();
	public static List<String> ipAddrList = new ArrayList<String>();
	public static List<Integer> portList = new ArrayList<Integer>(); 
	public static GrepHandler grepHandler = new GrepHandler();
	public static int myNum=0;
	public static HashMap<String,Integer> map = new HashMap<>();
	private static String inputInfo="";
	
	class ClientSocket extends Socket{
		protected Socket client;
		public ClientSocket(String ipAddr, int port) throws Exception{
			super(ipAddr,port);
			client=this;
		}
	}
	
	static class ClientThread extends Thread{
		private Socket socket;
		private long  startTime = System.currentTimeMillis();
		public ClientThread(Socket s) throws IOException {
			socket=s;
			//Bufferreader or File

			start();
		}
		
		@Override
		public void run() {
			threadList.add(this);
			DataInputStream input;
			//while(true) {
				try {
					input = new DataInputStream(socket.getInputStream());
		            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
		            out.writeUTF(inputInfo);
		            System.out.println("begin Input Object");
//					ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());

//					GrepObject grepObject = (GrepObject) objectInputStream.readObject();

//		            String ret = input.readUTF();
					String totals = input.readUTF(),totalLines="0",vmName="";
					if(totals.length()>1 || totals.split(" ").length>1){
						totalLines = totals.split(" ")[0];
						vmName = "vm"+totals.split(" ")[1];
					}
//					System.out.println(totalLines);
					// write result to file
					PrintWriter writer = new PrintWriter(vmName+".txt", "UTF-8");
					writer.println(vmName);
					InputStreamReader inputStreamReader = new InputStreamReader(input);
					BufferedReader br = new BufferedReader(inputStreamReader);
					String ret = null;
					int lineCnt = 0;

					while((ret = br.readLine())!=null && !totalLines.equals("0")){
						lineCnt++;
						if(lineCnt<30)
							System.out.println(vmName+" "+ret+"\n");
						writer.println(ret);

					}
					System.out.println(vmName+" received actual lines " +lineCnt+"\n");
					System.out.println(vmName+" total lines "+totalLines+"\n");
					if(totalLines.equals(""))
						writer.println(vmName+" , totalLines: 0");
					else if(Integer.parseInt(totalLines)==lineCnt){
						writer.println(vmName+" , totalLines: "+totalLines);

					}

//		            System.out.println("server sent: " + "VM "+grepObject.vmNum+"; line "+grepObject.index+" "+";totallines "+ grepObject.totalline);
					writer.close();
		            out.close();
		            input.close();
//		            objectInputStream.close();
					map.put(vmName,(int)(System.currentTimeMillis()-startTime)%1000);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			//}

		}
	}
	public static String getLogFilename() {
		return "vm"+myNum+".log";
	}
	public static String getLogFilepath() {
		return "/home/mp1/"+getLogFilename();
	}

    public static void main(String[] args) throws IOException {  
        
    	//
    	System.out.println("Please Input:");  
		try {
			String str = new BufferedReader(new InputStreamReader(System.in)).readLine();
			inputInfo=str;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	//Read in ip and port, vm set
    	String configFile="mp.config";
    	try {
    		BufferedReader in=new BufferedReader(new FileReader(configFile));
    		String line=in.readLine();
    		int count=1;
    		if(line!=null) {
    			myNum=Integer.parseInt(line);
    			line=in.readLine();
    		}
    		while(line!=null) {
    			if(count==myNum) {
    				line=in.readLine();
    				count++;
    				continue;
    			}
    			String[] splites=line.split(";");
    			ipAddrList.add(splites[1]);
    			portList.add(Integer.parseInt(splites[2]));
    			count++;
    			line=in.readLine();
    		}
    		
    	}catch(IOException e){
    		e.printStackTrace();
    	}
    	
    	
    	//Create sockets and threads
    	for (int i=0; i<ipAddrList.size();i++) {
    		try {
				System.out.println("sent to vm "+i);
    			Socket socket = new Socket(ipAddrList.get(i),portList.get(i));
//    			System.out.println("sent to vm "+i);
				ClientThread thread = new ClientThread(socket);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}

    	// get own log file
		if (grepHandler.isGrepInfo(inputInfo)){
			String linesInfo = grepHandler.getGrepResult(inputInfo,getLogFilename(),getLogFilepath());
			GrepResults grepResults = grepHandler.getGrepResultByLines(inputInfo,getLogFilename(),getLogFilepath());
			if(linesInfo.length()>0)
				linesInfo = linesInfo.substring(0,linesInfo.length()-1);
			String s = grepResults.toString();
			PrintWriter writer = new PrintWriter("vm"+myNum+".txt", "UTF-8");
			writer.println("vm"+myNum);
			int index = 0;
			for(String str: s.split("\n")){
				if(index<30)
					System.out.println("vm"+myNum+" "+str);
				writer.println(str);
				index++;
			}
			if(!linesInfo.equals("")){
				if(index == Integer.parseInt(linesInfo)){
					writer.println("Total lines: "+ index);
				}
				else {
					linesInfo = "0";
					writer.println("Total lines: 0");
				}
			}
			else{
				linesInfo = "0";
				writer.println("Total lines: 0");
			}
			System.out.println("vm"+myNum+" received actual lines " +index+"\n");
			System.out.println("vm"+myNum+" total lines "+linesInfo+"\n");
			writer.close();

			// output the cost of time in each thread
			for(Map.Entry<String,Integer> entry:map.entrySet()){
				System.out.println(entry.getKey()+", cost : " + entry.getValue() +" seconds");
			}
		}
    }
} 

