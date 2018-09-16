package team.cs425.g54;

import org.grep4j.core.result.GrepResult;
import org.grep4j.core.result.GrepResults;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
 
public class Server {
	public static final int PORT = 12345; 
	public static int myNum=0;
	public static GrepHandler grepHandler = new GrepHandler();
	
    public static void main(String[] args) {  
        Boolean res=grepHandler.isGrepInfo("heartbeats");
        System.out.println(res);
    	// Config
        String configFile="mp.config";
    	try {
    		BufferedReader in=new BufferedReader(new FileReader(configFile));
    		String line=in.readLine();
    		if(line!=null) {
    			myNum=Integer.parseInt(line);
    			line=in.readLine();
    		}
    		
    	}catch(IOException e){
    		e.printStackTrace();
    	}
  
        Server server = new Server();  
        server.init();  
    }  
  
    public void init() {  
        try {  
            ServerSocket serverSocket = new ServerSocket(PORT);
            System.out.println("Server listening at "+PORT);
            while(true) {
            	Socket client = serverSocket.accept();    
            	new HandlerThread(client);
            }
        } catch (Exception e) {  
            System.out.println("Server error: " + e.getMessage());  
        }  
    }  
    
    public String getLogFilename() {
    	return "vm"+myNum+".log";
    }
    
    public String getLogFilepath() {
    	return "/home/mp1/"+getLogFilename();
    }
  
    private class HandlerThread implements Runnable {  
        private Socket socket;  
        public HandlerThread(Socket client) {  
        	socket = client;  
        	new Thread(this).start();  
        }  
  
        public void run() {  
            try {  
                DataInputStream input = new DataInputStream(socket.getInputStream());
                String clientInputStr = input.readUTF();
                System.out.println("Receive from client " + clientInputStr);
                String retInfo="";
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                if (grepHandler.isGrepInfo(clientInputStr)) {
                	System.out.println("is grep info");
                	retInfo=grepHandler.getGrepResult(clientInputStr, getLogFilename(), getLogFilepath());

                    GrepResults grepResults = grepHandler.getGrepResultByLines(clientInputStr, getLogFilename(), getLogFilepath());
                    int totallines = grepResults.size();
                    int index = 1;
                    for(GrepResult result:grepResults){

                        GrepObject grepObject = new GrepObject(totallines,index,result.toString(),myNum);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        objectOutputStream.writeObject(grepObject);
                        index++;
//                        out.writeUTF("VM"+myNum+", Line number "+result.toString()+"; totalLines: "+totallines);

                    }
                }
                else {
                	System.out.println("not grep info");
                	retInfo=clientInputStr;
                }
                System.out.println(retInfo);

          
                out.writeUTF(retInfo);  
                
                out.close();  
                input.close();  
            } catch (Exception e) {  
                System.out.println("Server run error: " + e.getMessage());  
            } finally {  
                if (socket != null) {  
                    try {  
                        socket.close();  
                        System.out.println("Socket closed");
                    } catch (Exception e) {  
                        socket = null;  
                        System.out.println("Server finally error:" + e.getMessage());  
                    }  
                }  
            } 
        }  
    }  
}