package team.cs425.g54;
 
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
 
public class Client {

	public static List<ClientThread> threadList = new ArrayList<ClientThread>();
	public static List<String> ipAddrList = new ArrayList<String>();
	public static List<Integer> portList = new ArrayList<Integer>(); 
	public static GrepHandler grepHandler = new GrepHandler();
	public static int myNum=0;
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
					String totals = input.readUTF();
					String totalLines = totals.split(" ")[0];
					String vmName = "VM"+totals.split(" ")[1];
//					System.out.println(totalLines);
					// write result to file
					PrintWriter writer = new PrintWriter(vmName+".txt", "UTF-8");
					writer.println(vmName);
					InputStreamReader inputStreamReader = new InputStreamReader(input);
					BufferedReader br = new BufferedReader(inputStreamReader);
					String ret = null;
					int lineCnt = 0;

					while((ret = br.readLine())!=null){
						lineCnt++;
						System.out.println(ret+"\n");
						writer.println(vmName+" , totalLines: "+totalLines);

					}
					System.out.println(lineCnt+"\n");
					if(Integer.parseInt(totalLines)==lineCnt){
						writer.println(vmName+" , totalLines: "+totalLines);
						System.out.println(totalLines+"\n");
					}

//		            System.out.println("server sent: " + "VM "+grepObject.vmNum+"; line "+grepObject.index+" "+";totallines "+ grepObject.totalline);
					writer.close();
		            out.close();
		            input.close();
//		            objectInputStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			//}
            
		}
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
    			Socket socket = new Socket(ipAddrList.get(i),portList.get(i));
				ClientThread thread = new ClientThread(socket);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    }
} 

