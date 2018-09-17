package team.cs425.g54;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class JUnitRunner {
	public static void main(String[] args) {
		System.out.println("Start unit testing now");
		//GrepHandler
		System.out.println("Test GrepHandler");
		Result grepResult = JUnitCore.runClasses(GrepHandlerTest.class);
		for (Failure failure : grepResult.getFailures()) {
			System.out.println(failure.toString());
		}
		if (grepResult.wasSuccessful()) {
			System.out.println("GrepHandler passed");
		}
		
		//Client
		Result clientResult = JUnitCore.runClasses(ClientTest.class);
		for (Failure failure : clientResult.getFailures()) {
			System.out.println(failure.toString());
		}
		if (clientResult.wasSuccessful()) {
			System.out.println("Client passed");
		}
		
		//Server
		Result serverResult = JUnitCore.runClasses(ServerTest.class);
		for (Failure failure : serverResult.getFailures()) {
			System.out.println(failure.toString());
		}
		if (serverResult.wasSuccessful()) {
			System.out.println("Server passed");
		}
		
		// To test the function of client and server
		JUnitRunner runner=new JUnitRunner();
		runner.runTest("grep a");
		//runner.runTest("grep asdfg");
		//runner.runTest("grep asd");
		
	}
	
	public class TestThread implements Runnable {
		private Server socket;
		public TestThread(Server socket) {    
        	this.socket=socket;
			new Thread(this).start();  
        }
		
		@Override
		public void run() {
			socket.init();
			
		} 
	
	}
	
	public void runTest(String query) {
		List<TestThread> serverList = new ArrayList<TestThread>(); 
		Client client = new Client();
		List<String> ipAddrList = new ArrayList<String>();
		List<Integer> portList = new ArrayList<Integer>(); 
		
		int serverNum=5;
		String grepCommand=query;
		client.myNum=0;
		
		for (int i=1;i<=serverNum;i++) {
			int port=10001+i*10;
			ipAddrList.add("127.0.0.1");
			portList.add(port);
			Server server = new Server();
			server.PORT=port;
			server.myNum=i;
			try {   
				TestThread thread=new TestThread(server);
	        } catch (Exception e) {  
	            System.out.println("Server error: " + e.getMessage());  
	        }  
		}
		
		client.ipAddrList=ipAddrList;
		client.portList=portList;
		client.execute();
		
	}
	
}
