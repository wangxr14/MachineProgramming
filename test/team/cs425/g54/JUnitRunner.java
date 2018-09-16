package team.cs425.g54;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
public class JUnitRunner {

	/**
	 * @param args
	 */
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
		Result clientResult = JUnitCore.runClasses(GrepHandlerTest.class);
		for (Failure failure : clientResult.getFailures()) {
			System.out.println(failure.toString());
		}
		if (clientResult.wasSuccessful()) {
			System.out.println("Client passed");
		}
		
		//Server
		Result serverResult = JUnitCore.runClasses(GrepHandlerTest.class);
		for (Failure failure : serverResult.getFailures()) {
			System.out.println(failure.toString());
		}
		if (serverResult.wasSuccessful()) {
			System.out.println("Server passed");
		}
		
	}
}
