package team.cs425.g54;

import static org.junit.Assert.*;

import org.junit.Test;

public class GrepHandlerTest {
	
	@Test
	public void testGetGrepInfo() {
		//System.out.println("Create GrepHandler");
		//GrepHandler grepHandler=new GrepHandler();
		//System.out.println("Test getGrepInfo");
		//assertEquals("asdf",grepHandler.getGrepInfo("grep asdf"));
		//assertEquals("",grepHandler.getGrepInfo(""));
		//assertEquals("",grepHandler.getGrepInfo("just try"));
	}

	@Test
	public void testIsGrepInfo() {
		GrepHandler grepHandler=new GrepHandler();
		assertEquals(false,grepHandler.isGrepInfo(""));
		assertEquals(false,grepHandler.isGrepInfo("heartbeats"));
		assertEquals(true,grepHandler.isGrepInfo("grep sh"));
	}
	
	@Test
	public void testGetGrepResult() {
		GrepHandler grepHandler=new GrepHandler();
		String testFilename="test.log";
		String testFilepath="/home/MachineProgramming/test.log";
		//String res=grepHandler.getGrepResult("grep no", testFilename, testFilepath);
		//assertEquals("Nothing to add",res);
		//res=grepHandler.getGrepResult("testme", testFilename, testFilepath);
		//assertNull(res);
	}
	
	@Test
	public void testGetGrepResultByLines() {
		
	}
}
