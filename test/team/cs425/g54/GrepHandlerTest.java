package team.cs425.g54;

import static org.junit.Assert.*;

import org.junit.Test;

public class GrepHandlerTest {

	@Test
	public void test() {
		//fail("Not yet implemented");
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
		
	}
}
