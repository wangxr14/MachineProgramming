package team.cs425.g54;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


import org.grep4j.core.Grep4j;
import org.grep4j.core.command.linux.grep.*;
import org.grep4j.core.model.Profile;
import org.grep4j.core.model.ProfileBuilder;
import org.grep4j.core.options.Option;
import org.grep4j.core.result.GrepResult;
import org.grep4j.core.result.GrepResults;

public class GrepHandler {
	
	public String getGrepInfo(String inputInfo) {
		String[] words = inputInfo.split(" ");
		for (int i=0;i<words.length;i++) {
			if(words[i].toLowerCase().equals("grep") && i<words.length-1) {
				return words[i+1];
			}
		}
		return "";
	}

	public String getGrepResult(String inputInfo, String filename, String filepath) {
		String grepInfo=getGrepInfo(inputInfo);
		Profile locallog = ProfileBuilder.newBuilder().name(filename).filePath(filepath).onLocalhost().build();
		System.out.println(locallog.toString());
		// call regular expression pattern to find the strings that are in the log
		GrepResults results = Grep4j.grep(Grep4j.regularExpression(grepInfo),locallog, Option.countMatches());
		System.out.println(results.toString());
		return results.toString();

	}

	public GrepResults getGrepResultByLines(String inputInfo, String filename, String filepath){
		String grepInfo=getGrepInfo(inputInfo);
		Profile locallog = ProfileBuilder.newBuilder().name(filename).filePath(filepath).onLocalhost().build();
		GrepResults results = Grep4j.grep(Grep4j.regularExpression(grepInfo),locallog,Option.lineNumber());
		return results;
	}
	
	public Boolean isGrepInfo(String inputInfo) {
		if(inputInfo.isEmpty())
			return false;
		String[] words = inputInfo.split(" ");
		//System.out.println(words.length);
		for (int i=0;i<words.length;i++) {
			if(words[i].toLowerCase().equals("grep")) {
				return true;
			}
		}
		return false;
	}
}
