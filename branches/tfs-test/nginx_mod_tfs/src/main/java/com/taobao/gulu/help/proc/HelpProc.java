package com.taobao.gulu.help.proc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;

//import com.taobao.gaia.HelpBase;
import com.taobao.gulu.help.monitor.HelpShellOutputMonitor;
import com.taobao.gulu.server.ShellServer;



/**
 * @author gongyuan.cz
 *
 */

public class HelpProc
{

	private static Connection conn;
	private static Session session;

	public static void executeRemoteCommand(ShellServer server, String cmd, String expectStdout, String expectStderr) throws Exception 
	{
		initSession(server);
		System.out.println();
		System.out.println(cmd);
		session.execCommand(cmd);
		
		BufferedReader br1 = new BufferedReader(new InputStreamReader(session.getStdout()));
		try 
		{
			String line = br1.readLine();
			StringBuffer sb = new StringBuffer();
			while (line != null) 
			{
				sb.append(line);
				System.out.println(line);
				line = br1.readLine();
			}
			Assert.assertEquals("内容不匹配", true,sb.toString().contains(expectStdout));
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		
		BufferedReader br2 = new BufferedReader(new InputStreamReader(session.getStderr()));
		try 
		{
			String line = br2.readLine();
			StringBuffer sb = new StringBuffer();
			while (line != null)
			{
				sb.append(line);
				System.out.println(line);
				line = br2.readLine();
			}
			Assert.assertEquals("内容不匹配", true,sb.toString().contains(expectStderr));
		} catch (IOException e) 
		{
			e.printStackTrace();
		}
		
		session.waitForCondition(ChannelCondition.EXIT_STATUS, 0);
		int status = session.getExitStatus();
		System.out.println("exit_code=" + status);
		session.close();
		conn.close();
	}
	
	public static void executeRemoteCommandWithDumbPTY(ShellServer server, String cmd, String expectStdout, String expectStderr) throws Exception 
	{
		initSession(server);
		System.out.println();
		System.out.println(cmd);
		session.requestDumbPTY();
		session.execCommand(cmd);
		
		BufferedReader br1 = new BufferedReader(new InputStreamReader(session.getStdout()));
		try {
			String line = br1.readLine();
			StringBuffer sb = new StringBuffer();
			while (line != null) {
				sb.append(line);
				System.out.println(line);
				line = br1.readLine();
			}
			Assert.assertEquals("内容不匹配", true,sb.toString().contains(expectStdout));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		BufferedReader br2 = new BufferedReader(new InputStreamReader(session.getStderr()));
		try {
			String line = br2.readLine();
			StringBuffer sb = new StringBuffer();
			while (line != null) {
				sb.append(line);
				System.out.println(line);
				line = br2.readLine();
			}
			Assert.assertEquals("内容不匹配", true,sb.toString().contains(expectStderr));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		session.waitForCondition(ChannelCondition.EXIT_STATUS, 0);
		int status = session.getExitStatus();
		System.out.println("exit_code=" + status);
		session.close();
		conn.close();
	}
	
	public static void executeRemoteCommandNotEQ(ShellServer server, String cmd, String expectStdout) throws Exception {
		initSession(server);
		System.out.println();
		System.out.println(cmd);
		session.execCommand(cmd);
		
		BufferedReader br1 = new BufferedReader(new InputStreamReader(session.getStdout()));
		try {
			String line = br1.readLine();
			StringBuffer sb = new StringBuffer();
			while (line != null) {
				sb.append(line);
				System.out.println(line);
				line = br1.readLine();
			}
			Assert.assertFalse("内容不匹配", sb.toString().contains(expectStdout));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		BufferedReader br2 = new BufferedReader(new InputStreamReader(session.getStderr()));
		try {
			String line = br2.readLine();
			StringBuffer sb = new StringBuffer();
			while (line != null) {
				sb.append(line);
				System.out.println(line);
				line = br2.readLine();
			}
//			Assert.assertFalse("内容不匹配", sb.toString().contains(expectStderr));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		session.waitForCondition(ChannelCondition.EXIT_STATUS, 0);
		int status = session.getExitStatus();
		System.out.println("exit_code=" + status);
		session.close();
		conn.close();
	}

	private static void initSession(ShellServer server) throws IOException 
	{
		conn = new Connection(server.getHost());
		conn.connect();
		boolean success = conn.authenticateWithPassword(server.getUsername(),
				server.getPassword());
		Assert.assertTrue("ssh远程服务器失败", success);
		session = conn.openSession();
	}

	
	
	public void executeShell(List<String> cmd, String expectInputMessage, String expectErrorMessage)throws Exception{
		ProcessBuilder builder = new ProcessBuilder(cmd);
		Process p = builder.start();
		System.out.println();
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		executorService.execute(new HelpShellOutputMonitor(p.getInputStream(),expectInputMessage));
		executorService.execute(new HelpShellOutputMonitor(p.getErrorStream(),expectErrorMessage));
		int exit = p.waitFor();
		System.out.println(builder.command().toString() + " exit_code="
				+ exit);
		TimeUnit.SECONDS.sleep(2);
	}
	
//	public boolean netBlock(String tarIp, String blkIp) {
//		HelpBase helpBase = new HelpBase();
//		return helpBase.netFullBlockBase(tarIp, blkIp);
//	}
//	
//	public boolean netBlock(String tarIp, String blkIp, int iPort) {
//		HelpBase helpBase = new HelpBase();
//		return helpBase.portBlockBase(tarIp, blkIp, iPort);
//	}
//
//	public boolean netUnblock(String tarIp) {
//		HelpBase helpBase = new HelpBase();
//		return helpBase.netUnblockBase(tarIp);
//	}
	
}
