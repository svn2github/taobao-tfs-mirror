package com.taobao.gulu.help.monitor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.junit.Assert;

/**
 * @author gongyuan.cz
 *
 */

public class HelpShellOutputMonitor implements Runnable {

	private InputStream inputStream;
	private String expectInput;

	public HelpShellOutputMonitor(InputStream inputStream, String expectInput) {
		this.inputStream = inputStream;
		this.expectInput = expectInput;
	}

	public void run() {
		BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
		try {
			StringBuffer sb = new StringBuffer();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				System.out.println(line);
				line = br.readLine();
			}
			Assert.assertEquals("ShellOutputMonitor error!!", true, sb.toString().contains(expectInput));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
