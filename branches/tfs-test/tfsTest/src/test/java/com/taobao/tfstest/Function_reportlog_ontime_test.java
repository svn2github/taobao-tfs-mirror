package com.taobao.tfstest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import org.junit.Test;


import com.taobao.gaia.HelpBase;

/**
 * @author Administrator/lexin
 * 
 */
public class Function_reportlog_ontime_test extends HelpBase {

	private String ns_log_server = "10.232.4.1";
	/* Other */
	public String caseName = "";

	@Test
	public void ns_report_ontime_each_interval() {
		log.info("ns_report_ontime_each_interval===> start");

		Set<String> strServerList = new HashSet();

		strServerList.add("10.232.4.11:3200");
		strServerList.add("10.232.4.11:3202");
		strServerList.add("10.232.4.11:3204");
		strServerList.add("10.232.4.11:3206");
		strServerList.add("10.232.4.12:3200");
		strServerList.add("10.232.4.12:3202");
		strServerList.add("10.232.4.12:3204");
		strServerList.add("10.232.4.12:3206");

		for (String strServer : strServerList) {
			check_ns_report_on_server(strServer);
		}
	}

	private boolean check_ns_report_on_server(String strServer) {
		String fmt = "yyyy-MM-dd hh:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(fmt);

		try {
			// execute shell command:
			String strCmd = "cat nameserver.log | grep 'block successful'";
			strCmd += " | sed 's/\\[\\([ :0-9-]\\{19\\}\\)";
			strCmd += ".*\\(" + strServer + "\\).*/\\1/g'";
			Process fileSystemDfInfo = Runtime.getRuntime().exec(strCmd);
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					fileSystemDfInfo.getInputStream()));
			String strCurLine;
			String strNextLine;

			if ((strCurLine = reader.readLine()) != null) {
				while (true) {
					Date curDate = sdf.parse(strCurLine);
					if ((strNextLine = reader.readLine()) == null) {
						break;
					}
					Date nextDate = sdf.parse(strNextLine);

					assertEquals(5, (nextDate.getTime() - curDate.getTime()) / 1000);
					strCurLine = strNextLine;
				}
			}

			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}



}
