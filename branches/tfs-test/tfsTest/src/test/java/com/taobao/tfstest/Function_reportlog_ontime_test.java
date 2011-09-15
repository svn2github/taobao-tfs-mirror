package com.taobao.tfstest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Assert;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import com.taobao.gaia.AppCluster;
import com.taobao.gaia.AppServer;
import com.taobao.gaia.HelpBase;
import com.taobao.gaia.KillTypeEnum;

/**
 * @author Administrator/lexin
 * 
 */
public class Function_reportlog_ontime_test extends FailOverBaseCase {

	public String caseName = "";

	@Test
	public void ns_report_ontime_each_interval() {
		log.info("ns_report_ontime_each_interval===> start");

		for (int i = DSINDEX; i <= DS_CLUSTER_NUM; i++)
		{
		    AppCluster csCluster = tfsGrid.getCluster(i);
		    for(int j = 0; j < csCluster.getServerList().size(); j++)
		    {
		        String serverAddr;
		        AppServer cs = csCluster.getServer(j);
		        
		        serverAddr = cs.getIp() + ":" + cs.getPort();
			    check_ns_report_on_server(serverAddr);
			}
		}
	}

	private void check_ns_report_on_server(String strServer) {
		String fmt = "yyyy-MM-dd hh:mm:ss";
		SimpleDateFormat sdf = new SimpleDateFormat(fmt);
        ArrayList<String> result = new ArrayList<String>(100);
		// execute shell command:
		String strCmd = "cat /home/admin/tfs_bin/nameserver.log | grep 'block successful'";
		strCmd += " | sed 's/\\[\\([ :0-9-]\\{19\\}\\)";
		strCmd += ".*\\(" + strServer + "\\).*/\\1/g'";
		HA.proStartBase(MASTERIP, strCmd, result);

		for(int i = 0; i < result.size(); i++)
		{  Date curDate = new Date();


            try{
             curDate = sdf.parse(result.get(i));
          }catch (ParseException e) {
                e.printStackTrace();
            }

         if( (i+1) == result.size() )
            break;
            Date nextDate = new Date();

            try{
             nextDate = sdf.parse(result.get(i+1));
          }catch (ParseException e) {
                e.printStackTrace();
            }

         assertEquals(5, (nextDate.getTime() - curDate.getTime()) / 1000);
      }

	}

	@After
	public void teardown(){
		boolean bRet = false;
		
		/* Stop all client process */
		bRet = allCmdStop();
		Assert.assertTrue(bRet);
		
		/* Clean the caseName */
		caseName = "";
	}
	
	@Before
	public void setup(){
		boolean bRet = false;
		
		/* Reset case name */
		caseName = "";

		if (!grid_started)
		{
			/* Set the failcount */
			bRet = setAllFailCnt();
			Assert.assertTrue(bRet);
			
			/* Kill the grid */
			bRet = tfsGrid.stop(KillTypeEnum.FORCEKILL, WAITTIME);
			Assert.assertTrue(bRet);

			/* Set Vip */
			bRet = migrateVip();
			Assert.assertTrue(bRet);
			
			/* Clean the log file */
			bRet = tfsGrid.clean();
			Assert.assertTrue(bRet);
			
			bRet = tfsGrid.start();
			Assert.assertTrue(bRet);
			
			/* Set failcount */
			bRet = resetAllFailCnt();
			Assert.assertTrue(bRet);
			
			grid_started = true;
		}
	}

}
