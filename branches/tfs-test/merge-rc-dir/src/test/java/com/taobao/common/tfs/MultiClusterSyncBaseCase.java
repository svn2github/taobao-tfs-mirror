/**
 * 
 */
package com.taobao.common.tfs;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Date;

import com.taobao.gaia.AppCluster;
import com.taobao.gaia.AppGrid;
import com.taobao.gaia.AppServer;
import com.taobao.gaia.HelpConf;
import com.taobao.gaia.HelpFile;
import com.taobao.gaia.HelpHA;
import com.taobao.gaia.HelpLog;
import com.taobao.gaia.HelpProc;
import com.taobao.gaia.KillTypeEnum;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class MultiClusterSyncBaseCase extends NativeTfsBaseCase {
	
	boolean grid_started = false;
	protected final ApplicationContext beanFactory = new ClassPathXmlApplicationContext("multiClusterSyncServer.xml");
	protected final AppGrid tfsGrid 		= (AppGrid) beanFactory.getBean("tfsGrid");
	protected final AppGrid tfsGrid2 		= (AppGrid) beanFactory.getBean("tfsGrid2");
	//final AppGrid tfsGrid3 		= (AppGrid) beanFactory.getBean("tfsGrid3");
	protected final AppGrid tfsGrid3 		= null;
	
	final ApplicationContext clientFactory = new ClassPathXmlApplicationContext("tfsClient.xml");
	
	final AppServer tfsSeedClient = (AppServer) clientFactory.getBean("seedClient");
	protected final AppServer tfsReadClient = (AppServer) clientFactory.getBean("readClient");
	final AppServer tfsUnlinkClient = (AppServer) clientFactory.getBean("unlinkClient");
	
	//Define
	final public int NSINDEX = 0;
	final public int DSINDEX = 1;
	final public int DS_CLUSTER_NUM = tfsGrid.getClusterList().size() - 1;
	final public String NSVIP = tfsGrid.getCluster(NSINDEX).getServer(0).getVip();
	final public String MASTERIP = tfsGrid.getCluster(NSINDEX).getServer(0).getIp();
	final public String SLAVEIP = tfsGrid.getCluster(NSINDEX).getServer(1).getIp();
	final public int NSPORT = tfsGrid.getCluster(NSINDEX).getServer(0).getPort();
	final public String NSCONF = tfsGrid.getCluster(NSINDEX).getServer(0).getConfname();	
	final public String CLIENTIP = tfsSeedClient.getIp();
	final public String CLIENTCONF = tfsSeedClient.getConfname();
	
	/* Path */
	final public String TFS_HOME        = tfsGrid.getCluster(NSINDEX).getServer(0).getDir(); 
	final public String TEST_HOME       = tfsSeedClient.getDir(); 
	final public String TFS_LOG_HOME    = TFS_HOME + "/logs";
	final public String TFS_BIN_HOME    = tfsGrid.getCluster(NSINDEX).getServer(0).getBinPath();
	final public String NS_LOG_NAME     = TFS_LOG_HOME + "/nameserver.log";

	/* Client conf */
	final public String FILE_LIST_NAME = "filelist_name";
	final public String SEED_FILE_LIST_NAME = "tfsseed_file_list.txt";
	final public String UNLINKED_FILE_LIST_NAME = "tfsunlinked_file_list.txt";	

  /* file status, used in check_sync */
  final public int STATUS_NORMAL = 0;
  final public int STATUS_DELETED = 1;

  /* Standard */
	final public int CHK_TIME = 500;

	final public String VIPETHNAME = "eth0:1";
	
	public boolean setReadFileList(String filelist_name)
	{
		boolean bRet = false;
		bRet = conf.confReplaceSingleByPart(CLIENTIP, CLIENTCONF, "tfsread", FILE_LIST_NAME, filelist_name);
		return bRet;
	}

	public boolean mvUnlinkFile()
	{
		boolean bRet = false;
		String UnlinkFile = TEST_HOME + "/tfsunlinked_file_list.txt";
		bRet = File.fileCopy(CLIENTIP, UnlinkFile, UnlinkFile + caseName);
		if (bRet == false) return bRet;
		bRet = File.fileDel(CLIENTIP, UnlinkFile);
		return bRet;
	}
	
	/**
	 * @author lexin 
	 * @param srcClusterAddr
	 * @param destClusterAddr
	 * @param chkVal
	 * @return
	 */
	public boolean check_sync(String srcClusterAddr, String destClusterAddr, int chkVal) 
	{
		log.info("Start to check_sync: " + srcClusterAddr + " ==> " + destClusterAddr);
		ArrayList<String> result = new ArrayList<String>(500);

		String strCmd = "cat ";
		strCmd += TEST_HOME
				+ "/tfsseed_file_list.txt | sed 's/\\(.*\\) \\(.*\\)/\\1/g'";
		boolean bRet = false;

		bRet = Proc.proStartBase(CLIENTIP, strCmd, result);
		if (bRet == false)
		{
			log.error("Get write file list on cluster " + destClusterAddr + " failure!");
			return bRet;
		}
		for (int i = 0; i < result.size(); i++) {
			ArrayList<String> chkResult = new ArrayList<String>(20);
			strCmd = TEST_HOME + "/tfstool -s ";
			strCmd += destClusterAddr;
			strCmd += " -n -i \\\"stat ";
			strCmd += result.get(i) + "\\\"";
			log.info("Executed command is:" + strCmd);
			bRet = Proc.cmdOutBase(CLIENTIP, strCmd, "STATUS", 2, null, chkResult);
			if (bRet == false || chkResult.size() <= 0)
			{
				log.error("Check file on cluster " + destClusterAddr + " failure!!!!");
				return bRet;
			}
			log.info("-------------->Executed tfstool result is: " + chkResult.get(0));
			// if the deleted file has been compacted, will return 8025
			if (chkResult.get(0).contains("8025") && STATUS_DELETED == chkVal)
				return true;
			else
				assertEquals(chkVal, Integer.parseInt(chkResult.get(0)));
		}
		return bRet;
	}
	
	/**
	 * @author mingyan 
	 * @param tfsAppGrid
	 * @param exist
	 * @return
	 */
	public boolean chkSecondQueue(AppGrid tfsAppGrid, boolean exist) 
	{
		boolean bRet = false;
		AppServer cs = tfsAppGrid.getCluster(DSINDEX).getServer(0);
		String targetIp = cs.getIp();
		String dir = cs.getDir() + "/dataserver_1/mirror/secondqueue";
		
		/* wait for secondqueue created */
		int check_times = CHK_TIME;
		for (int i = 0; i < check_times; i++)
		{
			bRet = File.checkDirBase(targetIp, dir);
			if (true == exist)
			{
				if (true == bRet)
					return bRet;			
			}
			else
			{
				if (false == bRet)
					return true;
			}
		}
		return false;
	}
	
	public boolean restoreOneDs(AppGrid tfsAppGrid) {
		boolean bRet = false;
		log.info("Restore one ds start ===>");
		
		/* restore bin */
		String src_bin_file = tfsAppGrid.getCluster(DSINDEX).getServer(0).getBinPath() + "/dataserver132";
		String dest_bin_file = tfsAppGrid.getCluster(DSINDEX).getServer(0).getBinPath() + "/dataserver";
		
		bRet = File.fileCopy(tfsAppGrid.getCluster(DSINDEX).getServer(0).getIp(), src_bin_file, dest_bin_file);
		if (false == bRet)
			return bRet;
		
		/* replace conf */
		String src_conf_file = tfsAppGrid.getCluster(DSINDEX).getServer(0).getDir() + "/conf/ds_2.conf.132";
		String dest_conf_file = tfsAppGrid.getCluster(DSINDEX).getServer(0).getDir() + "/conf/ds_2.conf";

		bRet = File.fileCopy(tfsAppGrid.getCluster(DSINDEX).getServer(0).getIp(), src_conf_file, dest_conf_file);
		
		log.info("Restore one ds end ===>");
		return bRet;
	}
	
	public boolean replaceOneDs(AppGrid tfsAppGrid) {
		boolean bRet = false;
		log.info("Replace one ds start ===>");
		
		/* replace bin */
		String bak_bin_file = tfsAppGrid.getCluster(DSINDEX).getServer(0).getBinPath() + "/dataserver132";
		String src_bin_file = tfsAppGrid.getCluster(DSINDEX).getServer(0).getBinPath() + "/dataserver14";
		String dest_bin_file = tfsAppGrid.getCluster(DSINDEX).getServer(0).getBinPath() + "/dataserver";
	
		bRet = File.fileCopy(tfsAppGrid.getCluster(DSINDEX).getServer(0).getIp(), dest_bin_file, bak_bin_file);
		if (false == bRet)
			return bRet;
		bRet = File.fileCopy(tfsAppGrid.getCluster(DSINDEX).getServer(0).getIp(), src_bin_file, dest_bin_file);
		if (false == bRet)
			return bRet;
		
		/* replace conf */
		String bak_conf_file = tfsAppGrid.getCluster(DSINDEX).getServer(0).getDir() + "/conf/ds_2.conf.132";
		String src_conf_file = tfsAppGrid.getCluster(DSINDEX).getServer(0).getDir() + "/conf/ds_2.conf.14";
		String dest_conf_file = tfsAppGrid.getCluster(DSINDEX).getServer(0).getDir() + "/conf/ds_2.conf";
	
		bRet = File.fileCopy(tfsAppGrid.getCluster(DSINDEX).getServer(0).getIp(), dest_conf_file, bak_conf_file);
		if (false == bRet)
			return bRet;
		bRet = File.fileCopy(tfsAppGrid.getCluster(DSINDEX).getServer(0).getIp(), src_conf_file, dest_conf_file);
		
		log.info("Replace one ds end ===>");
		return bRet;
	}

	public boolean renameFileQueue(AppGrid tfsAppGrid) {
		boolean bRet = false;
		log.info("Rename file queue start ===>");
		String bin_dir = tfsAppGrid.getCluster(DSINDEX).getServer(0).getBinPath();
		String conf_name = tfsAppGrid.getCluster(DSINDEX).getServer(0).getDir() + tfsAppGrid.getCluster(DSINDEX).getServer(0).getConfname();
		String startCmd = bin_dir + "/filequeue_rename -f " + conf_name + " -i 1-4";
		bRet = Proc.proStartBase(tfsAppGrid.getCluster(DSINDEX).getServer(0).getIp(),
				startCmd);
		
		log.info("Rename file queue end ===>");
		return bRet;
	}

}
