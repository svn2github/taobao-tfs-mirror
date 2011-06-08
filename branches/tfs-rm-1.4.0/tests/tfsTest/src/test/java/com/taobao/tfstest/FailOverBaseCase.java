/**
 * 
 */
package com.taobao.tfstest;

import java.util.ArrayList;
import java.util.List;

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

public class FailOverBaseCase {
	
	final ApplicationContext beanFactory = new ClassPathXmlApplicationContext("tfsServer.xml");
	final AppGrid tfsGrid 		= (AppGrid) beanFactory.getBean("tfsGrid");
	//final AppGrid slaveTfsGrid 	= (AppGrid) beanFactory.getBean("slaveTfsGrid");
	
	protected static Logger log = Logger.getLogger("TfsTest");
	
	public HelpConf conf = new HelpConf();
	public HelpHA HA = new HelpHA();
	public HelpLog Log = new HelpLog();
	public HelpProc Proc = new HelpProc();
	public HelpFile File = new HelpFile();
	
	final ApplicationContext clientFactory = new ClassPathXmlApplicationContext("tfsClient.xml");
	
	final AppServer tfsSeedClient = (AppServer) clientFactory.getBean("seedClient");
	final AppServer tfsReadClient = (AppServer) clientFactory.getBean("readClient");
	//final AppServer tfsReadClient_slave = (AppServer) clientFactory.getBean("slaveReadClient");
	
	//Define
	final public int NSINDEX = 0;
	final public int DSINDEX = 1;
	final public int FAILCOUNTNOR = 0;
	final public String NSIPA = tfsGrid.getCluster(NSINDEX).getServer(0).getIp();
	final public String NSIPB = tfsGrid.getCluster(NSINDEX).getServer(1).getIp();
	final public String NSRES = tfsGrid.getCluster(NSINDEX).getServer(0).getResName();
	final public String NSMACA = tfsGrid.getCluster(NSINDEX).getServer(0).getMacName();
	final public String NSMACB = tfsGrid.getCluster(NSINDEX).getServer(1).getMacName();
	final public String IPALIAS = tfsGrid.getCluster(NSINDEX).getServer(0).getIpAlias();
	final public String CLIENTIP = tfsSeedClient.getIp();
	final public String CLIENTCONF = tfsSeedClient.getConfname();
	
	/* Key word */
	final public String WRITEFILE        = "writeFile:";
	final public String READFILE         = "readFile:";
	final public String SAVEUNIQUEFILE   = "SaveUniqueFile:";
	final public String UNIQUE           = "Unique:";
	final public String WRITEFILESTATIS  = "write statis:";
	final public String SAVEUNIQUESTATIS = "saveUnique statis:";
	final public String UNIQUESTATIS     = "uniqueFile statis:";
	final public String READFILESTATIS   = "read statis:";
	final public String UNLINKSTATIS   	 = "unlink statis:";
	
	/* Client conf */
	final public String LOOPFLAG = "loop_flag";
	final public String FILESIZE = "size";
	final public String UNITSIZE = "unit";
	final public String READTYPE = "readType";
	final public int LOOPON = 1;
	final public int LOOPOFF = 0;
	
	final public String WRITECMD = "cd /home/admin/tfstest_new; ./tfsControlPress -f test_tfs.conf -i tfsSeed -l tfsSeed.";
	final public String READCMD = "cd /home/admin/tfstest_new; ./tfsControlPress -f test_tfs.conf -i tfsRead -l tfsRead.";
	final public String UNLINKCMD = "cd /home/admin/tfstest_new; ./tfsControlPress -f test_tfs.conf -i tfsUnlink -l tfsUnlink.";
	
	/* For scan log on client */
	final public int TAILLINE = 100;
	final public int TAILRATECOL = 12;
	final public int RUNRATECOL = 13;
	final public int TAILTPSCOL = 13;
	final public int RUNTPSCOL = 11;
	final public int SCANTIME = 120;
	
	/* Thread count on client */
	final public int HIGHTHREAD = 100;
	final public int LOWTHREAD = 10;
	final public int READTHREAD = 1;
	
	/* The write mode on client */
	final public int WRITEONLY = 1;
	final public int WRITEUNI = 2;
	final public int WRITEMIX = 3;
	final public int READ = 4;
	final public int TEMP = 8;
	final public int READSLAVE = 16;
	final public int UNLINK = 32;
	
	/* Standard */
	final public float SUCCESSRATE = 100;
	final public float HIGHRATE = (float) 99.99;
	final public float HALFRATE = 50;
	final public float FAILRATE = 0;
	final public int WAITTIME = 30;
	
	/* Vip */
	public String masterIp = NSIPA;
	
	/**
	 * 
	 * @param sec
	 */
	public void sleep(int sec)
	{
		log.debug("wait for "+sec+"s");
		try
		{
			Thread.sleep(sec*1000);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param logName
	 * @return
	 */
	public boolean writeCmd(String logName)
	{
		boolean bRet = false;
		log.info("Write command start ===>");
		bRet = Proc.proStartBase(CLIENTIP, WRITECMD + logName);
		log.info("Write command end ===>");
		return bRet;
	}
	
	/**
	 * 
	 * @param logName
	 * @return
	 */
	public boolean readCmd(String logName)
	{
		boolean bRet = false;
		log.info("Read command start ===>");
		bRet = Proc.proStartBase(CLIENTIP, READCMD + logName);
		log.info("Read command end ===>");
		return bRet;
	}
	
	/**
	 * 
	 * @param logName
	 * @return
	 */
	public boolean unlinkCmd(String logName)
	{
		boolean bRet = false;
		log.info("Unlink command start ==>");
		bRet = Proc.proStartBase(CLIENTIP, UNLINKCMD + logName);
		log.info("Unlink command end ==>");
		return bRet;
	}
	
	/**
	 * 
	 * @param iSize
	 * @return
	 */
	public boolean setSeedSize(int iSize)
	{
		boolean bRet = false;
		bRet = conf.confReplaceSingle(CLIENTIP, CLIENTCONF, FILESIZE, String.valueOf(iSize));
		return bRet;
	}
	
	public boolean setUnitSize(int iSize)
	{
		boolean bRet = false;
		bRet = conf.confReplaceSingle(CLIENTIP, CLIENTCONF, UNITSIZE, String.valueOf(iSize));
		return bRet;
	}
	
	public boolean setSeedFlag(int iFlag)
	{
		boolean bRet = false;
		bRet = conf.confReplaceSingle(CLIENTIP, CLIENTCONF, LOOPFLAG, String.valueOf(iFlag));
		return bRet;
	}
	
	/**
	 * 
	 * @param grid
	 * @param bFlag
	 * @return
	 */
	public boolean setDsConf(AppGrid grid, boolean bFlag)
	{
		boolean bRet = false;
		/* Init */
		String strIp = null;
		String strConf = null;
		String strIpAddrList = null;
		String strIpAddr = grid.getCluster(0).getServer(0).getIp();
		int iServerCnt = grid.getCluster(0).getServerList().size();
		
		if (iServerCnt == 1)
		{
			strIpAddrList = grid.getCluster(0).getServer(0).getIp() + "|" + "0.0.0.0";
		} else if (iServerCnt == 2)
		{
			strIpAddrList = grid.getCluster(0).getServer(0).getIp() + "|" + grid.getCluster(0).getServer(1).getIp();
		} else {
			log.error("Nameserver's count(" + iServerCnt + ") is not correct!!!");
			return false;
		}

		if (bFlag == true)
		{
			for (int iLoop = 1; iLoop < grid.getClusterList().size(); iLoop ++)
			{
				iServerCnt = grid.getCluster(iLoop).getServerList().size();
				for (int jLoop = 0; jLoop < iServerCnt; jLoop ++)
				{
					strIp = grid.getCluster(iLoop).getServer(jLoop).getIp();
					strConf = grid.getCluster(iLoop).getServer(jLoop).getDir() + grid.getCluster(iLoop).getServer(jLoop).getConfname();
					bRet = conf.confReplaceSingle(strIp, strConf, "ip_addr", strIpAddr);
					if (bRet == false)
					{
						return bRet;
					}
					bRet = conf.confReplaceSingle(strIp, strConf, "ip_addr_list", strIpAddrList);
					if (bRet == false)
					{
						return bRet;
					}
				}
			}
			bFlag = false;
		}
		return bRet;
	}

	/**
	 * 
	 * @param strIp
	 * @return
	 */
	public boolean resetFailCount(String strIp)
	{
		boolean bRet = false;
		String macName = null;
		if (strIp == NSIPA)
		{
			macName = NSMACA;
		} else if (strIp == NSIPB)
		{
			macName = NSMACB;
		} else {
			log.error("IP address(" + strIp + ") is not a ns's ip!!!");
			return bRet;
		}
		bRet = HA.setFailCountBase(strIp, NSRES, macName, FAILCOUNTNOR);
		return bRet;
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean resetAllFailCnt()
	{
		log.info("Reset failcount start ===>");
		boolean bRet = resetFailCount(NSIPA);
		if (bRet == false) return bRet;
		
		bRet = resetFailCount(NSIPB);
		log.info("Reset failcount end ===>");
		return bRet;	
	}
	
	
	
	
	/**
	 * 
	 * @return
	 */
	public boolean migrateVip()
	{
		boolean bRet = false;
		log.info("Migrate vip start ===>");
		bRet = HA.setVipMigrateBase(NSIPA, IPALIAS, NSMACA);
		log.info("Migrate vip end ===>");
		return bRet;	
	}
	
	/**
	 * 
	 * @param tarIp
	 * @param fileName
	 * @param keyWord
	 * @return
	 */
	public float getRateEnd(String tarIp, String fileName, String keyWord)
	{
		boolean bRet = false;
		float fRet = -1;
		ArrayList<String> filter = new ArrayList<String>();
		ArrayList<Float> context = new ArrayList<Float>();
		filter.add("%");
		filter.add(",");
		filter.add("rate:");
		bRet = Log.scanTailFloat(tarIp, fileName, keyWord, TAILLINE, TAILRATECOL, filter, context);
		if ((bRet == false) || (context.size() != 1))
		{
			return fRet;
		}
		fRet = context.get(0);
		return fRet;	
	}
	
	/**
	 * 
	 * @param tarIp
	 * @param fileName
	 * @param keyWord
	 * @return
	 */
	public float getTPSEnd(String tarIp, String fileName, String keyWord)
	{
		boolean bRet = false;
		float fRet = -1;
		ArrayList<String> filter = new ArrayList<String>();
		ArrayList<Float> context = new ArrayList<Float>();
		filter.add(",");
		filter.add("TPS:");
		bRet = Log.scanTailFloat(tarIp, fileName, keyWord, TAILLINE, TAILTPSCOL, filter, context);
		if ((bRet == false) || (context.size() != 1))
		{
			return fRet;
		}
		fRet = context.get(0);
		return fRet;	
	}
	
	/**
	 * 
	 * @param tarIp
	 * @param fileName
	 * @param keyWord
	 * @return
	 */
	public float getRateRun(String tarIp, String fileName, String keyWord){
		boolean bRet = false;
		float fRet = -1;
		int iFailCount = 0;
		ArrayList<String> filter = new ArrayList<String>();
		ArrayList<Float> context = new ArrayList<Float>();
		filter.add(",");
		filter.add("sum_failed:");
		bRet = Log.scanAllFloat(tarIp, fileName, keyWord, RUNRATECOL, filter, context);
		if ((bRet == false) || (context.size() <= 1))
		{
			return fRet;
		}
		iFailCount = (int) (context.get(context.size() - 1) - context.get(0));
		fRet = (context.size() * 1000 - iFailCount) / (context.size() * 10); 
		return fRet;	
	}
	
	/**
	 * 
	 * @param tarIp
	 * @param fileName
	 * @param keyWord
	 * @return
	 */
	public float getTPSRun(String tarIp, String fileName, String keyWord){
		boolean bRet = false;
		float fRet = -1;
		float fTps = 0;
		ArrayList<String> filter = new ArrayList<String>();
		ArrayList<Float> context = new ArrayList<Float>();
		filter.add(",");
		filter.add("TPS(T/s):");
		bRet = Log.scanAllFloat(tarIp, fileName, keyWord, RUNTPSCOL, filter, context);
		if ((bRet == false) || (context.size() <= 1))
		{
			return fRet;
		}
		for (int iLoop = 0; iLoop < context.size(); iLoop ++)
		{
			fTps += context.get(iLoop);
		}
		fRet = fTps / context.size();
		return fRet;	
	}
	
	public boolean setClientThread(int iThreadCount){
		boolean bRet = false;
		bRet = conf.confReplaceSingle(CLIENTIP, CLIENTCONF, "thread_count", String.valueOf(iThreadCount));
		if (bRet == false)
		{
			return bRet;
		}
		return bRet;
	}
	
	public boolean setClientMode(int iMode){
		boolean bRet = false;
		bRet = conf.confReplaceSingle(CLIENTIP, CLIENTCONF, "seedSetMode", String.valueOf(iMode));
		if (bRet == false)
		{
			return bRet;
		}
		return bRet;
	}
	
	public boolean checkRateEnd(float fStd, int iMode){
		boolean bRet = false;
		float fRet = 0;
		if ((iMode & WRITEONLY) != 0)
		{
			fRet = getRateEnd(CLIENTIP, tfsSeedClient.getLogs(), WRITEFILESTATIS);
			if (fRet == -1)
			{
				return bRet;
			}
			if (fRet < fStd)
			{
				log.error("Write success rate(" + fRet + "%) if lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("Write success rate(" + fRet + "%) if higher than " + fStd + "% !!!");
			}
		}
		if ((iMode & WRITEUNI) != 0)
		{
			fRet = getRateEnd(CLIENTIP, tfsSeedClient.getLogs(), SAVEUNIQUESTATIS);
			if (fRet == -1)
			{
				return bRet;
			}
			if (fRet < fStd)
			{
				log.error("SaveUnique success rate(" + fRet + "%) if lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("SaveUnique success rate(" + fRet + "%) if higher than " + fStd + "% !!!");
			}
			fRet = getRateEnd(CLIENTIP, tfsSeedClient.getLogs(), UNIQUESTATIS);
			if (fRet == -1)
			{
				return bRet;
			}
			if (fRet < fStd)
			{
				log.error("Unique success rate(" + fRet + "%) if lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("Unique success rate(" + fRet + "%) if higher than " + fStd + "% !!!");
			}
		}
		if ((iMode & READ) != 0)
		{			
			fRet = getRateEnd(CLIENTIP, tfsReadClient.getLogs(), READFILESTATIS);
			if (fRet == -1)
			{
				return bRet;
			}	
			if (fRet < fStd)
			{
				log.error("Read success rate(" + fRet + "%) if lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("Read success rate(" + fRet + "%) if higher than " + fStd + "% !!!");
			}
		}
		if ((iMode & READSLAVE) != 0)
		{			
//			fRet = getRateEnd(CLIENTIP, tfsReadClient_slave.getLogs(), READFILESTATIS);
//			if (fRet == -1)
//			{
//				return bRet;
//			}	
//			if (fRet < fStd)
//			{
//				log.error("Read success rate(" + fRet + "%) if lower than " + fStd + "% !!!");
//				return bRet;
//			} else {
//				log.info("Read_slave success rate(" + fRet + "%) if higher than " + fStd + "% !!!");
//			}
		}
		
		if ((iMode & UNLINK) != 0)
		{			
			fRet = getRateEnd(CLIENTIP, tfsReadClient.getLogs(), UNLINKSTATIS);
			if (fRet == -1)
			{
				return bRet;
			}	
			if (fRet < fStd)
			{
				log.error("Read success rate(" + fRet + "%) if lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("Read_slave success rate(" + fRet + "%) if higher than " + fStd + "% !!!");
			}
		}
		
		return true;
	}
	
	public boolean checkRateEnd(float fStd, int iMode, String logName){
		boolean bRet = false;
		float fRet = 0;
		if ((iMode & WRITEONLY) != 0)
		{
			fRet = getRateEnd(CLIENTIP, "tfsSeedLarge." + logName, WRITEFILESTATIS);
			if (fRet == -1)
			{
				return bRet;
			}
			if (fRet < fStd)
			{
				log.error("Write success rate(" + fRet + "%) if lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("Write success rate(" + fRet + "%) if higher than " + fStd + "% !!!");
			}
		}

		if ((iMode & READ) != 0)
		{			
			fRet = getRateEnd(CLIENTIP, "tfsRead." + logName, READFILESTATIS);
			if (fRet == -1)
			{
				return bRet;
			}	
			if (fRet < fStd)
			{
				log.error("Read success rate(" + fRet + "%) if lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("Read success rate(" + fRet + "%) if higher than " + fStd + "% !!!");
			}
		}
		
		if ((iMode & UNLINK) != 0)
		{			
			fRet = getRateEnd(CLIENTIP, "tfsUnlink." + logName, UNLINKSTATIS);
			if (fRet == -1)
			{
				return bRet;
			}	
			if (fRet < fStd)
			{
				log.error("Read success rate(" + fRet + "%) if lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("Read_slave success rate(" + fRet + "%) if higher than " + fStd + "% !!!");
			}
		}
		
		return true;
	}
	
	public boolean checkRateRunByLog(float fStd, int iMode, String logFile){
		boolean bRet = false;
		float fRet = 0;
		if ((iMode & WRITEONLY) != 0)
		{
			fRet = getRateRun(CLIENTIP, logFile, WRITEFILESTATIS);
			if (fRet == -1)
			{
				return bRet;
			}
			if (fRet < fStd)
			{
				log.error("Write success rate(" + fRet + "%) is lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("Write success rate(" + fRet + "%) is higher than " + fStd + "%");
			}
		}
		if ((iMode & WRITEUNI) != 0)
		{
			fRet = getRateRun(CLIENTIP, logFile, SAVEUNIQUESTATIS);
			if (fRet == -1)
			{
				return bRet;
			}
			if (fRet < fStd)
			{
				log.error("SaveUnique success rate(" + fRet + "%) is lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("SaveUnique success rate(" + fRet + "%) is higher than " + fStd + "%");
			}
			fRet = getRateRun(CLIENTIP, logFile, UNIQUESTATIS);
			if (fRet == -1)
			{
				return bRet;
			}
			if (fRet < fStd)
			{
				log.error("Unique success rate(" + fRet + "%) is lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("Unique success rate(" + fRet + "%) is higher than " + fStd + "%");
			}
		}
		if ((iMode & READ) != 0)
		{			
			fRet = getRateRun(CLIENTIP, logFile, READFILESTATIS);
			if (fRet == -1)
			{
				return bRet;
			}	
			if (fRet < fStd)
			{
				log.error("Read success rate(" + fRet + "%) is lower than " + fStd + "% !!!");
				return bRet;
			} else {
				log.info("Read success rate(" + fRet + "%) is higher than " + fStd + "%");
			}
		}
		
		return true;
	}
	
	public boolean checkRateRun(float fStd, int iMode, String caseName)
	{
		boolean bRet = false;
		String sorLog = tfsSeedClient.getLogs();
		String tarLog = createClientLog(caseName, TEMP);
		String startCmd = "tail -f " + sorLog + " > " + tarLog;
		bRet = Proc.proStartBack(CLIENTIP, startCmd);
		if (bRet == false)
		{
			return bRet;
		}
		
		/* Wait */
		sleep(SCANTIME);
		
		bRet = Proc.proStopByCmd(CLIENTIP, startCmd);
		if (bRet == false)
		{
			return bRet;
		}
		
		/* check the result */
		checkRateRunByLog( fStd,  iMode,  tarLog);
		if (bRet == false)
		{
			return bRet;
		}
		
		return bRet;
	}
	
	public boolean checkWrittenFile(float fStd, String caseName){
		boolean bRet = false;
		int iRet = -1;
		
		/* Set the thread on client */
		bRet = setClientThread(READTHREAD);
		if (bRet == false)
		{
			return bRet;
		}
		
		/* Start the client process */
		bRet = tfsReadClient.start();
		if (bRet == false)
		{
			return bRet;
		}
		
		/* Monitor the client process */
		for ( ; ; )
		{
			iRet = Proc.proMonitorBase(CLIENTIP, tfsReadClient.getStartcmd());
			if (iRet <= 0)
			{
				if (iRet < 0) bRet = false;
				break;
			}
			sleep(60);
		}
		
		if (bRet == false) return bRet;
		/* Check the Result */
		bRet = checkRateEnd(fStd, READ);		
		return bRet;
	} 
	
	public boolean mvSeedFile(String suffix)
	{
		boolean bRet = false;
		String seedFile = "/home/admin/tfstest_new/tfsseed_file_list.txt";
		bRet = File.fileCopy(CLIENTIP, seedFile, seedFile + suffix);
		if (bRet == false) return bRet;
		bRet = File.fileDel(CLIENTIP, seedFile);
		return bRet;
	}
	
	public boolean mvLogFile(String logName, String suffix)
	{
		boolean bRet = false;
		/* Check the log file is exist or not */
		bRet = File.checkFileBase(CLIENTIP, logName);
		if (bRet == true)
		{
			bRet = File.fileCopy(CLIENTIP, logName, logName + "." + suffix);
			if (bRet == false)
			{
				return bRet;
			}
			bRet = File.fileDel(CLIENTIP, logName);
			if (bRet == false) return bRet;
		} else {
			bRet = true;
		}
		return bRet;
	}
	
	public boolean mvLog(String suffix)
	{
		boolean bRet = false;
		bRet = mvLogFile(tfsSeedClient.getLogs(), suffix);
		if (bRet == false) return bRet;
		
		bRet = mvLogFile(tfsReadClient.getLogs(), suffix);
		if (bRet == false) return bRet;
		
//		bRet = mvLogFile(tfsReadClient_slave.getLogs(), suffix);
//		if (bRet == false) return bRet;
		
		return bRet;
	}
	
	public String createClientLog(String suffix, int iMode)
	{
		if (((iMode & WRITEONLY) != 0) || ((iMode & WRITEUNI) != 0))
		{
			return (tfsSeedClient.getLogs()+ "." + suffix);
		} else if ((iMode & READ) != 0){
			return (tfsReadClient.getLogs()+ "." + suffix);
		} else if ((iMode & TEMP) != 0){
			return (tfsSeedClient.getLogs() + ".temp." + suffix);
		//} else if ((iMode & READSLAVE) != 0){
//			return (tfsReadClient_slave.getLogs() + ".slave." + suffix);
		} else {
			log.error("Mode(" + iMode + ") error");
			return ("");
		}
	}
	
	public boolean killMasterNs()
	{
		boolean bRet = false;
		String tempIp = "";
		String tempMaster = "";
		List<AppServer> listNs = tfsGrid.getCluster(NSINDEX).getServerList();

		/* Find the master ns */
		if (listNs.get(0).getIp().equals(masterIp))
		{
			bRet = listNs.get(0).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false) return bRet;
			tempIp = listNs.get(0).getIp();
			tempMaster = listNs.get(1).getIp();
		} else {
			bRet = listNs.get(1).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false) return bRet;
			tempIp = listNs.get(1).getIp();
			tempMaster = listNs.get(0).getIp();
		}
		
		/* Wait for vip migrate */
		sleep (5);
		
		/* Check vip */
		bRet = HA.chkVipBase(tempIp);
		if (bRet == true)
		{
			log.error("VIP is not migrate yet!!!");
			return false;
		}
		
		/* Reset the failcount */
		bRet = resetFailCount(tempIp);
		if (bRet == false) return bRet;
		
		/* Set the new vip */
		masterIp = tempMaster;
		
		return bRet;
	}
	
	public boolean killSlaveNs()
	{
		boolean bRet = false;
		List<AppServer> listNs = tfsGrid.getCluster(NSINDEX).getServerList();

		/* Find the master ns */
		if (listNs.get(0).getIp().equals(masterIp))
		{
			bRet = listNs.get(0).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false) return bRet;
		} else {
			bRet = listNs.get(1).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false) return bRet;
		}
		
		/* Wait for vip migrate */
		sleep (5);
		
		/* Check vip */
		bRet = HA.chkVipBase(masterIp);
		if (bRet == true)
		{
			log.error("VIP is not on master ns yet!!!");
			return false;
		}
		
		return bRet;
	}
	
	public boolean chkBlockCnt(int iTimes, int iBlockCnt)
	{
		boolean bRet = false;
		String vip = tfsGrid.getCluster(NSINDEX).getServer(0).getVip();
		int port = tfsGrid.getCluster(NSINDEX).getServer(0).getPort();
		ArrayList<String> listOut = new ArrayList<String>();
		String cmd = "cd /home/admin/tfs/lib;" + "./ssm -s " + vip + ":" + port + " -i " + "\\\"block\\\" | grep \\\"" + iBlockCnt + "$\\\" | wc -l";
		//String cmd = "cd /home/admin/tfs/lib; ls";
		
		for (int iLoop = 0; iLoop < iTimes; iLoop ++)
		{
			bRet = Proc.cmdOutBase(NSIPA, cmd, null, 1, null, listOut);
			if (bRet == false) return bRet;
			
			try{
				int temp = Integer.valueOf(listOut.get(listOut.size() - 1));			
				if (temp == 0)
				{
					bRet = true;
					break;
				}
				
//				for(int jLoop = 0;jLoop < listOut.size(); jLoop ++)
//				{
//					log.info("Result: " + listOut.get(jLoop));
//				}
//				break;
			} catch (Exception e){
				e.printStackTrace();
				bRet = false;
				break;
			}
			sleep(1);
		}
		return bRet;
	}
	
}
