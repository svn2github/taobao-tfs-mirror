/**
 * 
 */
package com.taobao.tfstest;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Date;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.zip.CRC32;
import java.io.*; 

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

public class RcBaseCase {
	
	boolean grid_started = false;

	final ApplicationContext rcBeanFactory = new ClassPathXmlApplicationContext("rcServer.xml");
	final AppGrid rcGrid 		= (AppGrid) rcBeanFactory.getBean("rcGrid");
	
	protected Logger log = Logger.getLogger("TfsTest");
	public HelpProc Proc = new HelpProc();
	
	//Define
	final public int RCINDEX = 0;
	final public int RCNSINDEX = 1;
        final public String MYSQLADDR = "10.232.36.204";

	/* Other */
	public String caseName = "";
        final public int WAITTIME = 30;
	
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
	
  public boolean killOneRc(int index)
  {
    boolean bRet = false;
    log.info("Kill one rc start ===>");
    AppServer cs = rcGrid.getCluster(RCINDEX).getServer(index);
    bRet = cs.stop(KillTypeEnum.NORMALKILL, WAITTIME);
    log.info("Kill one rc end ===>");
    return bRet;
  }

  public boolean startOneRc(int index)
  {
    boolean bRet = false;
    log.info("start one rc start ===>");
    AppServer cs = rcGrid.getCluster(RCINDEX).getServer(index);
    bRet = cs.start();
    log.info("start one rc end ===>");
    return bRet;
  }
	
  public long getCurrentQuote(String appKey)
  {
    long iRet = -1;
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh cur_quote " + appKey;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    if (bRet == false) return -1;
    try{
      iRet = Long.valueOf(listOut.get(listOut.size() - 1));			
      if (iRet > 0)
      {
        bRet = true;
        return iRet;
      }
    } catch (Exception e){
    	e.printStackTrace();
    }
    return iRet;
  }

  public long getMaxQuote(String appKey)
  {
    long iRet = -1;
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh max_quote " + appKey;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    if (bRet == false) return -1;
    try{
      iRet = Long.valueOf(listOut.get(listOut.size() - 1));			
      if (iRet > 0)
      {
        bRet = true;
        return iRet;
      }
    } catch (Exception e){
    	e.printStackTrace();
    }
    return iRet;
  }

  public void resetCurQuote(String appKey)
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh reset " + appKey;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public void modifyMaxQuote(String appKey, long newQuote)
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh mod_quote " + appKey + " " + newQuote;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public void setGroupPermission(String appKey, int mode)
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh set_group_mode " + appKey + " " + mode;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public void setClusterPermission(String appKey, int mode)
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh set_cluster_mode " + appKey + " " + mode;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public void setClusterPermission(String appKey, int index, int mode)
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String nsIp = rcGrid.getCluster(RCNSINDEX).getServer(index).getIp();
    int port = rcGrid.getCluster(RCNSINDEX).getServer(index).getPort();
    String nsAddr = nsIp + ":" + port;
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh set_cluster_mode " + appKey + " " + mode + " \"" + nsAddr + "\"";
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public int getClusterEx(String nsAddr)
  {
    int clusterIndex = -1;
    for (int i = 0; i < 3; i++)
    {
      String nsIp = rcGrid.getCluster(RCNSINDEX).getServer(i).getVip();
      int port = rcGrid.getCluster(RCNSINDEX).getServer(i).getPort();
      String tmpNsAddr = nsIp + ":" + port;
      if (nsAddr == tmpNsAddr)
      {
        clusterIndex = i;
        break;
      }
    }
    return clusterIndex;
  }
 
  public int getClusterIndex(String tfsname)
  {
    int iRet = -1;
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh get_addr " + tfsname;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    if (bRet == false) return -1;
    try{
      String result = listOut.get(listOut.size() - 1);			
      if (result.length() > 0)
      {
        iRet = getClusterEx(result);
      }
    } catch (Exception e){
      e.printStackTrace();
    }
    return iRet;
  }

  public void addNewTfsCluster(String appKey, int index, String clusterName) 
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String nsIp = rcGrid.getCluster(RCNSINDEX).getServer(index).getVip();
    int port = rcGrid.getCluster(RCNSINDEX).getServer(index).getPort();
    String nsAddr = nsIp + ":" + port;
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh add_cluster " + appKey + " " + clusterName + " " + nsAddr;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public void removeTfsCluster(String appKey, int index) 
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String nsIp = rcGrid.getCluster(RCNSINDEX).getServer(index).getVip();
    int port = rcGrid.getCluster(RCNSINDEX).getServer(index).getPort();
    String nsAddr = nsIp + ":" + port;
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh rm_cluster " + appKey + " " + nsAddr;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public void addOneRc(int index, int status) 
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String rcIp = rcGrid.getCluster(RCINDEX).getServer(index).getIp();
    int port = rcGrid.getCluster(RCINDEX).getServer(index).getPort();
    String rcAddr = rcIp + ":" + port;
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh add_rc " + rcAddr + " " + status;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public void removeOneRc(int index) 
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String cmd;
    String rcIp = rcGrid.getCluster(RCINDEX).getServer(index).getIp();
    int port = rcGrid.getCluster(RCINDEX).getServer(index).getPort();
    String rcAddr = rcIp + ":" + port;
    cmd = "sh /home/admin/get_mysql_data.sh rm_rc " + rcAddr;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public void modifyRc(int oldIndex, int newIndex) 
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String oldRcIp = rcGrid.getCluster(RCINDEX).getServer(oldIndex).getIp();
    int oldPort = rcGrid.getCluster(RCINDEX).getServer(oldIndex).getPort();
    String oldRcAddr = oldRcIp + ":" + oldPort;
    String newRcIp = rcGrid.getCluster(RCINDEX).getServer(newIndex).getIp();
    int newPort = rcGrid.getCluster(RCINDEX).getServer(newIndex).getPort();
    String newRcAddr = newRcIp + ":" + newPort;
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh mod_rc " + oldRcAddr + " " + newRcAddr;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public void setRcMode(int status) 
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh set_rc_mode " + status;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }

  public void setRcMode(int index, int status) 
  {
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>(); 
    String rcIp = rcGrid.getCluster(RCINDEX).getServer(index).getIp();
    int port = rcGrid.getCluster(RCINDEX).getServer(index).getPort();
    String rcAddr = rcIp + ":" + port;
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh set_rc_mode " + status + " " + rcAddr;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
  }


  public boolean checkSessionId(String sessionId){
    long lRet = -1;
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>();
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh get_cache_size '" + sessionId + "'";
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    if(bRet){
      System.out.println("+++++++++++++:" + listOut.size());
      bRet = listOut.size() == 1;
    }
    return bRet;
  }

  public boolean setDuplicateStatus(String appKey, int status){
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>();
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh set_duplicate_status " + appKey + " " + status;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    return bRet;
  }

  public boolean changeDuplicateServerAddr(int clusterRackID, String duplicateServerAddr){
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>();
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh change_duplicate_server_addr " + clusterRackID + " '" + duplicateServerAddr + "'";
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    return bRet;
  }

  public long getUsedCapacity(String appKey){
    long lRet = -1;
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>();
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh get_used_capacity '" + appKey + "'";
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    if(bRet){
      lRet = Long.valueOf(listOut.get(0));
    }
    return lRet;
  }

  public long getFileCount(String appKey){
    long lRet = -1;
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>();
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh get_file_count '" + appKey + "'";
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    if(bRet){
      lRet = Long.valueOf(listOut.get(0));
    }
    return lRet;
  }

  //get OPER_TIMES from t_session_stat
  public long getOperTimes(String sessionId, int operType){
    long lRet = -1;
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>();
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh get_oper_times '" + sessionId + "' " + operType;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    if(bRet){
      lRet = Long.valueOf(listOut.get(0));
    }
    return lRet;
  }

  //get SUCC_TIMES from t_session_stat
  public long getSuccTimes(String sessionId, int operType){
    long lRet = -1;
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>();
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh get_succ_times '" + sessionId + "' " + operType;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    if(bRet){
      lRet = Long.valueOf(listOut.get(0));
    }
    return lRet;
  }

  //get FILE_SIZE from t_session_stat
  public long getFileSize(String sessionId, int operType){
    long lRet = -1;
    boolean bRet = false;
    ArrayList<String> listOut = new ArrayList<String>();
    String cmd;
    cmd = "sh /home/admin/get_mysql_data.sh get_file_size '" + sessionId + "' " + operType;
    bRet = Proc.cmdOutBase(MYSQLADDR, cmd, null, 1, null, listOut);
    if(bRet){
      lRet = Long.valueOf(listOut.get(0));
    }
    return lRet;
  }

  protected int getCrc(String fileName){
    try{
      FileInputStream input = new FileInputStream(fileName);
      int readLength;
      byte[] data = new byte[102400];
      CRC32 crc = new CRC32();
      crc.reset();
      while ((readLength = input.read(data, 0, 102400)) > 0){
        crc.update(data, 0, readLength);
      }
      input.close();
      System.out.println(" file name crc " + fileName + " c " + crc.getValue());
      return (int)crc.getValue();
    }
    catch (Exception e){
      e.printStackTrace();
    }
    return 0;
  }

  protected static boolean createFile(String filePath, long size) {
    boolean ret = true;
    try
    {
      RandomAccessFile f = new RandomAccessFile(filePath, "rw");
      f.setLength(size);
    } 
    catch (Exception e) 
    {
      ret = false;
      e.printStackTrace();
    }
    return ret;
  }
}
