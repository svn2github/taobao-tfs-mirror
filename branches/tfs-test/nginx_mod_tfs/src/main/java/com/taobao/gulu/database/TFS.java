package com.taobao.gulu.database;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Random;

import junit.framework.TestCase;
import net.sf.json.JSONArray;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.impl.FSName;
import com.taobao.common.tfs.namemeta.FileMetaInfo;
import com.taobao.common.tfs.packet.FileInfo;
//import org.json.JSONArray;


public class TFS {
	public static final Log log = LogFactory.getLog(TestCase.class);
	private String name_server_address = "0.0.0.0:00000";
	private String tfs_rc_server_address = "0.0.0.0:0000";
	private String tfs_app_key = "";
	private long appid = 0;
	private long uid = 0;
	private long userId=0;
	private int timeOut = 8000;
	private int batchCount = 2;
	private DefaultTfsManager tfsManager = new DefaultTfsManager();
	private RandomAccessFile file;
	
	
	// private TfsBaseCase tfsCom = new TfsBaseCase();

	
	
	public String getName_server_address() {
		return name_server_address;
	}

	public int getTimeOut() {
		return timeOut;
	}

	public void setTimeOut(int timeOut) {
		this.timeOut = timeOut;
	}

	public int getBatchCount() {
		return batchCount;
	}

	public void setBatchCount(int batchCount) {
		this.batchCount = batchCount;
	}

	public void setName_server_address(String name_server_address) {
		this.name_server_address = name_server_address;
	}

	public String getTfs_rc_server_address() {
		return tfs_rc_server_address;
	}

	public void setTfs_rc_server_address(String tfs_rc_server_address) {
		this.tfs_rc_server_address = tfs_rc_server_address;
	}

	public String getTfs_app_key() {
		return tfs_app_key;
	}

	public void setTfs_app_key(String tfs_app_key) {
		this.tfs_app_key = tfs_app_key;
	}

	public long getAppid() {
		return appid;
	}

	public void setAppid(long appid) {
		this.appid = appid;
	}

	public long getUid() {
		return uid;
	}
	public long getUserId() {
		return userId;
		
	}

	public void setuserId(long usedrId) {
		this.userId = usedrId;
	}

	public void init() {
		// init tfs
		// tfsManager.setMasterIP(name_server_address);
		tfsManager.setRcAddr(tfs_rc_server_address);
		tfsManager.setAppKey(tfs_app_key);
		tfsManager.setTimeout(timeOut);
		tfsManager.setBatchCount(batchCount);
		
//		System.out.println(tfs_rc_server_address);
//		System.out.println(tfs_app_key);
//		System.out.println(timeOut);
//		System.out.println(batchCount);
	
//		tfsManager.setUseNameMeta(false);
		tfsManager.setUseNameMeta(true);
		tfsManager.init();
	}

	public int createFile(String path, long fileSize) {
		try {
			file = new RandomAccessFile(path, "rw");
		} catch (FileNotFoundException e) {
			System.out.println(e);
			return -1;
		}
		try {
			file.setLength(fileSize);
		} catch (IOException e) {
			System.out.println(e);
			return -1;
		}
		try {
			file.close();
		} catch (IOException e) {
			System.out.println(e);
			return -1;
		}
		return 0;
	}

	public int createFile(String path, long fileSize,boolean is_real_file)
	{
		try 
		{
			File f = new File(path);
			if(!f.exists())
				f.createNewFile();
			Random random = new Random();
			byte[] b = new byte[(int) fileSize];
			random.nextBytes(b);
			FileOutputStream  output = new FileOutputStream (f);
			output.write(b);
			output.close();
		} 
		catch (IOException e) 
		{
				// TODO Auto-generated catch block
				e.printStackTrace();
		}
		return 0;
	}
	/*
	 * use file to create a tfs file and save it to tfs file
	 * 
	 * @param path
	 * 
	 * @param size
	 * 
	 * @return tfs file name
	 */
	public String put(String path, long size) {
		String tfsFileName = null;
		int iRet = createFile(path, size);
		if (iRet != 0) {
			System.out.println("It's failed to create file(" + path + ")");

		} else {
			tfsFileName = tfsManager.saveLargeFile(path, "", "");
			System.out.println("Tfs file name : " + tfsFileName + ".");
		}
		Assert.assertNotNull("put TFS file fail !", tfsFileName);
		return tfsFileName;
	}

	public String putLarge(String path) {
		String tfsFileName = null;
		tfsFileName = tfsManager.saveLargeFile(path, "", "");
		System.out.println("Tfs file name : " + tfsFileName + ".");
		Assert.assertNotNull("put TFS file fail !", tfsFileName);
		return tfsFileName;
	}
	
	public String putLarge(String path, String suffix) {
		String tfsFileName = null;
		tfsFileName = tfsManager.saveLargeFile(path, "", suffix);
		System.out.println("Tfs file name : " + tfsFileName + ".");
		Assert.assertNotNull("put TFS file fail !", tfsFileName);
		return tfsFileName;
	}
	
	public String put(String path) {
		String tfsFileName = null;
		tfsFileName = tfsManager.saveFile(path, null, null);
		System.out.println("Tfs file name : " + tfsFileName + ".");
		Assert.assertNotNull("put TFS file fail !", tfsFileName);
		return tfsFileName;
	}
	
	public String put(String path, String suffix) {
		String tfsFileName = null;
		System.out.println(getTfs_app_key());
		tfsFileName = tfsManager.saveFile(path, "", suffix);
		System.out.println("Tfs file name : " + tfsFileName + ".");
		Assert.assertNotNull("put TFS file fail !", tfsFileName);
		return tfsFileName;
	}
	
	public String put(String path, boolean simpleName) {
		String tfsFileName = null;
		tfsFileName = tfsManager.saveFile(path, null, null, simpleName);
		System.out.println("Tfs file name : " + tfsFileName + ".");
		Assert.assertNotNull("put TFS file fail !", tfsFileName);
		return tfsFileName;
	}
	
	public String put(String path, String suffix, boolean simpleName) {
		String tfsFileName = null;
		tfsFileName = tfsManager.saveFile(path, "", suffix, simpleName);
		System.out.println("Tfs file name : " + tfsFileName + ".");
		Assert.assertNotNull("put TFS file fail !", tfsFileName);
		return tfsFileName;
	}

	protected void putNameMeta(String path, long size, String tfsFileName) {
		int iRet = createFile(path, size);
		if (iRet != 0) {
			System.out.println("It's failed to create file(" + path + ")");

		} else {
			Assert.assertTrue(tfsManager
					.saveFile(appid, uid, path, tfsFileName));
		}
	}

	protected void putNameMeta(String path, String tfsFileName) {
		Assert.assertTrue(tfsManager.saveFile(appid, uid, path, tfsFileName));
	}

	/*
	 * save local file(byte[]) to tfs (weird interface, reserve for
	 * compatibility)
	 * 
	 * @param data data to save
	 * 
	 * @return tfsfilename if save successully, or null if fail
	 */
	protected String put(byte[] data) {
		String tfsFileName = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull("put byte data fail !", tfsFileName);
		return tfsFileName;
	}

	/*
	 * save local file(byte[]) to tfs (weird interface, reserve for
	 * compatibility)
	 * 
	 * @param data data to save
	 * 
	 * @param tfsFileName
	 * 
	 * @return tfsfilename if save successully, or null if fail
	 */
	protected String put(byte[] data, String tfsFileName) {
		String name = tfsManager.saveFile(data, tfsFileName, null);
		Assert.assertNotNull("put byte data fail !", tfsFileName);
		return name;
	}

	/*
	 * delete a tfs file
	 * 
	 * @param tfsFileName
	 * 
	 * @param tfsSuffix
	 * 
	 * @return true if delete successully, or false if fail
	 */
	public void delete(String tfsFileName, String suffix) {
		boolean rs = tfsManager.unlinkFile(tfsFileName, suffix);
		Assert.assertTrue("delete tfs file fail !", rs);
	}

	public void delete(String tfsFileName) {
		boolean rs = tfsManager.unlinkFile(tfsFileName, null);
		Assert.assertTrue("delete tfs file fail !", rs);
	}
	


	
	/*
	 * hide file
	 * 
	 * @param tfsFileName
	 * 
	 * @param tfsSuffix
	 * 
	 * @param option 1 conceal 0 reveal
	 * 
	 * @return
	 */
	protected void hide(String tfsFileName, String suffix) {
		boolean rs = tfsManager.hideFile(tfsFileName, null, 1);
		Assert.assertTrue("hide tfs file fail !", rs);
	}

	/*
	 * fetch a tfsfile to output stream
	 * 
	 * @param tfsFileName
	 * 
	 * @param tfsSuffix
	 * 
	 * @param output
	 * 
	 * @return
	 */
	protected void get(String tfsFileName, String suffix, OutputStream output) {
		boolean rs = tfsManager.fetchFile(tfsFileName, suffix, output);
		Assert.assertTrue("get tfs file fail!", rs);
	}
	
	/*
	 * get a tfsfile's file info
	 */
	public FileInfo getTfsStat(String tfsFileName, String tfsSuffix) {
		return tfsManager.statFile(tfsFileName, tfsSuffix);
	}
	
	/*
	 * get a tfsfile's file info
	 */
	public FileInfo getTfsStat(String tfsFileName) {
		return tfsManager.statFile(tfsFileName, null);
	}
	
	/*
	 * get a tfsfile's block id
	 */
	public int getTfsBlockID(String tfsFileName, String tfsSuffix) throws TfsException {
		FSName fsName = new FSName(tfsFileName, tfsSuffix);
		return fsName.getBlockId();
	}
	
	/*
	 * get a tfsfile's block id
	 */
	public int getTfsBlockID(String tfsFileName) throws TfsException {
		FSName fsName = new FSName(tfsFileName, null);
		return fsName.getBlockId();
	}
	
	/*
	 * get a tfsfile's file id
	 */
	public long getTfsFileID(String tfsFileName, String tfsSuffix) throws TfsException {
		FSName fsName = new FSName(tfsFileName, tfsSuffix);
		return fsName.getFileId();
	}
	
	public JSONArray getFileMetaInfo(String filePath) 
	{
		System.out.println("appid is " +appid +"uid is"+userId + "filepath is"+filePath);
		List < FileMetaInfo>  fileMetaInfo =tfsManager.lsDir(appid, userId, filePath);
		//System.out.println("Tfs ls dis  : " + filePath + fileMetaInfo);
		Assert.assertNotNull("the fileMetaInfo is null  !", fileMetaInfo);

		//JSONArray jsonArray = JSONArray.fromObject( "['json','is','easy']" );  
		//System.out.println("print  user define"+jsonArray);
		JSONArray jsonArray = JSONArray.fromObject(fileMetaInfo);
		return  jsonArray;
	}
	public JSONArray getFileMetaFileInfo(String filePath) 
	{
		System.out.println("appid is " +appid +"uid is"+userId + "filepath is"+filePath);
		FileMetaInfo  fileMetaInfo =tfsManager.lsFile(appid, userId, filePath);
		System.out.println("Tfs ls ret is   : " +fileMetaInfo.getFileName());
		Assert.assertNotNull("the fileMetaInfo is null  !", fileMetaInfo);

		//JSONArray jsonArray = JSONArray.fromObject( "['json','is','easy']" );  
		//System.out.println("print  user define"+jsonArray);
		JSONArray jsonArray = JSONArray.fromObject(fileMetaInfo);
		return  jsonArray;
	}

}
