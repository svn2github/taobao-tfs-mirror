package com.taobao.common.tfs.function;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.CRC32;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.taobao.common.tfs.TfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class NameMetaManager_01_basic_directory_operation extends  NameMetaBaseCase{
	
	public static List<FileMetaInfo>  fileInfoList;
	public static FileMetaInfo fileInfo;
	public static String localFile = "src/test/resources/100K";
  public static String rootDir = "/NameMetaTest1";

	@BeforeClass
  public  static void setUpOnce() throws Exception {
    NameMetaBaseCase.setUpOnce();
		boolean ret = tfsManager.createDir(appId, userId, "/ ");
    Assert.assertTrue(ret);
    rmDirRecursive(appId, userId, rootDir); 
  }

  @AfterClass
  public static void tearDownOnce() throws Exception {
    NameMetaBaseCase.tearDownOnce();
    rmDirRecursive(appId, userId, rootDir); 
  }

	@Test
	public void test_00_ls_top_directory()
	{
     boolean ret = false;
     int oldSize = 0;
     int newSize = 0;

     fileInfoList = tfsManager.lsDir(appId, userId, "/");
     oldSize = fileInfoList.size();

		 ret = tfsManager.createDir(appId, userId, rootDir);
     Assert.assertTrue(ret);

     fileInfoList = tfsManager.lsDir(appId, userId, "/");
     newSize = fileInfoList.size();
     Assert.assertEquals(1, newSize - oldSize); 
     
     ret = tfsManager.rmDir(appId, userId, rootDir);
     Assert.assertTrue(ret);
	}
	
	//@Test
	public void test_01_create_directory()
	{
    boolean ret = false;
    String filepath1 = rootDir + "/dddd";

    ret = tfsManager.createDir(appId, userId, filepath1);
    Assert.assertTrue(ret);

    String filepath2 = filepath1 + "/ffff";
    ret = tfsManager.createDir(appId, userId, filepath2);
    Assert.assertTrue(ret);

    fileInfoList= tfsManager.lsDir(appId, userId, filepath1);
    Assert.assertFalse(fileInfoList.isEmpty()); 

    ret = tfsManager.rmDir(appId, userId, filepath2);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath1);
    Assert.assertTrue(ret);
	}
	
	//@Test
	public void test_02_remove_directory()
	{
    boolean ret = false;
    String filepath1 = rootDir + "/eeee";

    ret = tfsManager.createDir(appId, userId, filepath1);
    Assert.assertTrue(ret);

    String filepath2 = filepath1 + "/gggg";
    ret = tfsManager.createDir(appId, userId, filepath2);
    Assert.assertTrue(ret);  

    fileInfoList= tfsManager.lsDir(appId, userId, filepath1);
    Assert.assertFalse(fileInfoList.isEmpty());

    fileInfo = fileInfoList.get(0);

    long  pid  = fileInfo.getPid();
    Assert.assertTrue(pid > 0);

    String dirname = fileInfo.getFileName();
    Assert.assertNotNull(dirname);

    fileInfoList= tfsManager.lsDir(appId, userId, filepath2);
    Assert.assertTrue(fileInfoList.isEmpty()); 

    ret = tfsManager.rmDir(appId, userId, filepath2);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath1);
    Assert.assertTrue(ret);
	}

	//@Test
	public void test_03_rename_directory()
  {
    boolean ret = false;
    String filepath1 = rootDir + "/dddd";
    String filepath2 = rootDir + "/cccc";

    ret = tfsManager.createDir(appId, userId, filepath1);
    Assert.assertTrue(ret);

    ret = tfsManager.mvDir(appId, userId, filepath1, filepath2);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath1);
    Assert.assertFalse(ret);

    ret = tfsManager.rmDir(appId, userId, filepath2);
    Assert.assertTrue(ret);
	}
	
	//@Test
	public void test_04_move_directory()
  {
    boolean ret = false;
    String filepath1 = rootDir + "/dddd";

    ret = tfsManager.createDir(appId, userId, filepath1);
    Assert.assertTrue(ret);

    String filepath2 = filepath1 + "/ffff";
    ret = tfsManager.createDir(appId, userId, filepath2);
    Assert.assertTrue(ret);

    String filepath3 = filepath2 + "/ttt";
    ret = tfsManager.createDir(appId, userId, filepath3);
    Assert.assertTrue(ret);

    String filepath4 = filepath1 + "/ttt";
    ret = tfsManager.mvDir(appId, userId, filepath3, filepath4);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath3);
    Assert.assertFalse(ret);

    ret = tfsManager.rmDir(appId, userId, filepath4);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath2);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath1);
    Assert.assertTrue(ret);
  }
	
	//@Test
	public void test_05_save_file()
	{
    boolean ret = false;
    String filepath1 = rootDir + "/dddd";
    String filepath2 = filepath1 + "/ffff";
    String filepath3 = filepath2 + "/file";

    ret= tfsManager.createDir(appId, userId, filepath1);
		Assert.assertTrue(ret);

		ret= tfsManager.createDir(appId, userId, filepath2);
		Assert.assertTrue(ret);

		ret = tfsManager.saveFile(appId, userId, localFile, filepath3);
		Assert.assertTrue(ret);

	}
}
