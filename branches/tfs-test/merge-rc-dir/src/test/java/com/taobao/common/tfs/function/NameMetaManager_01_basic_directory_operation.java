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

public class NameMetaManager_01_basic_directory_operation extends  tfsNameBaseCase{
	
	public static List<FileMetaInfo>  file_info_list ;
	public static FileMetaInfo file_info;
	public static String local_file = "src/test/resources/200k.jpg";
	
  @BeforeClass
	public  static void setUp() throws Exception {

	}
	@AfterClass
	public static void tearDown() throws Exception 
	{
	}
	@Test
	public void test_00_ls_top_directory()
	{
		 boolean ret = false;
     String filepath1 = "/dddd";
         
		 ret = TfsManager.createDir(appId, userId, filepath1);
     Assert.assertTrue(ret);

     file_info_list= tfsManager.lsDir(appId, userId, "/");
     Assert.assertEquals(1, file_info_list.size()); 
     
     ret = tfsManager.rmDir(appId, userId, filepath1);
     Assert.assertTrue(ret);
	}
	
	@Test
	public void test_01_create_directory()
	{
    boolean ret = false;
    String filepath1 = "/dddd";

    ret = tfsManager.createDir(appId, userId, filepath1);
    Assert.assertTrue(ret);

    String filepath2 = "/dddd/ffff";
    ret = tfsManager.createDir(appId, userId, filepath2);
    Assert.assertTrue(ret);

    file_info_list= tfsManager.lsDir(appId, userId, filepath1);
    Assert.assertFalse(file_info_list.isEmpty()); 

    ret = tfsManager.rmDir(appId, userId, filepath2);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath1);
    Assert.assertTrue(ret); /**/
	}
	
	@Test
	public void test_02_remove_directory()
	{
    boolean ret = false;
    String filepath1 = "/eeee";

    ret = tfsManager.createDir(appId, userId, filepath1);
    Assert.assertTrue(ret);

    String filepath2 = "/eeee/gggg";
    ret = tfsManager.createDir(appId, userId, filepath2);
    Assert.assertTrue(ret);  

    file_info_list= tfsManager.lsDir(appId, userId, filepath1);
    Assert.assertFalse(file_info_list.isEmpty());

    file_info_list= tfsManager.lsDir(appId, userId, filepath2);
    Assert.assertTrue(file_info_list.isEmpty()); 

    file_info = file_info_list.get(0);

    long  pid  = file_info.getPid();
    Assert.assertTrue(pid >0);

    String dirname = file_info.getFileName();
    Assert.assertNotNull(dirname);

    ret = tfsManager.rmDir(appId, userId, filepath2);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath1);
    Assert.assertTrue(ret);
	}

	@Test
	public void test_03_rename_directory()
  {
    boolean ret = false;
    String filepath1 = "/dddd";
    String filepath2 = "/cccc";

    ret = tfsManager.createDir(appId, userId, filepath1);
    Assert.assertTrue(ret);

    ret = tfsManager.mvDir(appId, userId, filepath1, filepath2);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath1);
    Assert.assertFalse(ret);

    ret = tfsManager.rmDir(appId, userId, filepath2);
    Assert.assertTrue(ret);
	}
	
	@Test
	public void test_04_move_directory()
  {
    boolean ret = false;
    String filepath1 = "/dddd";

    ret = tfsManager.createDir(appId, userId, filepath1);
    Assert.assertTrue(ret);

    String filepath2 = "/dddd/ffff";
    ret = tfsManager.createDir(appId, userId, filepath2);
    Assert.assertTrue(ret);

    String filepath3 = "/dddd/ffff/ttt";
    ret = tfsManager.createDir(appId, userId, filepath3);
    Assert.assertTrue(ret);

    ret = tfsManager.mvDir(appId, userId, "/dddd/ffff/ttt","/dddd/ttt");

    ret = tfsManager.rmDir(appId, userId, filepath2);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, "/dddd/ttt");
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId, filepath1);
    Assert.assertTrue(ret);
  }
	
	@Test
	public void test_05_create()
	{
		boolean ret= tfsManager.createDir(appId, userId, "/teaa");
		Assert.assertTrue(ret);
		ret= tfsManager.createDir(appId, userId, "/teaa/among");
		Assert.assertTrue(ret);
		ret = tfsManager.saveFile(userId, "D:\\workspace\\tfs-client-java-with-name-meta\\src\\test\\resources\\100k.jpg", "/teaa/among/lys");
		Assert.assertTrue(ret);

	}
}
