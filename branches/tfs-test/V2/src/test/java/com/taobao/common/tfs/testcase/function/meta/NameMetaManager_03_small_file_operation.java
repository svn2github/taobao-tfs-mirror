package com.taobao.common.tfs.testcase.function.meta;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.zip.CRC32;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.testcase.metaTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;

public class NameMetaManager_03_small_file_operation extends  metaTfsBaseCase
{

    public static String rootDir = "/NameMetaTest3";
    public static String filepath1 = rootDir + "/1";
    public static String filepath2 = filepath1 + "/2";
    public static String filepath3 = filepath2 + "/3";
    public static String filename = filepath3 + "/smallfile";
    public static String localFile = resourcesPath + "2M.jpg";
    public static String savedFile = resourcesPath + "saved";
    public static File small_file = new File(localFile);
    public static long file_len = 1<<21, write_len = (1<<15)+1;

    public static InputStream In = null ; 
    public static byte[] data= new byte[(int)small_file.length()];
    public static CRC32 local_crc ;
    public static CRC32 tfs_crc;

    public static void getLocalData()
    {	 
        int read_length = 0;
        int ret = 0;
        try 
        {
            In = new FileInputStream(small_file);
            while ((ret = In.read(data)) != -1) 
            {
              read_length += ret;
            }
            if (read_length != (int)small_file.length()) 
            {
              return ;
            }
        }
        catch(Exception e) 
        {
            e.printStackTrace();
        }
        finally 
        {
            if (In != null) 
            {
                try 
                {
                    In.close();
                } 
                catch(Exception e) 
                {
                }
            }
        }
    }

    @BeforeClass
    public static void setUpOnce() throws Exception 
    {
        int iRet = -1;
        //rmDirRecursive(appId, userId, rootDir); 

        iRet = tfsManager.createDir(appId, userId, rootDir);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        iRet = tfsManager.createDir(appId, userId, filepath1);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        iRet = tfsManager.createDir(appId, userId, filepath2);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        iRet = tfsManager.createDir(appId, userId, filepath3);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        // read file into mem
        getLocalData();
    }
   
      @AfterClass
      public  static void tearDownOnce() throws Exception 
      {
          rmDirRecursive(appId, userId, rootDir); 
      }
  
    @Test
    public void test_01_write_small_file_forwards_by_parts() 
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
    	int iRet = -1;
    	boolean bRet = false;
    	iRet = tfsManager.createFile(appId, userId, filename);
    	Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
      
        long write_ret = 0, offset = 0, left_len = file_len;

        while(left_len > write_len) 
        {
           write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
           Assert.assertEquals(write_ret, write_len);
           offset += write_ret;
           left_len -= write_ret;
        }

        // write the left
        write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        bRet = tfsManager.fetchFile(appId, userId, savedFile, filename);
        Assert.assertTrue(bRet);

        //verify the file
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc(savedFile));

        iRet = tfsManager.rmFile(appId, userId, filename);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
    }

   @Test
    public void test_02_write_small_file_backwards_by_parts() 
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        boolean bRet = false;
        int iRet = -1;
        iRet = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
       
        long write_ret = 0, offset = file_len - write_len, left_len = file_len;

        while (left_len > write_len) 
        {
            write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
            Assert.assertEquals(write_ret, write_len);
            left_len -= write_ret;
            offset -= (left_len > write_ret ? write_ret : left_len);
        } 

        // write the left
        Assert.assertEquals(offset, 0);
        write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        //verify the file
        bRet = tfsManager.fetchFile(appId, userId, savedFile, filename);
        Assert.assertTrue(bRet);

        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc(savedFile));

        iRet = tfsManager.rmFile(appId, userId, filename);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
    }

    @Test
    public void test_03_forward_write_small_file_rename_and_go_on() 
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        boolean bRet = false;
        int iRet = -1;

        iRet = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        long write_ret = 0, offset = 0, left_len = file_len;

        while (left_len > write_len) 
        {
           write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
           Assert.assertEquals(write_ret, write_len);
           offset += write_ret;
           left_len -= write_ret;
        }

        String filename_new = filepath3 + "/renamed";

        // rename
        iRet = tfsManager.mvFile(appId, userId, filename, filename_new);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        // write the left
        write_ret = tfsManager.write(appId, userId, filename_new, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        bRet = tfsManager.fetchFile(appId, userId, savedFile, filename_new);
        Assert.assertTrue(bRet);

        //verify the file
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc(savedFile));

        iRet = tfsManager.rmFile(appId, userId, filename_new);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
    }

    @Test
    public void test_04_forward_write_small_file_move_and_go_on() 
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        boolean bRet = false;
        int iRet = -1;

        iRet = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        long write_ret = 0, offset = 0, left_len = file_len;

        while(left_len > write_len) 
        {
           write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
           Assert.assertEquals(write_ret, write_len);
           offset += write_ret;
           left_len -= write_ret;
        }

        String filename_new = filepath2 + "/moved";

        // rename
        iRet = tfsManager.mvFile(appId, userId, filename, filename_new);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        // write the left
        write_ret = tfsManager.write(appId, userId, filename_new, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        bRet = tfsManager.fetchFile(appId, userId, savedFile, filename_new);
        Assert.assertTrue(bRet);

        //verify the file
        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc(savedFile));

        iRet = tfsManager.rmFile(appId, userId, filename_new);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
    }

    @Test
    public void test_05_backward_write_small_file_rename_and_go_on() 
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        boolean bRet = false;
        int iRet = -1;
        
        iRet = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        long write_ret = 0, offset = file_len - write_len, left_len = file_len;

        while (left_len > write_len) 
        {
            write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
            Assert.assertEquals(write_ret, write_len);
            left_len -= write_ret;
            offset -= (left_len > write_ret ? write_ret : left_len);
        } 

        String filename_new = filepath3 + "/renamed";

        // rename
        iRet = tfsManager.mvFile(appId, userId, filename, filename_new);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        // write the left
        Assert.assertEquals(offset, 0);
        write_ret = tfsManager.write(appId, userId, filename_new, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        //verify the file
        bRet = tfsManager.fetchFile(appId, userId, savedFile, filename_new);
        Assert.assertTrue(bRet);

        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc(savedFile));

        iRet = tfsManager.rmFile(appId, userId, filename_new);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
    }

    @Test
    public void test_06_backward_write_small_file_move_and_go_on() 
    {
    	log.info(new Throwable().getStackTrace()[0].getMethodName());
        boolean bRet = false;
        int iRet = -1;
        
        iRet = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        long write_ret = 0, offset = file_len - write_len, left_len = file_len;

        while (left_len > write_len) 
        {
            write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
            Assert.assertEquals(write_ret, write_len);
            left_len -= write_ret;
            offset -= (left_len > write_ret ? write_ret : left_len);
        } 

        String filename_new = filepath2 + "/moved";

        // rename
        iRet = tfsManager.mvFile(appId, userId, filename, filename_new);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

        // write the left
        Assert.assertEquals(offset, 0);
        write_ret = tfsManager.write(appId, userId, filename_new, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        //verify the file
        bRet = tfsManager.fetchFile(appId, userId, savedFile, filename_new);
        Assert.assertTrue(bRet);

        Assert.assertEquals(FileUtility.getCrc(localFile), FileUtility.getCrc(savedFile));

        iRet = tfsManager.rmFile(appId, userId, filename_new);
        Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
    }
}
