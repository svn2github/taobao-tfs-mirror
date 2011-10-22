package com.taobao.common.tfs.function;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.CRC32;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class NameMetaManager_03_small_file_operation extends  NameMetaManagerBaseCase{

    public static String rootDir = "/NameMetaTest3";
    public static String filepath1 = rootDir + "/1";
    public static String filepath2 = filepath1 + "/2";
    public static String filepath3 = filepath2 + "/3";
    public static String filename = filepath3 + "/smallfile";
    public static String localFile = resourcesPath + "/2M";
    public static String savedFile = resourcesPath + "/saved";
    public static File small_file = new File(localFile);
    public static long file_len = 1<<21, write_len = (1<<15)+1;

    public static InputStream In = null ; 
    public static byte[] data= new byte[(int)small_file.length()];
    public static CRC32 local_crc ;
    public static CRC32 tfs_crc;

    public static void getLocalData() {	 
        int read_length = 0;
        int ret = 0;
        try {
            In = new FileInputStream(small_file);

            while ((ret = In.read(data)) != -1) {
              read_length += ret;
            }
            if (read_length != (int)small_file.length()) {
              return ;
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally {
            if (In != null) {
                try {
                    In.close();
                } 
                catch(Exception e) {
                }
            }
        }
    }

    @BeforeClass
    public static void setUpOnce() throws Exception {
        boolean bret = false;
        rmDirRecursive(appId, userId, rootDir); 

        NameMetaBaseCase.setUpOnce();
        bret = tfsManager.createDir(appId, userId, rootDir);
        Assert.assertTrue(bret);

        bret = tfsManager.createDir(appId, userId, filepath1);
        Assert.assertTrue(bret);

        bret = tfsManager.createDir(appId, userId, filepath2);
        Assert.assertTrue(bret);

        bret = tfsManager.createDir(appId, userId, filepath3);
        Assert.assertTrue(bret);

        // read file into mem
        getLocalData();
      }
   
      @AfterClass
      public  static void tearDownOnce() throws Exception {
          rmDirRecursive(appId, userId, rootDir); 
      }
  
    @Test
    public void test_01_write_small_file_forwards_by_parts() {
        boolean ret = false;

        ret = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(ret);
      
        long write_ret = 0, offset = 0, left_len = file_len;

        while(left_len > write_len) {
           write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
           Assert.assertEquals(write_ret, write_len);
           offset += write_ret;
           left_len -= write_ret;
        }

        // write the left
        write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        ret = tfsManager.fetchFile(appId, userId, savedFile, filename);
        Assert.assertTrue(ret);

        //verify the file
        Assert.assertEquals(getCrc(localFile), getCrc(savedFile));

        ret = tfsManager.rmFile(appId, userId, filename);
        Assert.assertTrue(ret);
    }

    //@Test
    public void test_02_write_small_file_backwards_by_parts() {
        boolean ret = false;

        ret = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(ret);
       
        long write_ret = 0, offset = file_len - write_len, left_len = file_len;

        while (left_len > write_len) {
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
        ret = tfsManager.fetchFile(appId, userId, savedFile, filename);
        Assert.assertTrue(ret);

        Assert.assertEquals(getCrc(localFile), getCrc(savedFile));

        ret = tfsManager.rmFile(appId, userId, filename);
        Assert.assertTrue(ret);
    }

    //@Test
    public void test_03_forward_write_small_file_rename_and_go_on() {
        boolean ret = false;

        ret = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(ret);

        long write_ret = 0, offset = 0, left_len = file_len;

        while (left_len > write_len) {
           write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
           Assert.assertEquals(write_ret, write_len);
           offset += write_ret;
           left_len -= write_ret;
        }

        String filename_new = filepath3 + "/renamed";

        // rename
        ret = tfsManager.mvFile(appId, userId, filename, filename_new);
        Assert.assertTrue(ret);

        // write the left
        write_ret = tfsManager.write(appId, userId, filename_new, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        ret = tfsManager.fetchFile(appId, userId, savedFile, filename_new);
        Assert.assertTrue(ret);

        //verify the file
        Assert.assertEquals(getCrc(localFile), getCrc(savedFile));

        ret = tfsManager.rmFile(appId, userId, filename_new);
        Assert.assertTrue(ret);
    }

    //@Test
    public void test_04_forward_write_small_file_move_and_go_on() {
        boolean ret = false;

        ret = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(ret);

        long write_ret = 0, offset = 0, left_len = file_len;

        while(left_len > write_len) {
           write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
           Assert.assertEquals(write_ret, write_len);
           offset += write_ret;
           left_len -= write_ret;
        }

        String filename_new = filepath2 + "/moved";

        // rename
        ret = tfsManager.mvFile(appId, userId, filename, filename_new);
        Assert.assertTrue(ret);

        // write the left
        write_ret = tfsManager.write(appId, userId, filename_new, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        ret = tfsManager.fetchFile(appId, userId, savedFile, filename_new);
        Assert.assertTrue(ret);

        //verify the file
        Assert.assertEquals(getCrc(localFile), getCrc(savedFile));

        ret = tfsManager.rmFile(appId, userId, filename_new);
        Assert.assertTrue(ret);
    }

    //@Test
    public void test_05_backward_write_small_file_rename_and_go_on() {
        boolean ret = false;
        
        ret = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(ret);

        long write_ret = 0, offset = file_len - write_len, left_len = file_len;

        while (left_len > write_len) {
            write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
            Assert.assertEquals(write_ret, write_len);
            left_len -= write_ret;
            offset -= (left_len > write_ret ? write_ret : left_len);
        } 

        String filename_new = filepath3 + "/renamed";

        // rename
        ret = tfsManager.mvFile(appId, userId, filename, filename_new);
        Assert.assertTrue(ret);

        // write the left
        Assert.assertEquals(offset, 0);
        write_ret = tfsManager.write(appId, userId, filename_new, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        //verify the file
        ret = tfsManager.fetchFile(appId, userId, savedFile, filename_new);
        Assert.assertTrue(ret);

        Assert.assertEquals(getCrc(localFile), getCrc(savedFile));

        ret = tfsManager.rmFile(appId, userId, filename_new);
        Assert.assertTrue(ret);
    }

    //@Test
    public void test_06_backward_write_small_file_move_and_go_on() {
        boolean ret = false;
        
        ret = tfsManager.createFile(appId, userId, filename);
        Assert.assertTrue(ret);

        long write_ret = 0, offset = file_len - write_len, left_len = file_len;

        while (left_len > write_len) {
            write_ret = tfsManager.write(appId, userId, filename, offset, data, offset, write_len);
            Assert.assertEquals(write_ret, write_len);
            left_len -= write_ret;
            offset -= (left_len > write_ret ? write_ret : left_len);
        } 

        String filename_new = filepath2 + "/moved";

        // rename
        ret = tfsManager.mvFile(appId, userId, filename, filename_new);
        Assert.assertTrue(ret);

        // write the left
        Assert.assertEquals(offset, 0);
        write_ret = tfsManager.write(appId, userId, filename_new, offset, data, offset, left_len);
        Assert.assertEquals(write_ret, left_len);

        //verify the file
        ret = tfsManager.fetchFile(appId, userId, savedFile, filename_new);
        Assert.assertTrue(ret);

        Assert.assertEquals(getCrc(localFile), getCrc(savedFile));

        ret = tfsManager.rmFile(appId, userId, filename_new);
        Assert.assertTrue(ret);
    }
}
