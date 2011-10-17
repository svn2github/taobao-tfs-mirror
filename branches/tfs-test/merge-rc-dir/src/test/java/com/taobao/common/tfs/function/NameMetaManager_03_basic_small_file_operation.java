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

import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class NameMetaManager_03_basic_small_file_operation extends  NameMetaBaseCase{

  public  static List<FileMetaInfo>  file_info_list ;
  public  static FileMetaInfo file_info;

  public  static InputStream In = null ; 
  public  static File small_file =new File("src/test/resources/1m.jpg");

  public  static byte[] data= new byte[(int)small_file.length()];
  public  static CRC32 local_crc ;
  public  static CRC32 tfs_crc;

  @BeforeClass
  public  static void setUpOnce() throws Exception {
    NameMetaBaseCase.setUpOnce();
    int read_length = 0;
    int ret = 0;
    try{
      In=new FileInputStream(small_file);

      while((ret = In.read(data)) != -1)
      {
        read_length += ret;
      }
      if (read_length != (int)small_file.length())
      {
        return ;
      }
    }catch(Exception e){
      e.printStackTrace();
    }finally{
      if(In!= null)
      {
        In.close();
      }
    }
  }

	@AfterClass
	public  static void tearDownOnce() throws Exception {
	}

	@Test
	public void test_01_write_small_file_forwards_by_parts()
  {
    boolean ret = false;
    String filepath1 = "/one";

    ret = tfsManager.createDir(appId, userId,filepath1);
    Assert.assertTrue(ret);

    String filepath2 = "/one/two";
    ret = tfsManager.createDir(appId, userId,filepath2);
    Assert.assertTrue(ret);

    String filepath3 = "/one/two/three";
    ret = tfsManager.createDir(appId, userId,filepath3);
    Assert.assertTrue(ret);

    ret = tfsManager.createFile(appId, userId,filepath3+"/smallfile");
    Assert.assertTrue(ret);

    long total_ret =0 ,write_ret =0, offset=0;

    while( total_ret!= 1<<20)
    {
      write_ret = tfsManager.write(appId, userId,filepath3+"/smallfile", offset, data, offset, 1<<15);
      Assert.assertEquals(write_ret, 1<<15);
      offset += write_ret;
      total_ret += write_ret;
    }
    /*ret = tfsManager.saveFile(res_path+"1m.jpg", filepath3+"/smallfile");
      Assert.assertTrue(ret);*/

    ret = tfsManager.fetchFile(appId, userId,resourcesPath+"empty.jpg", filepath3+"/smallfile");
    Assert.assertTrue(ret);

    //verify the file
    Assert.assertEquals(getCrc(resourcesPath+"1m.jpg"), getCrc(resourcesPath+"empty.jpg"));

    ret = tfsManager.rmFile(appId, userId,filepath3+"/smallfile");
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId,filepath3);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId,filepath2);
    Assert.assertTrue(ret);

    ret = tfsManager.rmDir(appId, userId,filepath1);
    Assert.assertTrue(ret);

  }
  @Test
    public void test_02_write_small_file_backwards_by_parts()
    {
      boolean ret = false;
      String filepath1 = "/one";

      ret = tfsManager.createDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

      String filepath2 = "/one/two";
      ret = tfsManager.createDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      String filepath3 = "/one/two/three";
      ret = tfsManager.createDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.createFile(appId, userId,filepath3+"/smallfile");
      Assert.assertTrue(ret);

      long total_ret =0 ;
      long write_ret =0;
      long offset=(1<<20)-(1<<15);

      while( total_ret!= 1<<20)
      {
        write_ret = tfsManager.write(appId, userId,filepath3+"/smallfile", -1, data, offset, 1<<15);
        Assert.assertEquals(write_ret, 1<<15);
        offset -= 1<<15;
        total_ret += write_ret;
      } 
      //verify the file
      Assert.assertEquals(total_ret, 1<<20);

      ret = tfsManager.fetchFile(appId, userId,resourcesPath+"empty.jpg", filepath3+"/smallfile");
      Assert.assertTrue(ret);
      //get the files' crc to  verify
      Assert.assertEquals(getCrc(resourcesPath+"1m.jpg"),getCrc(resourcesPath+"empty.jpg") );

      ret = tfsManager.rmFile(appId, userId,filepath3+"/smallfile");
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath1);
      Assert.assertTrue(ret);
    }

  @Test
    public void test_03_write_small_file_by_parts()
    {
      boolean ret = false;
      String filepath1 = "/one";

      ret = tfsManager.createDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

      String filepath2 = "/one/two";
      ret = tfsManager.createDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      String filepath3 = "/one/two/three";
      ret = tfsManager.createDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.createFile(appId, userId,filepath3+"/smallfile");
      Assert.assertTrue(ret);

      long total_ret =0 ,write_ret =0, offset=0;

      while( total_ret!= 1<<20)
      {
        write_ret = tfsManager.write(appId, userId,filepath3+"/smallfile", offset, data, offset, 1<<15);
        Assert.assertEquals(write_ret, 1<<15);
        offset += write_ret;
        total_ret += write_ret;
      }
      /*ret = tfsManager.saveFile(res_path+"1m.jpg", filepath3+"/smallfile");
        Assert.assertTrue(ret);*/

      ret = tfsManager.fetchFile(appId, userId,resourcesPath+"empty.jpg", filepath3+"/smallfile");
      Assert.assertTrue(ret);

      //verify the file
      Assert.assertEquals(getCrc(resourcesPath+"1m.jpg"),getCrc(resourcesPath+"empty.jpg") );

      ret = tfsManager.rmFile(appId, userId,filepath3+"/smallfile");
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

    }

  @Ignore
    public void test_04_write_small_file_interrupted_by_other_ops()
    {
      boolean ret = false;
      String filepath1 = "/one";

      ret = tfsManager.createDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

      ret = tfsManager.createFile(appId, userId,filepath1+"/smallfile");
      Assert.assertTrue(ret);

      long total_ret =0 ,write_ret =0;
      /*       
               while( total_ret< 1<<20)
               {
               if(total_ret < 1<<19)
               {
               write_ret = tfsManager.write(filepath1+"/smallfile", total_ret, data, total_ret, 1<<18);
               Assert.assertEquals(write_ret, 1<<18);
               total_ret += write_ret;
               }
               else if(total_ret == 1<<19)
               {
               ret = tfsManager.mvFile(filepath1+"/smallfile", filepath1+"/renamed");
               Assert.assertTrue(ret);
               }
               else
               {
               write_ret = tfsManager.write(filepath1+"/renamed", total_ret, data, total_ret, 1<<18);
               Assert.assertEquals(write_ret, 1<<18);
               total_ret += write_ret;
               }
               }
               */       
      write_ret = tfsManager.write(appId, userId,filepath1+"/smallfile", 0, data, 1<<18, 1<<18);
      Assert.assertEquals(write_ret, 1<<18);
      write_ret = tfsManager.write(appId, userId,filepath1+"/smallfile", 1<<18, data, 1<<18, 1<<18);
      Assert.assertEquals(write_ret, 1<<18);

      ret = tfsManager.mvFile(appId, userId,filepath1+"/smallfile", filepath1+"/renamed");
      Assert.assertTrue(ret);

      write_ret = tfsManager.write(appId, userId,filepath1+"/renamed",1<<19, data, 1<<19, 1<<19);
      Assert.assertEquals(write_ret, 1<<19);

      ret = tfsManager.fetchFile(appId, userId,resourcesPath+"empty.jpg", filepath1+"/renamed");
      Assert.assertTrue(ret);

      //verify the file
      Assert.assertEquals(getCrc(resourcesPath+"1m.jpg"), getCrc(resourcesPath+"empty.jpg"));

      ret = tfsManager.rmFile(appId, userId,filepath1+"/renamed");
      Assert.assertTrue(ret);


      ret = tfsManager.rmDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

    }
  @Test
    public void test_05_forward_write_part_of_small_file_and_rename_then_go_on()
    {
      boolean ret = false;
      String filepath1 = "/one";

      ret = tfsManager.createDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

      String filepath2 = "/one/two";
      ret = tfsManager.createDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      String filepath3 = "/one/two/three";
      ret = tfsManager.createDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.createFile(appId, userId,filepath3+"/smallfile");
      Assert.assertTrue(ret);

      long  write_ret = tfsManager.write(appId, userId,filepath3+"/smallfile", 0, data, 0, 1<<19);
      Assert.assertEquals(write_ret, 1<<19);


      ret = tfsManager.mvFile(appId, userId,filepath3+"/smallfile", filepath3+"/renamed");
      Assert.assertTrue(ret);

      write_ret = tfsManager.write(appId, userId,filepath3+"/renamed", 1<<19, data, 1<<19, 1<<19);
      Assert.assertEquals(write_ret, 1<<19);


      ret = tfsManager.fetchFile(appId, userId,resourcesPath+"empty.jpg", filepath3+"/renamed");
      Assert.assertTrue(ret);

      //verify the file
      Assert.assertEquals(getCrc(resourcesPath+"1m.jpg"), getCrc(resourcesPath+"empty.jpg"));


      ret = tfsManager.rmFile(appId, userId,filepath3+"/renamed");
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

    }
  @Test
    public void test_06_forward_write_part_of_small_file_and_move_then_go_on()
    {
      boolean ret = false;
      String filepath1 = "/one";

      ret = tfsManager.createDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

      String filepath2 = "/one/two";
      ret = tfsManager.createDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      String filepath3 = "/one/two/three";
      ret = tfsManager.createDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.createFile(appId, userId,filepath3+"/smallfile");
      Assert.assertTrue(ret);

      long  write_ret = tfsManager.write(appId, userId,filepath3+"/smallfile", 0, data, 0, 1<<19);
      Assert.assertEquals(write_ret, 1<<19);

      ret = tfsManager.mvFile(appId, userId,filepath3+"/smallfile", filepath1+"/renamed");
      Assert.assertTrue(ret);

      write_ret = tfsManager.write(appId, userId,filepath1+"/renamed", 1<<19, data, 1<<19, 1<<19);
      Assert.assertEquals(write_ret, 1<<19);

      ret = tfsManager.fetchFile(appId, userId,resourcesPath+"empty.jpg", filepath1+"/renamed");
      Assert.assertTrue(ret);

      //verify the file
      Assert.assertEquals(getCrc(resourcesPath+"1m.jpg"),getCrc(resourcesPath+"empty.jpg") );

      ret = tfsManager.rmFile(appId, userId,filepath1+"/renamed");
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

    }
  @Test
    public void test_07_backward_write_part_of_small_file_and_rename_then_go_on()
    {
      boolean ret = false;
      String filepath1 = "/one";

      ret = tfsManager.createDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

      String filepath2 = "/one/two";
      ret = tfsManager.createDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      String filepath3 = "/one/two/three";
      ret = tfsManager.createDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.createFile(appId, userId,filepath3+"/smallfile");
      Assert.assertTrue(ret);

      long  write_ret = tfsManager.write(appId, userId,filepath3+"/smallfile", -1, data, 1<<19, 1<<19);
      Assert.assertEquals(write_ret, 1<<19);

      ret = tfsManager.mvFile(appId, userId,filepath3+"/smallfile", filepath3+"/renamed");
      Assert.assertTrue(ret);

      write_ret = tfsManager.write(appId, userId,filepath3+"/renamed", -1, data, 0, 1<<19);
      Assert.assertEquals(write_ret, 1<<19);

      ret = tfsManager.fetchFile(appId, userId,resourcesPath+"empty.jpg", filepath3+"/renamed");
      Assert.assertTrue(ret);

      //verify the file
      Assert.assertEquals(getCrc(resourcesPath+"1m.jpg"),getCrc(resourcesPath+"empty.jpg") );

      ret = tfsManager.rmFile(appId, userId,filepath3+"/renamed");
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath1);
      Assert.assertTrue(ret);
    }

  @Test
    public void test_08_backward_write_part_of_small_file_and_move_then_go_on()
    {
      boolean ret = false;
      String filepath1 = "/one";

      ret = tfsManager.createDir(appId, userId,filepath1);
      Assert.assertTrue(ret);

      String filepath2 = "/one/two";
      ret = tfsManager.createDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      String filepath3 = "/one/two/three";
      ret = tfsManager.createDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.createFile(appId, userId,filepath3+"/smallfile");
      Assert.assertTrue(ret);

      long  write_ret = tfsManager.write(appId, userId,filepath3+"/smallfile", -1, data, 1<<19, 1<<19);
      Assert.assertEquals(write_ret, 1<<19);

      ret = tfsManager.mvFile(appId, userId,filepath3+"/smallfile", filepath1+"/renamed");
      Assert.assertTrue(ret);

      write_ret = tfsManager.write(appId, userId,filepath1+"/renamed", -1, data, 0, 1<<19);
      Assert.assertEquals(write_ret, 1<<19);

      ret = tfsManager.fetchFile(appId, userId,resourcesPath+"empty.jpg", filepath1+"/renamed");
      Assert.assertTrue(ret);

      //verify the file
      Assert.assertEquals(getCrc(resourcesPath+"1m.jpg"), getCrc(resourcesPath+"empty.jpg"));


      ret = tfsManager.rmFile(appId, userId,filepath1+"/renamed");
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath3);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath2);
      Assert.assertTrue(ret);

      ret = tfsManager.rmDir(appId, userId,filepath1);
      Assert.assertTrue(ret);
    }
}
