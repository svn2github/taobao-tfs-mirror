package com.taobao.common.tfs.function;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.CRC32;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class NameMetaManager_05_basic_large_file_operation extends  NameMetaBaseCase{
	
    public  static List<FileMetaInfo>  file_info_list ;
    public  static FileMetaInfo file_info;
    public  static File large_file =new File("src/test/resources/1g.jpg");	
      
    public  static InputStream In = null;  
    
    public  static byte[] data= new byte[1<<22];
   
    public  static void get_local_data() 
    {	 
    	 int read_length = 0;
	 	   int ret = 0;
	     try{
         	   while((ret = In.read(data)) != -1)
		        {
	    	    	read_length += ret;
		        }
	    	    if (read_length != (int)large_file.length())
	    	    {
	    	    	return ;
	    	    }
		        }catch(Exception e){
		           e.printStackTrace();
		        }finally{
		        
		        }
    }

    public  static void get_local_data_with_offset(int data_offset,int len)
    {
   	 int read_length = 0;
 	   int ret = 0;
       try{
         while((ret = In.read(data, data_offset, len)) != -1) {
           read_length += ret;
           if(read_length == len) {
             break;
           }
         }
       }catch(Exception e){
         e.printStackTrace();
       }finally{
       }
    }

	@Before
	public void setUp(){
		try {
			In = new FileInputStream(large_file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@After
	public void tearDown() {
        if(In!= null)
        {
          try {
			In.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        }
	}
	
	@Test
	public void test_01_create_and_write_large_file_forwards_by_parts_and_fetch()
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
         
         ret = tfsManager.createFile(appId, userId,filepath3+"/largefile");
         Assert.assertTrue(ret);
         
         long total_ret =0 ,write_ret =0, offset=0 ,data_offset = 0;
         
         for (int i = 0; i<1<<8 ;i++)
         {
           get_local_data();
           if( total_ret!= 1<<30)
           {
             write_ret = tfsManager.write(appId, userId,filepath3+"/largefile", offset, data, data_offset, 1<<22);
             Assert.assertEquals(write_ret, 1<<22);
             offset += write_ret;
             total_ret += write_ret;
           }
         }
         Assert.assertEquals(total_ret, 1<<30);
         ret = tfsManager.fetchFile(appId, userId,resourcesPath+"empty.jpg", filepath3+"/largefile");
         Assert.assertTrue(ret);
         
         //verify the file by crc
         Assert.assertEquals(getCrc(resourcesPath+"1g.jpg"),getCrc(resourcesPath+"empty.jpg"));
         
         file_info = tfsManager.lsFile(appId, userId,filepath3+"/largefile");
         
         Assert.assertEquals(file_info.getLength(), 1<<30);
         Assert.assertTrue(file_info.isFile());
         Assert.assertEquals("largefile", file_info.getFileName());
        
         
         ret = tfsManager.rmFile(appId, userId,filepath3+"/largefile");
         Assert.assertTrue(ret);
         
         file_info_list = tfsManager.lsDir(appId, userId,filepath3);
		 Assert.assertEquals(file_info_list.size(), 0);
         
         ret = tfsManager.rmDir(appId, userId,filepath3);
         Assert.assertTrue(ret);
         
         ret = tfsManager.rmDir(appId, userId,filepath2);
         Assert.assertTrue(ret);
         
         ret = tfsManager.rmDir(appId, userId,filepath1);
         Assert.assertTrue(ret);
   
	}

	@Test
	public void test_02_create_and_write_large_file_backwards_by_parts_and_fetch()
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
         
         ret = tfsManager.createFile(appId, userId,filepath3+"/largefile");
         Assert.assertTrue(ret);
         
         //从本地文件末尾开始倒序写
         long total_ret =0 ,write_ret =0 ;
         long data_offset = 0;
 
         for (int i = 0; i<1<<8 ;i++)
         {
           get_local_data_with_offset((int)data_offset,1<<22);

           if( total_ret!= 1<<30)
           {
             write_ret = tfsManager.write(appId, userId,filepath3+"/largefile", -1, data, data_offset, 1<<22);
             Assert.assertEquals(write_ret, 1<<22);
             total_ret += write_ret;
           }
         }
         Assert.assertEquals(total_ret, 1<<30);
         ret = tfsManager.fetchFile(appId, userId,resourcesPath+"empty.jpg", filepath3+"/largefile");
         Assert.assertTrue(ret);
         
         //verify the file
         Assert.assertEquals(getCrc(resourcesPath+"1g.jpg"),getCrc(resourcesPath+"empty.jpg"));
         
         file_info = tfsManager.lsFile(appId, userId,filepath3+"/largefile");
         
         Assert.assertEquals(file_info.getLength(), 1<<30);
         Assert.assertTrue(file_info.isFile());
         Assert.assertEquals("largefile", file_info.getFileName());
         
         ret = tfsManager.rmFile(appId, userId,filepath3+"/largefile");
         Assert.assertTrue(ret);
         
         file_info_list = tfsManager.lsDir(appId, userId,filepath3);
		     Assert.assertEquals(file_info_list.size(), 0);
         
         ret = tfsManager.rmDir(appId, userId,filepath3);
         Assert.assertTrue(ret);
         
         ret = tfsManager.rmDir(appId, userId,filepath2);
         Assert.assertTrue(ret);
         
         ret = tfsManager.rmDir(appId, userId,filepath1);
         Assert.assertTrue(ret);
	}
}
