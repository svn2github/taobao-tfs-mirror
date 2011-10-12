package com.taobao.common.tfs.performance;

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

public class NametfsManager_06_large_file_operation_interrupted_by_other_ops extends  tfsNameBaseCase{
	
	public  static List<FileMetaInfo>  file_info_list ;
	public  static FileMetaInfo file_info;
    public  static File large_file =new File("src/test/resources/10M.jpg");	
    
	public  static InputStream In = null;  

    
    public  static byte[] data= new byte[1<<21];
   
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
    public  static void get_local_data_with_offset(long data_offset,long len)
    {
   	 long read_length = 0;
 	 long ret = 0;
     try{
     	    while((ret = In.read(data, (int)data_offset, (int)len)) != -1)
	        {
 	    		read_length += ret;
     	    	if(read_length == len)
     	    	{
     	    		break;
     	    	}
     	    }
	        }catch(Exception e){
	           e.printStackTrace();
	        }finally{
	        
	        }
    }
    @BeforeClass
	public  static void setUp() throws Exception {


 
	}
	@AfterClass
	public static void tearDown() throws Exception 
	{

	}
	@Before
	public void Before(){
       
		
		try {
			In = new FileInputStream(large_file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@After
	public void After() {
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
	public void test_01_write_part_of_the_large_file_and_rename_then_go_on()
	{
		 boolean ret = false;
         String filepath3 = "/one";
		 
		 
		 ret = tfsManager.createDir(userId,filepath3);
         Assert.assertTrue(ret);
        
         ret = tfsManager.createFile(userId,filepath3+"/largefile");
         Assert.assertTrue(ret);
         
         long total_ret =0 ;
         long write_ret =0 ;
         long offset = 0 ;
         
         for (int i = 0; i< 5 ;i++)
         {
           if(i < 3){	 
        	 get_local_data_with_offset(offset,1<<21);
        	 log.info("第"+i+"次写入,buffer长度："+data.length+" dataoffset:"+ offset);
             if( total_ret!= 10*(1<<20))
             {
               write_ret = tfsManager.write(userId,filepath3+"/largefile", offset, data, 0, 1<<21);
               Assert.assertEquals(write_ret, 1<<21);
               log.info("第"+i+"次写入"+write_ret);
               
               offset += write_ret;
               total_ret += write_ret;
               log.info("共写入"+total_ret);
             }
           }
           else 
           {   
        	   if (i == 3 )
        	   {
        	     ret = tfsManager.mvFile(userId,filepath3+"/largefile", filepath3+"/renamed");
                 Assert.assertTrue(ret);
                 log.info("改名成功？："+ret);
               }
          	   get_local_data_with_offset(offset,1<<21);
          	   log.info("第"+i+"次写入,buffer长度："+data.length+" dataoffset:"+ offset);
               if( total_ret!= 10*(1<<20))
               {
                 write_ret = tfsManager.write(userId,filepath3+"/renamed", offset, data, 0, 1<<21);
                 Assert.assertEquals(write_ret, 1<<21);
                 log.info("第"+i+"次写入"+write_ret);
                 offset += write_ret;
                 total_ret += write_ret;
                 log.info("共写入"+total_ret);
               }
           }
         }
         log.info("最后共写入"+total_ret);
         Assert.assertEquals(total_ret, 10*(1<<20));
         ret = tfsManager.fetchFile(userId,resourcesPath+"empty.jpg", filepath3+"/renamed");
         Assert.assertTrue(ret);
         
         //verify the file
         Assert.assertEquals(getCrc(res_path+"10M.jpg").getValue(),getCrc(res_path+"empty.jpg").getValue() );
         
        
         ret = tfsManager.rmFile(userId,filepath3+"/renamed");
         Assert.assertTrue(ret);
         
         ret = tfsManager.rmDir(userId,filepath3);
         Assert.assertTrue(ret);
         
}

	@Test
	public void test_02_write_part_of_the_large_file_and_move_then_go_on()
	{
		 boolean ret = false;
         String filepath3 = "/one";
		
		 ret = tfsManager.createDir(userId,filepath3);
         Assert.assertTrue(ret);
        
         ret = tfsManager.createFile(userId,filepath3+"/largefile");
         Assert.assertTrue(ret);
         
         long total_ret =0 ;
         long write_ret =0 ;
         long offset = 0 ;
         
         for (int i = 0; i< 5 ;i++)
         {
           if(i < 3){	 
        	 get_local_data_with_offset(offset,1<<21);
        	 log.info("第"+i+"次写入,buffer长度："+data.length+" dataoffset:"+ offset);
             if( total_ret!= 10*(1<<20))
             {
               write_ret = tfsManager.write(userId,filepath3+"/largefile", offset, data, 0, 1<<21);
               Assert.assertEquals(write_ret, 1<<21);
               log.info("第"+i+"次写入"+write_ret);
               
               offset += write_ret;
               total_ret += write_ret;
               log.info("共写入"+total_ret);
             }
           }
           else 
           {   
        	   if (i == 3 )
        	   {
        	     ret = tfsManager.mvFile(userId,filepath3+"/largefile", "/largefile");
                 Assert.assertTrue(ret);
                 log.info("改名成功？："+ret);
               }
          	   get_local_data_with_offset(offset,1<<21);
          	   log.info("第"+i+"次写入,buffer长度："+data.length+" dataoffset:"+ offset);
               if( total_ret!= 10*(1<<20))
               {
                 write_ret = tfsManager.write(userId,"/largefile", offset, data, 0, 1<<21);
                 Assert.assertEquals(write_ret, 1<<21);
                 log.info("第"+i+"次写入"+write_ret);
                 offset += write_ret;
                 total_ret += write_ret;
                 log.info("共写入"+total_ret);
               }
           }
         }
         log.info("最后共写入"+total_ret);
         Assert.assertEquals(total_ret, 10*(1<<20));
         ret = tfsManager.fetchFile(userId,resourcesPath+"empty.jpg", "/largefile");
         Assert.assertTrue(ret);
         
         //verify the file
         Assert.assertEquals(getCrc(userId,resourcesPath+"10M.jpg").getValue(),getCrc(resourcesPath+"empty.jpg").getValue() );
         
        
         ret = tfsManager.rmFile(userId,"/largefile");
         Assert.assertTrue(ret);
         
         ret = tfsManager.rmDir(userId,filepath3);
         Assert.assertTrue(ret);
         
}

}
