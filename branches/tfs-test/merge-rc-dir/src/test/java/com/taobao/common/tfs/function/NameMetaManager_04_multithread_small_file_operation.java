package com.taobao.common.tfs.function;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import net.sourceforge.groboutils.junit.v1.MultiThreadedTestRunner; 
import net.sourceforge.groboutils.junit.v1.TestRunnable;  
 
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tfs.function.NameMetaBaseCase;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class NameMetaManager_04_multithread_small_file_operation  extends  NameMetaBaseCase{

		public static List<FileMetaInfo>  file_info    = null;
		public static boolean suc_flag=false;

	  @Before
		public  void setUp()  {
	    Assert.assertTrue(tfsManager.createDir(appId, userId,"/public"));
		}

		@After
		public  void tearDown() {
			tfsManager.createDir(appId, userId,"/public");
		}

		@Test
		public void test_01_multi_threads_create_and_delete_different_small_files() throws Throwable {

			TestRunnable tr1 ,tr2, tr3,tr4 ,tr5, tr6;
			tr1 = new Thread1();
			tr2 = new Thread1();
			tr3 = new Thread1();
			tr4 = new Thread1();
			tr5 = new Thread1();
			tr6 = new Thread1();
			TestRunnable[] trs = {tr1,tr2,tr3,tr4,tr5,tr6};
			MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
			mttr.runTestRunnables();
			file_info = tfsManager.lsDir(appId, userId,"/public");
			Assert.assertEquals(file_info.size(), 0);
		}
		
		@Test
		public void test_02_multi_threads_rename_differernt_files_in_same_dir() throws Throwable {

			TestRunnable tr1 ,tr2, tr3,tr4 ,tr5, tr6;
			tr1 = new Thread2();
			tr2 = new Thread2();
			tr3 = new Thread2();
			tr4 = new Thread2();
			tr5 = new Thread2();
			tr6 = new Thread2();
			TestRunnable[] trs = {tr1,tr2,tr3,tr4,tr5,tr6};
			MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
			mttr.runTestRunnables();
			file_info = tfsManager.lsDir(appId, userId,"/public");
			Assert.assertEquals(file_info.size(), 0);
		}
		
		@Test
		public void test_03_multi_threads_move_different_files_to_same_dir() throws Throwable {

			TestRunnable tr1 ,tr2, tr3,tr4 ,tr5, tr6;
			tr1 = new Thread3();
			tr2 = new Thread3();
			tr3 = new Thread3();
			tr4 = new Thread3();
			tr5 = new Thread3();
			tr6 = new Thread3();
			TestRunnable[] trs = {tr1,tr2,tr3,tr4,tr5,tr6};
			MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
			mttr.runTestRunnables();
			file_info = tfsManager.lsDir(appId, userId,"/public");
			Assert.assertEquals(file_info.size(), 0);
		}
	  
		 
		@Test
		public void test_04_multi_threads_create_and_save_and_delete_one_file() throws Throwable {

			TestRunnable tr1 ,tr2, tr3,tr4 ,tr5, tr6;
			tr1 = new Thread5();
			tr2 = new Thread5();
			tr3 = new Thread5();
			tr4 = new Thread5();
			tr5 = new Thread5();
			tr6 = new Thread5();
			TestRunnable[] trs = {tr1 ,tr2, tr3,tr4 ,tr5, tr6};
			MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
			mttr.runTestRunnables();
			
			file_info = tfsManager.lsDir(appId, userId,"/public");
			Assert.assertEquals(file_info.size(), 0);
			
		}
	    
		@Test
		public void test_05_multi_threads_move_one_directory() throws Throwable {

			TestRunnable tr1 ,tr2, tr3,tr4 ,tr5, tr6;
			tr1 = new Thread6();
			tr2 = new Thread6();
			tr3 = new Thread6();
			tr4 = new Thread6();
			tr5 = new Thread6();
			tr6 = new Thread6();
			TestRunnable[] trs = {tr1 ,tr2, tr3,tr4 ,tr5, tr6};
			MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
			mttr.runTestRunnables();
			
			file_info = tfsManager.lsDir(appId, userId,"/public");
			Assert.assertEquals(file_info.size(), 0);
		}
		
		@Test
		public void test_06_multi_threads_rename_one_directory() throws Throwable {

			TestRunnable tr1 ,tr2;
			tr1 = new Thread7();
			tr2 = new Thread7();
		
			TestRunnable[] trs = {tr1,tr2};
			MultiThreadedTestRunner mttr = new MultiThreadedTestRunner(trs);
			mttr.runTestRunnables();
			file_info = tfsManager.lsDir(appId, userId,"/public");
			Assert.assertEquals(file_info.size(), 0);
		}
		
		//Threads for every test case to call
	    private class Thread1 extends TestRunnable {
			@Override
			public void runTest() throws Throwable {
				//线程要调用的方法或者要执行的操作
				create_different_files();
			}
		}
		private class Thread2 extends TestRunnable {
			@Override
			public void runTest() throws Throwable {
				//线程要调用的方法或者要执行的操作
				rename_different_files();
			}
		}
		private class Thread3 extends TestRunnable {
			@Override
			public void runTest() throws Throwable {
				//线程要调用的方法或者要执行的操作
				move_different_files();
			}
		}

	    private class Thread5 extends TestRunnable {
			@Override
			public void runTest() throws Throwable {
				//线程要调用的方法或者要执行的操作
				create_and_save_and_delete_one_file();
			}
		}
	    private class Thread6 extends TestRunnable {
			@Override
			public void runTest() throws Throwable {
				//线程要调用的方法或者要执行的操作
				move_one_file();
			}
		}
	    private class Thread7 extends TestRunnable {
			@Override
			public void runTest() throws Throwable {
				//线程要调用的方法或者要执行的操作
				rename_one_file();
			}
		}

	    public void create_different_files() throws Exception 
		{    	 
			 log.info("线程===" + Thread.currentThread().getId() + "执行开始");
			 boolean ret;

	     String filepath =String.valueOf(Thread.currentThread().getId());
	         
	     log.info("filepath"+filepath);
	         
			 ret = tfsManager.createDir(appId, userId,"/public/"+filepath);
	     Assert.assertTrue(ret);
	         
			 ret = tfsManager.saveFile(appId, userId,resourcesPath+"1m.jpg","/public/"+filepath+"/newfile");
	     Assert.assertTrue(ret);
	         
			 ret = tfsManager.rmFile(appId, userId,"/public/"+filepath+"/newfile");
			 Assert.assertTrue(ret);

			 ret = tfsManager.rmDir(appId, userId,"/public/"+filepath);
			 Assert.assertTrue(ret);

		}
		public void create_and_save_and_delete_one_file() throws Exception 
		{    	 
			 log.info("线程===" + Thread.currentThread().getId() + "执行开始");
	     //cannot verify the result of every single step ,do that in the last step
			 tfsManager.createDir(appId, userId,"/public/bus");
             
			 tfsManager.saveFile(appId, userId,resourcesPath+"1m.jpg", "/public/bus/blogbus");
			 
       tfsManager.rmFile(appId, userId,"/public/bus/blogbus");
			 
       tfsManager.rmDir(appId, userId,"/public/bus");


		}
		public void rename_different_files() throws Exception 
		{    	 
			 log.info("线程===" + Thread.currentThread().getId() + "执行开始");
			 boolean ret;

	     String filepath =String.valueOf(Thread.currentThread().getId());
	         
	     log.info("filepath"+filepath);
	         
			 ret = tfsManager.createDir(appId, userId,"/public/"+filepath);
	     Assert.assertTrue(ret);
	         
			 ret = tfsManager.createFile(appId, userId,"/public/"+filepath+"/newfile");
	     Assert.assertTrue(ret);
	         
			 ret = tfsManager.mvFile(appId, userId,"/public/"+filepath+"/newfile","/public/"+filepath+"/renamed");
	     Assert.assertTrue(ret);
			 
	     ret = tfsManager.rmFile(appId, userId,"/public/"+filepath+"/renamed");
			 Assert.assertTrue(ret);

			 ret = tfsManager.rmDir(appId, userId,"/public/"+filepath);
			 Assert.assertTrue(ret);
		}

		public void rename_one_file() throws Exception 
		{    	 
      log.info("线程===" + Thread.currentThread().getId() + "执行开始");
	        
			tfsManager.createDir(appId, userId,"/public/test");
			 
			tfsManager.saveFile(appId, userId,resourcesPath+"1m.jpg", "/public/test/bus");
	         
	    tfsManager.mvFile(appId, userId,"/public/test/bus","/public/test/renamed");
	         
	    tfsManager.rmFile(appId, userId,"/public/test/renamed");
	         
	    tfsManager.rmDir(appId, userId,"/public/test");
		}

		public void move_different_files() 
		{    	 
			 log.info("线程===" + Thread.currentThread().getId() + "执行开始");
			 boolean ret;

	     String filepath = String.valueOf(Thread.currentThread().getId());
	     String filename = String.valueOf(Thread.currentThread().getId());
	         
	     log.info("filepath"+filepath);
	         
			 ret = tfsManager.createDir(appId, userId,"/public"+"/"+filepath);
	     Assert.assertTrue(ret);
     
			 ret = tfsManager.createFile(appId, userId,"/public"+"/"+filepath+"/"+filename);
	     Assert.assertTrue(ret);
	         
			 ret = tfsManager.mvFile(appId, userId,"/public"+"/"+filepath+"/"+filename,"/public/"+filename);
	     Assert.assertTrue(ret);
			 
			 ret = tfsManager.rmFile(appId, userId,"/public/"+filename);
	     Assert.assertTrue(ret);
	         
	     ret = tfsManager.rmDir(appId, userId,"/public/"+filepath);
			 Assert.assertTrue(ret);
		}

		public void move_one_file() throws Exception 
		{    	 
			 log.info("线程===" + Thread.currentThread().getId() + "执行开始");
		        
			 tfsManager.createDir(appId, userId,"/public/test");
			 
			 tfsManager.saveFile(appId, userId,resourcesPath+"1m.jpg", "/public/test/bus");
	         
	     tfsManager.mvFile(appId, userId,"/public/test/bus","/public/bus");
	         
	     tfsManager.rmFile(appId, userId,"/public/bus");
	         
	     tfsManager.rmDir(appId, userId,"/public/test");
		}
	}

