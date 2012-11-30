package com.taobao.common.tfs.testcase.function.meta;


import java.util.ArrayList;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;

import org.junit.AfterClass;

import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.common.tfs.testcase.metaTfsBaseCase;
import com.taobao.common.tfs.common.TfsConstant;



public class NameMetaManager_02_multithread_directory_operation extends  metaTfsBaseCase
{
	public static String rootDir = "/NameMetaTest2";
	

	@BeforeClass
	public  static void setUpOnce() throws Exception 
	{
		//rmDirRecursive(appId, userId, rootDir); 
	    tfsManager.createDir(appId, userId, rootDir); 
	    
	}

	@AfterClass
	public static void tearDownOnce() throws Exception
	{
		rmDirRecursive(appId, userId, rootDir); 
	}
	  
	public class OperationFunction
	{
  
		public void actuator(int command)
		{
			switch(command)
			{
				case 0:CreateDir();break;
				case 1:create_and_delete_one_directory();break;
				case 2:rename_different_directories();break;
				case 3:rename_one_directory();break;
				case 4:move_different_directories();break;
				case 5:move_one_directory();break;
				case 6:delete_different_directories();break;
			}
		}

		public void CreateDir()
		{
			long count = 1000;
			while (--count > 0) 
		    {
				int iRet = -1;
				iRet = tfsManager.createDir(appId, userId, rootDir + "/" + Thread.currentThread().getId() + "/" + String.valueOf(count));
				Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
			}
		      
		}
    	public void create_and_delete_one_directory()
    	{
        	int iRet = -1;
		    String filepath = rootDir + "/public";
		    String subDir = filepath + "/subdir";
		    String suberDir = subDir + "/suberdir";

		    iRet=tfsManager.createDir(appId, userId, filepath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		    iRet=tfsManager.createDir(appId, userId, subDir);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		    iRet=tfsManager.createDir(appId, userId, suberDir);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		    iRet=tfsManager.rmDir(appId, userId, suberDir);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		    iRet=tfsManager.rmDir(appId, userId, subDir);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		    iRet=tfsManager.rmDir(appId, userId, filepath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		}

    	public void rename_different_directories()
		{
    		int iRet = -1;
		    long threadId = Thread.currentThread().getId();
		    String filepath = rootDir + "/" + String.valueOf(threadId);
		    String subDir = filepath + "/subdir";
		    String renamedDir = filepath + "/renamed";

		    iRet = tfsManager.createDir(appId, userId, filepath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		    iRet = tfsManager.createDir(appId, userId, subDir);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		    iRet = tfsManager.mvDir(appId, userId, subDir, renamedDir);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		    iRet = tfsManager.rmDir(appId, userId, renamedDir);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		    iRet = tfsManager.rmDir(appId, userId, filepath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		}

		public void rename_one_directory() 
		{  
			int iRet = -1;
		    String filepath = rootDir + "/public/test";
		    String newpath = rootDir + "/public/renamed";

		    iRet=tfsManager.createDir(appId, userId, filepath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		    iRet=tfsManager.mvDir(appId, userId, filepath, newpath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		    iRet=tfsManager.rmDir(appId, userId, newpath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		}

		public void move_different_directories() 
		{    	 
			int iRet = -1;
		    long threadId = Thread.currentThread().getId();
		    String filepath = rootDir + "/" + String.valueOf(threadId);
		    String newpath = rootDir + "/moved_" + String.valueOf(threadId);

		    iRet = tfsManager.createDir(appId, userId, filepath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		    iRet= tfsManager.mvDir(appId, userId, filepath, newpath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		    iRet = tfsManager.rmDir(appId, userId, newpath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);   
		}

		public void move_one_directory() 
		{    	 
			int iRet = -1;
		    String filepath = rootDir + "/public/test";
		    String newpath = rootDir + "/moved";

		    iRet = tfsManager.createDir(appId, userId, filepath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		    iRet=  tfsManager.mvDir(appId, userId, filepath, newpath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		    iRet = tfsManager.rmDir(appId, userId, newpath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		}

		public void delete_different_directories()
		{    	 
			int iRet = -1;
		    long threadId = Thread.currentThread().getId();

		    String filepath = rootDir + "/" + String.valueOf(threadId);

		    iRet = tfsManager.createDir(appId, userId, filepath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);

		    iRet = tfsManager.rmDir(appId, userId, filepath);
		    Assert.assertTrue(iRet==TfsConstant.TFS_SUCCESS);
		}
	}
	
	class Task implements Callable<String>
	{
	    private int id;
	    private OperationFunction op;

	    public Task(int id)
	    {
	    	this.op = new OperationFunction();
	        this.id = id;
	    }

	    public String call() throws Exception
	    {
	    	op.actuator(id);
	        return "result of Task " + id;
	    }
	}

	public void wait (ArrayList<Future<String>> results)
	{
		boolean wait = true;
		int i;
		while(wait)
		{
			wait = false;
			try 
			{
				Thread.sleep(1000L);
			} 
			catch (InterruptedException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for(i=0;i<results.size();i++)
			{
				if(!results.get(i).isDone())
				{
					wait = true;
					break;
				}
			}
		}
	}
	@Test
	public void test_01_multi_threads_create_different_directories()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 10; i++)
        {
            results.add(exec.submit(new Task(1)));
        }        
        wait(results);
	}
	
	@Test
	public void test_02_multi_threads_rename_differernt_directories_in_same_dir() 
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 3; i++)
        {
            results.add(exec.submit(new Task(2)));
        }        
        wait(results);
	}
	
	@Test
	public void test_03_multi_threads_move_differernt_directories_in_same_dir() 
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 3; i++)
        {
            results.add(exec.submit(new Task(4)));
        }        
        wait(results);
	}
  
	@Test
	public void test_04_multi_threads_delete_different_directories() 
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 6; i++)
        {
            results.add(exec.submit(new Task(6)));
        }        
        wait(results);
	}
    
	@Test
	public void test_05_multi_threads_create_and_delete_one_directory()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 2; i++)
        {
            results.add(exec.submit(new Task(1)));
        }        
        wait(results);
	}
    
	@Test
	public void test_06_multi_threads_move_one_directory()
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 2; i++)
        {
            results.add(exec.submit(new Task(5)));
        }        
        wait(results);
	}
	
	@Test
	public void test_07_multi_threads_rename_one_directory() 
	{
		log.info(new Throwable().getStackTrace()[0].getMethodName());
		ExecutorService exec = Executors.newFixedThreadPool(30);
        ArrayList<Future<String>> results = new ArrayList<Future<String>>();   	
        for (int i = 0; i < 2; i++)
        {
            results.add(exec.submit(new Task(3)));
        }        
        wait(results);
	}
	
}
