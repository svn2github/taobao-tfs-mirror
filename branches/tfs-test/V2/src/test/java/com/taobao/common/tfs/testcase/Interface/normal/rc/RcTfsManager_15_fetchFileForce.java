package com.taobao.common.tfs.RcITest_2_2_3;

import org.junit.Test;

import junit.framework.Assert;

import com.taobao.common.tfs.tfsNameBaseCase;

public class RcTfsManager_15_fetchFileForce extends tfsNameBaseCase {
	private final int FILE_CONCEAL = 1;
	private static int UNDELETE = 0x2;
	
	@Test
	public void saveSmallFileNotHideFetchForceTest_01(){
		String caseTitle = Thread.currentThread().getStackTrace()[1].getMethodName();
		log.info( "begin: " + caseTitle);
		
		int savecrc;
		int fetchcrc;
		String tfsName="";
		
		tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		savecrc=getCrc(resourcesPath+"100K.jpg");
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean fetchResult;
		fetchResult = tfsManager.fetchFileForce(tfsName, null, resourcesPath+"TEMP");
		Assert.assertTrue(fetchResult);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
	public void saveSmallFileHideFetchForceTest_02(){
		String caseTitle = Thread.currentThread().getStackTrace()[1].getMethodName();
		log.info( "begin: " + caseTitle);
		
		int savecrc;
		int fetchcrc;
		String tfsName="";
		
		tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		savecrc=getCrc(resourcesPath+"100K.jpg");
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean fileOperationRet = false;
		fileOperationRet = tfsManager.hideFile(tfsName, null, FILE_CONCEAL);
		Assert.assertTrue(fileOperationRet);
		
		fileOperationRet = tfsManager.fetchFileForce(tfsName, null, resourcesPath+"TEMP");
		Assert.assertTrue(fileOperationRet);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
	public void saveLargeFileNotHideFetchForceTest_03(){
		String caseTitle = Thread.currentThread().getStackTrace()[1].getMethodName();
		log.info( "begin: " + caseTitle);
		
		int savecrc;
		int fetchcrc;
		String tfsName="";
		
		tfsName=tfsManager.saveLargeFile(resourcesPath+"10M.jpg", null, null);
		savecrc=getCrc(resourcesPath+"10M.jpg");
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean fetchResult;
		fetchResult = tfsManager.fetchFileForce(tfsName, null, resourcesPath+"TEMP");
		Assert.assertTrue(fetchResult);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
	}
	
	@Test
	public void saveLargeFileHideFetchForceTest_04(){
		String caseTitle = Thread.currentThread().getStackTrace()[1].getMethodName();
		log.info( "begin: " + caseTitle);
		
		int savecrc;
		int fetchcrc;
		String tfsName="";
		
		tfsName=tfsManager.saveLargeFile(resourcesPath+"10M.jpg", null, null);
		savecrc=getCrc(resourcesPath+"10M.jpg");
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean fileOperationRet = false;
		fileOperationRet = tfsManager.hideFile(tfsName, null, FILE_CONCEAL);
		Assert.assertTrue(fileOperationRet);
		
		fileOperationRet = tfsManager.fetchFileForce(tfsName, null, resourcesPath+"TEMP");
		Assert.assertTrue(fileOperationRet);
		
		fetchcrc=getCrc(resourcesPath+"TEMP");
		Assert.assertEquals(savecrc, fetchcrc);
		
	}
	
	@Test
	public void saveSmallFileUnlinkFetchForceTest_05(){
		String caseTitle = Thread.currentThread().getStackTrace()[1].getMethodName();
		log.info( "begin: " + caseTitle);
		
		String tfsName="";
		
		tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean fileOperationResult;
		fileOperationResult = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(fileOperationResult);
		fileOperationResult = tfsManager.fetchFileForce(tfsName, null, resourcesPath+"TEMP");
		System.out.println(resourcesPath+"TEMP");
		Assert.assertFalse(fileOperationResult);
		
	}
	
	@Test
	public void saveLargeFileUnlinkFetchForceTest_06(){
		String caseTitle = Thread.currentThread().getStackTrace()[1].getMethodName();
		log.info( "begin: " + caseTitle);
		
		String tfsName="";
		
		tfsName=tfsManager.saveLargeFile( resourcesPath+"10M.jpg",null,null);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean fileOperationResult;
		fileOperationResult = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(fileOperationResult);
		fileOperationResult = tfsManager.fetchFileForce(tfsName, null, resourcesPath+"TEMP");
		System.out.println(resourcesPath+"TEMP");
		Assert.assertFalse(fileOperationResult);
	}
	
	@Test
	public void saveFileHideUnlinkFetchForceTest_07(){
		String caseTitle = Thread.currentThread().getStackTrace()[1].getMethodName();
		log.info( "begin: " + caseTitle);
		
		String tfsName="";
		
		tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean fileOperationResult;
		fileOperationResult = tfsManager.hideFile(tfsName, null, FILE_CONCEAL);
		Assert.assertTrue(fileOperationResult);
		
		fileOperationResult = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(fileOperationResult);
		
		fileOperationResult = tfsManager.fetchFileForce(tfsName, null, resourcesPath+"TEMP");
		Assert.assertFalse(fileOperationResult);
	}
	
	@Test
	public void saveFileHideUnlinkLinkFetchForceTest_08(){
		String caseTitle = Thread.currentThread().getStackTrace()[1].getMethodName();
		log.info( "begin: " + caseTitle);
		
		String tfsName="";
		
		tfsName=tfsManager.saveFile( resourcesPath+"100K.jpg",null,null,false);
		Assert.assertNotNull(tfsName);
		log.info("The tfs file name is "+ tfsName);
		
		boolean fileOperationResult;
		fileOperationResult = tfsManager.hideFile(tfsName, null, FILE_CONCEAL);
		Assert.assertTrue(fileOperationResult);
		
		fileOperationResult = tfsManager.unlinkFile(tfsName, null);
		Assert.assertTrue(fileOperationResult);
		
		fileOperationResult = tfsManager.fetchFileForce(tfsName, null, resourcesPath+"TEMP");
		Assert.assertFalse(fileOperationResult);
		
		fileOperationResult = tfsManager.unlinkFile(tfsName, null,UNDELETE);
		Assert.assertTrue(fileOperationResult);
		
		fileOperationResult = tfsManager.fetchFileForce(tfsName, null, resourcesPath+"TEMP");
		Assert.assertTrue(fileOperationResult);
		
		fileOperationResult = tfsManager.fetchFile(tfsName, null, resourcesPath+"TEMP");
		Assert.assertTrue(fileOperationResult);
	}
	

	
}
