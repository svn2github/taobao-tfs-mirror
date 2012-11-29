package com.taobao.common.tfs.testcase.function.ds;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;

public class SetCacheSwichTest extends rcTfsBaseCase {

	/*
	 * small file test
	 */
	@Test
	public void localOpenRemoteOpensamllFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte[] data = FileUtility.getByte(localFile);

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);

		String sRet;
		/* set local cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);

		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFile),
				FileUtility.getCrc("tmp"));
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		// tfsManager.destroy();
		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	@Test
	public void localOpenRemoteCloseSamllFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte[] data = FileUtility.getByte(this.localFile);

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);

		String sRet;
		/* set local cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(false);

		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);

		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFile),
				FileUtility.getCrc("tmp"));
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));
		// tfsManager.destroy();
		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	@Test
	public void localCloseRemoteCloseSamllFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte[] data = FileUtility.getByte(this.localFile);

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);

		String sRet;
		/* set local cache */
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(false);

		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);

		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFile),
				FileUtility.getCrc("tmp"));
		Assert.assertFalse(tfsManager.isHitLocalCache(sRet));
		Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));
		// tfsManager.destroy();
		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	@Test
	public void localCloseRemoteOpenSamllFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte[] data = FileUtility.getByte(this.localFile);

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);

		String sRet;
		/* set local cache */
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(true);

		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);

		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFile),
				FileUtility.getCrc("tmp"));
		Assert.assertFalse(tfsManager.isHitLocalCache(sRet));
		Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
		// tfsManager.destroy();
		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	/*
	 * large file test
	 */
	@Test
	public void localOpenRemoteOpenLargellFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte[] data = FileUtility.getByte(localFileL);

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);

		String sRet;
		/* set local cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);

		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL),
				FileUtility.getCrc("tmp"));
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		// tfsManager.destroy();
		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	@Test
	public void localOpenRemoteCloseLargellFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte[] data = FileUtility.getByte(this.localFileL);

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);

		String sRet;
		/* set local cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(false);

		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);

		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL),
				FileUtility.getCrc("tmp"));
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));
		// tfsManager.destroy();
		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	@Test
	public void localCloseRemoteCloseLargellFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte[] data = FileUtility.getByte(this.localFileL);

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);

		String sRet;
		/* set local cache */
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(false);

		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);

		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL),
				FileUtility.getCrc("tmp"));
		Assert.assertFalse(tfsManager.isHitLocalCache(sRet));
		Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));
		// tfsManager.destroy();
		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	@Test
	public void localCloseRemoteOpenLargellFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte[] data = FileUtility.getByte(this.localFileL);

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);

		String sRet;
		/* set local cache */
		tfsManager.setEnableLocalCache(false);
		tfsManager.setEnableRemoteCache(true);

		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);

		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL),
				FileUtility.getCrc("tmp"));
		Assert.assertFalse(tfsManager.isHitLocalCache(sRet));
		Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
		// tfsManager.destroy();
		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}
}