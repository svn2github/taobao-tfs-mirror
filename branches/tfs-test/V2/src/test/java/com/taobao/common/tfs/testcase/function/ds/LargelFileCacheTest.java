package com.taobao.common.tfs.testcase.function.ds;

import static org.junit.Assert.assertTrue;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.impl.FSName;
import com.taobao.common.tfs.impl.LocalKey;
import com.taobao.common.tfs.impl.SegmentInfo;
import com.taobao.common.tfs.testcase.rcTfsBaseCase;
import com.taobao.common.tfs.utility.FileUtility;

public class LargelFileCacheTest extends rcTfsBaseCase {
	public List<Long> dsList = new ArrayList<Long>();

	/*
	 * 修改全部分片的缓存
	 */

	@Test
	public void localExsitRemoteExistLargelFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte[] data = FileUtility.getByte(localFileL);

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);

		String sRet;
		/* set local remote cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		/* Write file */
		sRet = tfsManager.saveFile(data, null, null);
		Assert.assertNotNull(sRet);
		/* Read file */
		tfsManager.removeLocalCache(sRet);
		tfsManager.removeRemoteCache(sRet);
		Assert.assertFalse(tfsManager.isHitLocalCache(sRet));
		Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFile),
				FileUtility.getCrc("tmp"));
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));

		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);

		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	@Test
	public void localExistRemoteNonexsitentLargelFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);

		String sRet;
		/* set cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		/* Write file */
		sRet = tfsManager.saveLargeFile(localFileL, null, null);
		Assert.assertNotNull(sRet);
		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL),
				FileUtility.getCrc("tmp"));
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));
		// remove remote cache
		tfsManager.removeRemoteCache(sRet);

		// read file form local cache
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	@Test
	public void localNonexsitentRemoteNonexsitentLargelFile() throws Exception {
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
		tfsManager.setEnableRemoteCache(true);

		/* Write file */
		sRet = tfsManager.saveLargeFile(localFileL, null, null);
		Assert.assertNotNull(sRet);
		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL),
				FileUtility.getCrc("tmp"));

		// remove remote local cache
		tfsManager.removeRemoteCache(sRet);
		tfsManager.removeLocalCache(sRet);
		Assert.assertFalse(tfsManager.isHitLocalCache(sRet));
		Assert.assertFalse(tfsManager.isHitRemoteCache(sRet));
		// read file form ds
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
		// tfsManager.destroy();
		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	@Test
	public void localNonexsitentRemoteExistLargelFile() throws Exception {
		boolean bRet = false;
		OutputStream output = new FileOutputStream("tmp");
		byte[] data = FileUtility.getByte(this.localFileL);

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");
		tfsManager.setRemoteCacheInfo(tairMasterAddr, tairSlaveAddr,
				tairGroupName, 1);
		String sRet;
		/* Write file */
		sRet = tfsManager.saveLargeFile(localFileL, null, null);
		Assert.assertNotNull(sRet);

		/* set local remote cache */
		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		/* Read file */
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL),
				FileUtility.getCrc("tmp"));
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));

		// remove local cache
		tfsManager.removeLocalCache(sRet);
		// read file form ds
		bRet = tfsManager.fetchFile(sRet, null, output);
		Assert.assertTrue(bRet);
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
		// tfsManager.destroy();
		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===========> end");
	}

	/*
	 * 修改部分分片的缓存
	 */

	@Test
	public void localNonexsitenPartRemoteExistLargelFile() throws Exception {
		String segmentFile = "segment_info";
		OutputStream output = new FileOutputStream(segmentFile);

		boolean bRet = false;

		log.info(new Throwable().getStackTrace()[0].getMethodName()
				+ "===> start");

		tfsManager.setEnableLocalCache(true);
		tfsManager.setEnableRemoteCache(true);

		/* save one 10M large file, then fetch it */
		String sFileName = tfsManager.saveLargeFile(localFileL, null, null);
		String sRet=sFileName;
		bRet = tfsManager.fetchFile(sFileName, null, "temp");
		assertTrue(bRet);
		Assert.assertEquals(FileUtility.getCrc(localFileL),
				FileUtility.getCrc("temp"));

		sFileName = "T" + sFileName.substring(1);
		bRet = tfsManager.fetchFile(sFileName, null, output);
		assertTrue(bRet);
		output.flush();
		output.close();
		/* get segment info */
		LocalKey localKey = new LocalKey();
		localKey.loadFile(segmentFile);
		TreeSet<SegmentInfo> segmengInfoSet = localKey.getSegmentInfos();
		int count = 0;
		for (SegmentInfo segInfo : segmengInfoSet) {
			FSName fsName = new FSName(segInfo.getBlockId(),
					segInfo.getFileId());
			String fileName = fsName.get();
			try {
				if (count == 2) {
					for (SegmentInfo segInfo1 : segmengInfoSet) {
						FSName fsName1 = new FSName(segInfo1.getBlockId(),
								segInfo1.getFileId());
						String fileName1 = fsName1.get();
						tfsManager.removeLocalBlockCache(fileName1);
					}
				}
			
			} catch (Exception e) {
				System.out.println("@@@FSname is " + fsName.get());
			}
			count++;
		}
		assertTrue(tfsManager.fetchFile(sRet, null, output));
		Assert.assertTrue(tfsManager.isHitLocalCache(sRet));
		Assert.assertTrue(tfsManager.isHitRemoteCache(sRet));
	}
}