package com.taobao.common.tfs.testcase.function.rc;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.taobao.common.tfs.config.TfsStatus;
import com.taobao.common.tfs.testcase.BaseCase;
import com.taobao.common.tfs.utility.FileUtility;
import com.taobao.common.tfs.utility.TimeUtility;

public class QuoteCalculationTest extends BaseCase {
	private String appKey;
	@Before
	public void setUp(){
		tfsManager = createTfsManager();
		appKey = tfsManager.getAppKey();
	}
	
	
	
	@Test
	public void testQuoteLessThanMax(){
		log.info("begin: " + getCurrentFunctionName());
		
		TfsStatus tfsStatus = new TfsStatus();
		tfsStatus.resetCurrentQuote(appKey);
		
		TimeUtility.sleep(MAX_STAT_TIME);
		
		// small file
		testQuote("3M.jpg");
		// large file
		testQuote("1G.jpg");
		
		log.info("end: " + getCurrentFunctionName());
	}
	
	private void testQuote(String localFile){
		TfsStatus tfsStatus = new TfsStatus();
		TimeUtility.sleep(MAX_STAT_TIME);
		
		boolean result = false;
		long max_quote = tfsStatus.getMaxQuote(appKey);
		long old_quote,new_quote,expect_quote,actual_quote;
		ArrayList<String> names = new ArrayList<String>();
		old_quote = tfsStatus.getCurrentQuote(appKey);
		
		long fileSize = 0;
		try {
			fileSize = FileUtility.getFileSize(localFile);
		} catch (FileNotFoundException e) {
			Assert.assertTrue(false);
			e.printStackTrace();
		}
		
		for (int i = 0; i < 10; i++) {
			String tfsname = tfsManager.saveLargeFile(localFile, null, null);
			if (tfsname == null) {
				log.warn("save small file failed");
				break;
			}
			log.debug("@@ tfsname: " + tfsname);
			names.add(tfsname);
		}
		TimeUtility.sleep(MAX_STAT_TIME);
		new_quote = tfsStatus.getCurrentQuote(appKey);
		log.debug("max_quote: " + max_quote + ", old_quote: " + old_quote
				+ ", new_quote: " + new_quote);

		result = (new_quote < max_quote);
		Assert.assertTrue(result);
		expect_quote = (long) 10 * fileSize;
		actual_quote = (new_quote - old_quote);
		log.debug("@@ expect quote: " + expect_quote + ", actual quote: "
				+ actual_quote);
		Assert.assertEquals(expect_quote, actual_quote);

		old_quote = new_quote;
		for (int i = 0; i < 5; i++) {
			boolean ret = tfsManager.unlinkFile(names.get(i), null);
			if (!ret) {
				log.warn("unlinkFile " + names.get(i) + " failed.");
				break;
			}
		}
		TimeUtility.sleep(MAX_STAT_TIME);
		new_quote = tfsStatus.getCurrentQuote(appKey);
		log.debug("max_quote: " + max_quote + ", old_quote: " + old_quote
				+ ", new_quote: " + new_quote);

		result = (new_quote < max_quote);
		Assert.assertTrue(result);
		expect_quote = (long) 5 * fileSize;
		actual_quote = (old_quote - new_quote);
		log.debug("@@ expect quote: " + expect_quote + ", actual quote: "
				+ actual_quote);
		Assert.assertEquals(expect_quote, actual_quote);
	}
}
