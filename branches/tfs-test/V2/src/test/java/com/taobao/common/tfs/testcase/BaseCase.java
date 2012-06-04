package com.taobao.common.tfs.testcase;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.utility.FileUtility;

public class BaseCase {
	public static DefaultTfsManager tfsManager = null;
	public static String resourcesPath = "";

	public static int INVALID_MODE = 0;
	public static int R_MODE = 1;
	public static int RW_MODE = 2;
	public static int RC_VALID_MODE = 1;
	public static int RC_INVALID_MODE = 2;

	public static long appId;
	public static long userId = 8;
	public static String appKey = "testKeyYS";
	//public static String appKey = "regression";
	protected final int MAX_UPDATE_TIME = 20;
	protected final int MAX_STAT_TIME = 4;
	public static String key = "1k.jpg";

	protected List<String> tfsNames = new ArrayList<String>();
	protected List<String> metaFiles = new ArrayList<String>();

	private static ArrayList<String> testFileList = new ArrayList<String>();
	protected static Log log = LogFactory.getLog(BaseCase.class);

	static {
		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(
				new String[] { "tfs.xml" });
		tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
		appId = tfsManager.getAppId();
		System.out.println("appid is : " + appId);
		// appKey = tfsManager.getAppKey();
	}

	public String currentDateTime() {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddhhmm");
		return dateFormat.format(new Date());
	}

	public String getCurrentFunctionName() {
		return Thread.currentThread().getStackTrace()[2].getMethodName();
	}

	public DefaultTfsManager createTfsManager() {
		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(
				new String[] { "tfs.xml" });
		DefaultTfsManager tfsManager = (DefaultTfsManager) appContext
				.getBean("tfsManager");
		appId = tfsManager.getAppId();

		return tfsManager;
	}

	/**
	 * MD5 哈希函数(对文件哈希)
	 * 
	 * @param path
	 * @return
	 */
	public static StringBuilder fileMd5(String path) {
		StringBuilder sb = new StringBuilder();
		StringBuilder noAlgorithm = new StringBuilder(
				"无MD5算法，这可能是你的JDK/JRE版本太低");
		StringBuilder fileNotFound = new StringBuilder("未找到文件，请重新定位文件路径");
		StringBuilder IOerror = new StringBuilder("文件流输入错误");
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5"); // 生成MD5类的实例
			File file = new File(path); // 创建文件实例，设置路径为方法参数
			FileInputStream fs = new FileInputStream(file);
			BufferedInputStream bi = new BufferedInputStream(fs);
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			byte[] b = new byte[bi.available()]; // 定义字节数组b大小为文件的不受阻塞的可访问字节数
			int i;
			// 将文件以字节方式读到数组b中
			while ((i = bi.read(b, 0, b.length)) != -1) {
			}
			md5.update(b);// 执行MD5算法
			for (byte by : md5.digest()) {
				sb.append(String.format("%02X", by)); // 将生成的字节MD５值转换成字符串
			}
			bo.close();
			bi.close();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			return noAlgorithm;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			return fileNotFound;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			return IOerror;
		}
		return sb;// 返回MD5串
	}
	
	/**
     * MD5 哈希函数(对字符串哈希)
     * @param plainByte
     * @param num
     * @return
     */
    public static String md5(byte[] plainByte) { 
            try { 
                    MessageDigest md = MessageDigest.getInstance("MD5"); 
                    md.update(plainByte); 
                    byte b[] = md.digest(); 
                    int i; 
                    StringBuffer buf = new StringBuffer(""); 
                    for (int offset = 0; offset < b.length; offset++) { 
                            i = b[offset]; 
                            if(i<0) i+= 256; 
                            if(i<16) 
                            buf.append("0"); 
                            buf.append(Integer.toHexString(i)); 
                    } 
                    return buf.toString();
            } catch (Exception e) { 
                    e.printStackTrace(); 
            } 
            return null;
    } 
	 protected byte[]  getByte(String fileName) throws IOException
	    {
	    	   InputStream in = new FileInputStream(fileName);
	           byte[] data= new byte[in.available()];
	           in.read(data);
	           return data;
	    }
	@BeforeClass
	public static void beforeSetup() {
		log.debug(" @@ beforeclass begin");

		Map<String, Integer> fileMap = new HashMap<String, Integer>() {
			private static final long serialVersionUID = 1L;

			{
				put("1B.jpg", 1);
				put("2B.jpg", 2);
				put("1K.jpg", 1 << 10);
				put("10K.jpg", 10 * (1 << 10));
				put("100K.jpg", 100 * (1 << 10));
				put("1M.jpg", 1 << 20);
				put("2M.jpg", 2 * (1 << 20));
				put("3M.jpg", 3 * (1 << 20));
				put("4M.jpg", 4 * (1 << 20));
				put("5M.jpg", 5 * (1 << 20));
				put("10M.jpg", 10 * (1 << 20));
				put("20M.jpg", 20 * (1 << 20));
				put("50M.jpg", 50 * (1 << 20));
				put("100M.jpg", 100 * (1 << 20));
				put("1G.jpg", 1 << 30);
				put("6G.jpg", 6 * (1 << 30));
			}
		};

		for (String file : fileMap.keySet()) {
			testFileList.add(file);
			FileUtility.createFile(file, fileMap.get(file));
		}

	}

	@AfterClass
	public static void afterTearDown() {

		System.out.println(" @@ afterclass begin");
		int size = testFileList.size();
		for (int i = 0; i < size; i++) {
			File file = new File(testFileList.get(i));
			System.out.println("file name: " + testFileList.get(i));
			if (file.exists()) {
				file.delete();
			}
		}
	}

	@After
	public void tearDown() {
		if (tfsManager == null) {
			return;
		}

		for (String each : tfsNames) {
			log.info("unlink file: " + each);
			tfsManager.unlinkFile(each, null);
		}

		for (String each : metaFiles) {
			tfsManager.rmFile(appId, userId, each);
		}
	}
}
