package com.taobao.common.tfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.zip.CRC32;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ERcBaseCase
{
	public static String appIp = "10.13.88.118";
	public static String appKey = "foobarRcAA";
	public static String rcAddr = "10.232.36.206:6261";
	public DefaultTfsManager tfsManager = null;
	public String caseName = "";
	public static ArrayList<String> testFileList = new ArrayList<String>();
	protected Logger log = Logger.getLogger("TfsTest");
	
	@BeforeClass
	public static void BeforeSetup() {
		System.out.println(" @@ beforeclass begin");
		String localFile = "";
		// 1b file
		localFile = "1b.jpg";
		if (createFile(localFile, 1)) {
			testFileList.add(localFile);
		}
		// 1k file
		localFile = "1k.jpg";
		if (createFile(localFile, 1 << 10)) {
			testFileList.add(localFile);
		}
		// 10k file
		localFile = "10k.jpg";
		if (createFile(localFile, 10 * (1 << 10)) ) {
			testFileList.add(localFile);
		}
		// 100k file
		localFile = "100k.jpg";
		if (createFile(localFile, 100 * (1 << 10)) ) {
			testFileList.add(localFile);
		}
		// 2M file
		localFile = "2M.jpg";
		if (createFile(localFile, 2 * (1 << 20))) {
			testFileList.add(localFile);
		}
		// 3M file
		localFile = "3M.jpg";
		if (createFile(localFile, 3 * (1 << 20))) {
			testFileList.add(localFile);
		}

		// 10M file
		localFile = "10M.jpg";
		if (createFile(localFile, 10 * (1 << 20))) {
			testFileList.add(localFile);
		}
		
		// 6G file
		localFile = "6G.jpg";
		if (createFile(localFile, (long)6 * (1 << 30))) {
			testFileList.add(localFile);
		}
	}

	@AfterClass
	public static void AfterTearDown() {

		System.out.println(" @@ afterclass begin");
		int size = testFileList.size();
		for (int i = 0; i < size; i++) {
			File file = new File(testFileList.get(i));
			if (file.exists()) {
				file.delete();
			}
		}
	}
	
	protected int getCrc(String fileName) 
	{
		try 
		{
			FileInputStream input = new FileInputStream(fileName);
			int readLength;
			byte[] data = new byte[102400];
			CRC32 crc = new CRC32();
			crc.reset();
			while ((readLength = input.read(data, 0, 102400)) > 0) 
			{
				crc.update(data, 0, readLength);
			}
			input.close();
			System.out.println(" file name crc " + fileName + " c "+ crc.getValue());
			return (int) crc.getValue();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		return 0;
	}
	
	protected static boolean createFile(String filePath, long size) 
	{
		boolean ret = true;
		try 
		{
			RandomAccessFile f = new RandomAccessFile(filePath, "rw");
			f.setLength(size);
		} 
		catch (Exception e) 
		{
			ret = false;
			e.printStackTrace();
		}
		return ret;
	}
	
    protected byte[]  getByte(String fileName) throws IOException
    {
      InputStream in = new FileInputStream(fileName);
      byte[] data= new byte[in.available()];
      in.read(data);
      return data;
    }
}