package com.taobao.common.tfs.utility;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.log4j.Logger;

public class FileUtility 
{
	private static Logger logger = Logger.getLogger(FileUtility.class);

	public static long getFileSize(String path) throws FileNotFoundException 
	{
		File f = new File(path);

		if (!f.exists()) 
		{
			throw new FileNotFoundException();
		}

		return f.length();
	}

	public static boolean deleteFile(String path) 
	{
		return false;
	}

	public static boolean compareTwoFiles(String src, String dest) 
	{
		// TODO
		return false;
	}

	public static boolean generateRadomFile(String fileName, long size) 
	{
		byte[] buffer;
		FileOutputStream fos = null;
		int defaultBufferSize = 8 * 1024 * 1024;

		File f = new File(fileName);

		try {
			if (!f.exists()) 
			{
				f.createNewFile();
			}

			fos = new FileOutputStream(f);
			Random random = new Random();

			while (size > 0) 
			{
				if (size >= defaultBufferSize) 
				{
					buffer = new byte[defaultBufferSize];
				}
				
				else 
				{
					buffer = new byte[(int) size];
				}

				random.nextBytes(buffer);
				fos.write(buffer);
				fos.flush();

				size -= defaultBufferSize;
			}

			fos.close();
		} 
		catch (IOException e) 
		{
			logger.error("generate random file error: " + e.getStackTrace());
			return false;
		} 
		finally 
		{
			if (fos != null) 
			{
				try 
				{
					fos.close();
				}
				catch (IOException e) 
				{
					logger.error("close fos fail " + e.getStackTrace());
					return false;
				}
			}
		}

		return true;
	}

	public static StringBuilder fileMd5(String path) 
	{
		StringBuilder sb = new StringBuilder();
		StringBuilder noAlgorithm = new StringBuilder("无MD5算法，这可能是你的JDK/JRE版本太低");
		StringBuilder fileNotFound = new StringBuilder("未找到文件，请重新定位文件路径");
		StringBuilder IOerror = new StringBuilder("文件流输入错误");
			
		try
		{
			MessageDigest md5 = MessageDigest.getInstance("MD5"); // 生成MD5类的实例
			File file = new File(path); // 创建文件实例，设置路径为方法参数
			FileInputStream fs = new FileInputStream(file);
			BufferedInputStream bi = new BufferedInputStream(fs);
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			byte[] b = new byte[bi.available()]; // 定义字节数组b大小为文件的不受阻塞的可访问字节数
			int i;
			// 将文件以字节方式读到数组b中
			while ((i = bi.read(b, 0, b.length)) != -1)
			{
			}
			md5.update(b);// 执行MD5算法
			
			for (byte by : md5.digest())
			{
				sb.append(String.format("%02X", by)); // 将生成的字节MD５值转换成字符串
			}
			bo.close();
			bi.close();
		} 
		
		catch (NoSuchAlgorithmException e) 
		{
			// TODO Auto-generated catch block
			return noAlgorithm;
		} 
		catch (FileNotFoundException e) 
		{
			// TODO Auto-generated catch block
			return fileNotFound;
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			return IOerror;
		}
		return sb;// 返回MD5串
	}
	

    public static String md5(byte[] plainByte)
    { 
    	try { 
    			MessageDigest md = MessageDigest.getInstance("MD5"); 
                md.update(plainByte); 
                byte b[] = md.digest(); 
                int i; 
                StringBuffer buf = new StringBuffer(""); 
                for (int offset = 0; offset < b.length; offset++) 
                { 
                	i = b[offset]; 
                    if(i<0) i+= 256; 
                    if(i<16) 
                    buf.append("0"); 
                    buf.append(Integer.toHexString(i)); 
                } 
                return buf.toString();
            } 
    	catch (Exception e) 
    	{ 
    		e.printStackTrace(); 
        } 
            
    	return null;
    } 
    
	public static int getCrc(OutputStream opstream) 
	{
		try 
		{
			String str = opstream.toString();
			byte[] data = str.getBytes();
			CRC32 crc = new CRC32();
			crc.reset();
			crc.update(data);
			System.out.println(crc.getValue());
			return (int) crc.getValue();
		} 
		catch (Exception e) 
		{
			logger.error("Get crc error: " + e.getStackTrace());
			return -1;
		}
	}

	public static int getCrc(String fileName) 
	{
		FileInputStream input = null;
		try 
		{
			input = new FileInputStream(fileName);
			int readLength;
			byte[] data = new byte[102400];
			CRC32 crc = new CRC32();
			crc.reset();
			while ((readLength = input.read(data, 0, 102400)) > 0) 
			{
				crc.update(data, 0, readLength);
			}
			input.close();
			return (int) crc.getValue();
		} 
		catch (IOException e) 
		{
			logger.error("Get crc error: " + e.getStackTrace());
			return -1;
		} 
		finally 
		{
			if (input != null) 
			{
				try 
				{
					input.close();
				} 
				catch (IOException e) 
				{
					logger.error("Close input stream error: "
							+ e.getStackTrace());
				}
			}
		}
	}

	public static boolean createFile(String filePath, long size) 
	{
		boolean ret = true;
		try 
		{
			RandomAccessFile f = new RandomAccessFile(filePath, "rw");
			f.setLength(size);
			f.close();
		} 
		catch (Exception e) 
		{
			ret = false;
			e.printStackTrace();
		}
		return ret;
	}

	public static void dataToFile(String filename, byte data[])throws IOException 
	{
		File file = new File(filename);
		OutputStream out = new FileOutputStream(file);
		out.write(data);
		out.close();
	}

	public static byte[] getByte(String fileName) throws IOException 
	{
		InputStream in = new FileInputStream(fileName);
		byte[] data = new byte[in.available()];
		in.read(data);
		return data;
	}

}
