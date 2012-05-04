package com.taobao.common.tfs.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Random;
import java.util.zip.CRC32;

import org.apache.log4j.Logger;

public class FileUtility {
	private static Logger logger = Logger.getLogger(FileUtility.class);

	public static long getFileSize(String path) throws FileNotFoundException {
		File f = new File(path);

		if (!f.exists()) {
			throw new FileNotFoundException();
		}

		return f.length();
	}

	public static boolean deleteFile(String path) {
		return false;
	}

	public static boolean compareTwoFiles(String src, String dest) {
		// TODO
		return false;
	}

	public static boolean generateRadomFile(String fileName, long size) {
		byte[] buffer;
		FileOutputStream fos = null;
		int defaultBufferSize = 8 * 1024 * 1024;

		File f = new File(fileName);

		try {
			if (!f.exists()) {
				f.createNewFile();
			}

			fos = new FileOutputStream(f);
			Random random = new Random();

			while (size > 0) {
				if (size >= defaultBufferSize) {
					buffer = new byte[defaultBufferSize];
				} else {
					buffer = new byte[(int) size];
				}

				random.nextBytes(buffer);
				fos.write(buffer);
				fos.flush();

				size -= defaultBufferSize;
			}

			fos.close();
		} catch (IOException e) {
			logger.error("generate random file error: " + e.getStackTrace());
			return false;
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					logger.error("close fos fail " + e.getStackTrace());
					return false;
				}
			}
		}

		return true;
	}

	public static int getCrc(OutputStream opstream) {
		try {
			String str = opstream.toString();
			byte[] data = str.getBytes();
			CRC32 crc = new CRC32();
			crc.reset();
			crc.update(data);
			System.out.println(crc.getValue());
			return (int) crc.getValue();
		} catch (Exception e) {
			logger.error("Get crc error: " + e.getStackTrace());
			return -1;
		}
	}

	public static int getCrc(String fileName) {
		FileInputStream input = null;
		try {
			input = new FileInputStream(fileName);
			int readLength;
			byte[] data = new byte[102400];
			CRC32 crc = new CRC32();
			crc.reset();
			while ((readLength = input.read(data, 0, 102400)) > 0) {
				crc.update(data, 0, readLength);
			}
			input.close();
			return (int) crc.getValue();
		} catch (IOException e) {
			logger.error("Get crc error: " + e.getStackTrace());
			return -1;
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error("Close input stream error: "
							+ e.getStackTrace());
				}
			}
		}
	}

	public static boolean createFile(String filePath, long size) {
		boolean ret = true;
		try {
			RandomAccessFile f = new RandomAccessFile(filePath, "rw");
			f.setLength(size);
			f.close();
		} catch (Exception e) {
			ret = false;
			e.printStackTrace();
		}
		return ret;
	}

	public static void dataToFile(String filename, byte data[])
			throws IOException {
		File file = new File(filename);
		OutputStream out = new FileOutputStream(file);
		out.write(data);
		out.close();
	}

	public static byte[] getByte(String fileName) throws IOException {
		InputStream in = new FileInputStream(fileName);
		byte[] data = new byte[in.available()];
		in.read(data);
		return data;
	}

}
