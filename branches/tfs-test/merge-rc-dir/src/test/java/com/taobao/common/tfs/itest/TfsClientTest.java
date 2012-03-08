package com.taobao.common.tfs.itest;

import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Ignore;

import java.lang.Math;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.zip.CRC32;
import java.io.File;
import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;
import com.taobao.common.tfs.packet.FileInfo;

public class TfsClientTest
{
    class DataCase {
        private String fileName;
        private String tfsName;
        private long offset;
        private long length;
        private int crc;

        public void setFileName(String fileName) {
            this.fileName = fileName;
            /////
        }
        public String getFileName() {
            return fileName;
        }
        public void setTfsName(String tfsName) {
            this.tfsName = tfsName;
        }
        public String getTfsName() {
            return tfsName;
        }
        public void setOffset(long offset) {
            this.offset = offset;
        }
        public long getOffset() {
            return offset;
        }
        public void setLength(long length) {
            this.length = length;
        }
        public long getLength() {
            return length;
        }
        public void setCrc(int crc) {
            this.crc = crc;
        }
        public int getCrc() {
            return crc;
        }
    }

    private static final Log log = LogFactory.getLog(TfsClientTest.class);

    private static DefaultTfsManager tfsManager = null;
    private static Random rd = null;
    private static List<DataCase> files = new ArrayList<DataCase>();
    private static DataCase smallFile = null;
    private static DataCase smallFileRet = null;
    private static DataCase largeFile = null;
    private static DataCase largeFileRet = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
         
        ClassPathXmlApplicationContext appContext =
            new ClassPathXmlApplicationContext(new String[] { "tfs.xml" });
        tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
        rd = new Random();
        smallFile = createFile(1024*1024 + rd.nextInt() % (1024*1024));
        files.add(smallFile);
        smallFileRet = new TfsClientTest().new DataCase();
        smallFileRet.setFileName(smallFile.fileName + "ret");
        files.add(smallFileRet);
        largeFile = createFile((long)1024*1024*10+ Math.abs(rd.nextLong()) % (long)(1024*1024));
        files.add(largeFile);
        largeFileRet = new TfsClientTest().new DataCase();
        largeFileRet.setFileName(largeFile + "ret");
        files.add(largeFileRet);
        
        appId = tfsManager.getAppId();
        
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        for (DataCase dataCase : files) {
            if (dataCase.getFileName() != null) {
                new File(dataCase.getFileName()).delete();
            }
        }
        tfsManager.destroy();
    }

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

    private static DataCase createFile(long length) {
        DataCase dataCase = new TfsClientTest().new DataCase();
        try {
            File file = File.createTempFile("tfsclienttest::", String.valueOf(length), new File("."));
            FileOutputStream output = new FileOutputStream(file);
            int len = 1024*1024*8;
            byte[] data = new byte[len];
            CRC32 crc = new CRC32();
            long totalLength = length;
            log.info("create file: " + length);

            while (totalLength > 0) {
                len = (int)Math.min(len, totalLength);
                rd.nextBytes(data);
                output.write(data, 0, len);
                crc.update(data, 0, len);
                totalLength -= len;
            }
            output.close();
            dataCase.setFileName(file.getAbsolutePath());
            dataCase.setLength(length);
            dataCase.setCrc((int)crc.getValue());
            log.info(" small file crc " + crc.getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dataCase;
    }

    private int getCrc(String fileName) {
        try {
            FileInputStream input = new FileInputStream(fileName);
            int readLength;
            byte[] data = new byte[102400];
            CRC32 crc = new CRC32();
            crc.reset();
            while ((readLength = input.read(data, 0, 102400)) > 0) {
                crc.update(data, 0, readLength);
            }
            input.close();
            log.info(" file name crc " + fileName + " c " + crc.getValue());

            return (int)crc.getValue();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    ///////////////////////////////
    // Rc interface test
    ///////////////////////////////
    //@Test
    public void TestSmallFile() {
        log.info("== test small file");
        for (int i = 0; i < 10; i++) {
            String name = tfsManager.saveFile(smallFile.getFileName(), null, ".uc");
            assertNotNull(name);
            log.info("small ret name " + name);
            assertTrue(tfsManager.fetchFile(name, ".uc", smallFileRet.getFileName()));
            FileInfo fileInfo = tfsManager.statFile(name, null);
            assertNotNull(fileInfo);
            assertEquals(fileInfo.getLength(), smallFile.getLength());
            assertEquals(smallFile.getCrc(), getCrc(smallFileRet.getFileName()));
            // hide
            assertTrue(tfsManager.hideFile(name, ".uc", 1));
            assertFalse(tfsManager.fetchFile(name, ".uc", smallFileRet.getFileName()));
            try {
                assertTrue(tfsManager.fetchFile(name, ".uc", 10, (new FileOutputStream(smallFileRet.getFileName()))));
            } catch (FileNotFoundException e) {
            }
            assertTrue(tfsManager.hideFile(name, ".uc", 0));
            // unlink
            assertTrue(tfsManager.unlinkFile(name, null));
            assertTrue(tfsManager.unlinkFile(name, null, 2));
            assertFalse(tfsManager.unlinkFile(name, null, 2));
            fileInfo  = tfsManager.statFile(name, null);
            assertNotNull(fileInfo);
            assertEquals(fileInfo.getFlag(), 0);
        }
    }

    //@Test
    public void TestLargeFileStream() {
        log.info("== test large file stream == ");
        // test 0 ~ 10M ~ 50M ~ 100M ~ 500M ~ 1G ~ 1.5G ~ 4G ~ 10G ~ 13G
        int[] threshHold = {1, 10, 50, 100, 500, 1000, 1500, 4000, 10000, 13000};
        // for (int i = 0; i < threshHold.length - 1; i++) {
        for (int i = 0; i < 4; i++) {
            CRC32 writeCrc = new CRC32();
            writeCrc.reset();
            Random rd = new Random();
            long totalLength = ((long)(threshHold[i] + Math.abs(rd.nextInt()) % (threshHold[i+1] - threshHold[i]))) * (1 << 20);
            // each len 5.5M ~ 22.5M
            int len  = (11 * (1 << 20))/2 + rd.nextInt() % 17;
            log.info("===========test write total length: " + totalLength + " per len: " + len + "=============");
            byte[] data = new byte[len];
            long writeLength = 0;
            // open file to write, get fd
            int fd = tfsManager.openWriteFile(null, ".jpg", largeFile.getFileName());
            int perLength = 0;
            while (writeLength < totalLength) {
                perLength = (int)Math.min(len, totalLength - writeLength);
                rd.nextBytes(data);
                // write data
                assertEquals(tfsManager.writeFile(fd, data, 0, perLength), perLength);
                writeCrc.update(data, 0, perLength);
                writeLength += perLength;
            }
            // close tfs file, get tfs file name
            String retTfsName = tfsManager.closeFile(fd);
            assertNotNull(retTfsName);
            log.info("==== save tfs file: " + retTfsName + " crc: " + writeCrc.getValue());

            /////////////////////////////////////////
            // read large file in stream way
            /////////////////////////////////////////
            CRC32 readCrc = new CRC32();
            readCrc.reset();
            // open tfs file to read
            fd = tfsManager.openReadFile(retTfsName, null);
            int readLength = 0;
            // random read len every time
            len  = (11 * (1 << 20))/2 + rd.nextInt() % 17;
            data = new byte[len];
            while (true) {
                // read data
                readLength = tfsManager.readFile(fd, data, 0, len);
                assertTrue(readLength >= 0);
                readCrc.update(data, 0, readLength);
                if (readLength < len) {
                    log.info("read data file end");
                    break;
                }
            }

            log.info("== read tfs file " + retTfsName + " crc: " + readCrc.getValue());
            assertEquals(readCrc.getValue(), writeCrc.getValue());

            // pread, should check crc
            long tmplen = tfsManager.readFile(fd, data, 0, len);
            log.info("read over again " + tmplen);
            assertTrue(tmplen <= 0);
            tmplen = tfsManager.readFile(fd, totalLength/2, data, 0, len);
            log.info("pread over again readed " + tmplen + " require len " + len + " offset " + totalLength/2 + " totallen: " + totalLength);
            assertEquals(tmplen, Math.min(totalLength/2, len));
            tmplen = tfsManager.readFile(fd, totalLength/3, data, 0, len);
            log.info("pread over again 2 readed " + tmplen + " require len " + len + " offset" + totalLength/3  + " totallen: " + totalLength);
            assertEquals(tmplen, Math.min(totalLength - totalLength/3, len));

            // close tfs file
            tfsManager.closeFile(fd);
        }
    }

    //@Test
    public void TestLargeFileLocal() {
        String retTfsName = tfsManager.saveLargeFile(largeFile.getFileName(), null, ".avi");
        assertNotNull(retTfsName);
        assertEquals(retTfsName.charAt(0), 'L');
        log.info("== save local file to tfs ok: length: " + largeFile.getLength() +
                 " "  + largeFile.getFileName() + " => " + retTfsName);

        assertTrue(tfsManager.fetchFile(retTfsName, ".avi", largeFile.getFileName()));

        /////////////////////
        // stat tfs file
        /////////////////////
        FileInfo fileInfo = tfsManager.statFile(retTfsName, null);
        assertNotNull(fileInfo);
        log.info("stat tfs file " + retTfsName + " ok. " + fileInfo);

        ///////////////////////////////
        // fetch tfs file to local in stream way
        ///////////////////////////////
        FileOutputStream output = null;
        try {
            output = new FileOutputStream(largeFile.getFileName());
            assertTrue(tfsManager.fetchFile(retTfsName, null, output));
            output.close();
        } catch (Exception e) {
            log.info("local exception ");
            return;
        }
        // hide
        assertTrue(tfsManager.hideFile(retTfsName, null, 1));
        fileInfo = tfsManager.statFile(retTfsName, null);
        assertNotNull(fileInfo);
        assertEquals(fileInfo.getFlag(), 4);
        assertTrue(tfsManager.hideFile(retTfsName, null, 0));
        fileInfo = tfsManager.statFile(retTfsName, null);
        assertNotNull(fileInfo);
        assertEquals(fileInfo.getFlag(), 0);
        log.info("after hide stat tfs file " + retTfsName + " ok. " + fileInfo);

        /////////////////////
        // unlink tfs file
        /////////////////////
        assertTrue(tfsManager.unlinkFile(retTfsName, null));
        fileInfo = tfsManager.statFile(retTfsName, null);
        assertNotNull(fileInfo);
        assertEquals(fileInfo.getFlag(), 1);
        log.info("after unlink stat tfs file " + retTfsName + " ok. " + fileInfo);
        assertFalse(tfsManager.unlinkFile(retTfsName, null));
    }

    ///////////////////////////////
    // NameMeta test
    ///////////////////////////////
    private long userId = 6789;
    private static long appId;
    private String dirName = "/nnytest_dir";
    private String newDirName = "/nnytest_new_dir";
    private String fileName = dirName + "/nnytest_file";
    private String wrongFileName = "nnytest_file";
    private String newFileName = newDirName + "/nnytest_new_file";

    //@Test
    public void TestDir() {
        tfsManager.rmFile(appId, userId, newFileName);
        tfsManager.rmDir(appId, userId, newDirName);
        tfsManager.rmFile(appId, userId, fileName);
        tfsManager.rmDir(appId, userId, dirName);
        assertTrue(tfsManager.createDir(appId, userId, dirName));
        assertFalse(tfsManager.createDir(appId, userId, dirName));
        assertTrue(tfsManager.mvDir(appId, userId, dirName, newDirName));
        assertTrue(tfsManager.rmDir(appId, userId, newDirName));
    }

    //@Test
    public void TestFile() {
        assertFalse(tfsManager.createDir(appId, userId, "/ "));
        tfsManager.createDir(appId, userId, "/text1");
        tfsManager.createDir(appId, userId, "/text1/text2");
        tfsManager.createDir(appId, userId, "/text1/text2/text3");
        assertFalse(tfsManager.mvDir(appId, userId, "/text1", "/text1/text2/text3/text4"));
        tfsManager.rmDir(appId, userId, "/text1/text2/text3");
        tfsManager.rmDir(appId, userId, "/text1/text2");
        tfsManager.rmDir(appId, userId, "/text1");

        tfsManager.rmFile(appId, userId, fileName);
        tfsManager.rmFile(appId, userId, newFileName);
        tfsManager.rmDir(appId, userId, dirName);
        tfsManager.rmDir(appId, userId, newDirName);

        assertTrue(tfsManager.createDir(appId, userId, dirName));
        assertFalse(tfsManager.createDir(appId, userId, dirName));

        assertTrue(tfsManager.mvDir(appId, userId, dirName, newDirName));
        assertFalse(tfsManager.mvDir(appId, userId, dirName, newDirName));
        assertTrue(tfsManager.rmDir(appId, userId, newDirName));

        assertFalse(tfsManager.createFile(appId, userId, fileName));
        assertTrue(tfsManager.createDir(appId, userId, dirName));
        assertTrue(tfsManager.createFile(appId, userId, fileName));
        assertFalse(tfsManager.createFile(appId, userId, fileName));
        assertFalse(tfsManager.mvFile(appId, userId, fileName, newFileName));
        assertTrue(tfsManager.createDir(appId, userId, newDirName));

        assertTrue(tfsManager.mvFile(appId, userId, fileName, newFileName));
        assertFalse(tfsManager.mvFile(appId, userId, fileName, newFileName));

        assertTrue(tfsManager.rmFile(appId, userId, newFileName));
        assertFalse(tfsManager.rmFile(appId, userId, newFileName));
        assertTrue(tfsManager.rmDir(appId, userId, dirName));
        assertTrue(tfsManager.rmDir(appId, userId, newDirName));
        assertFalse(tfsManager.rmDir(appId, userId, newDirName));
    }

    //@Test
    public void TestAction() {
        tfsManager.rmFile(appId, userId, fileName);
        tfsManager.rmDir(appId, userId, dirName);
        assertTrue(tfsManager.createDir(appId, userId, dirName));
        assertTrue(tfsManager.createFile(appId, userId, fileName));
        assertTrue(tfsManager.rmFile(appId, userId, fileName));
        assertTrue(tfsManager.rmDir(appId, userId, dirName));
    }

    //@Test
    public void TestWrite() {
        //for (int i = 0; i < 10; i++) {
            tfsManager.rmFile(appId, userId, fileName);
            tfsManager.rmFile(appId, userId, newFileName);
            tfsManager.rmDir(appId, userId, dirName);
            assertTrue(tfsManager.createDir(appId, userId, dirName));
            assertTrue(tfsManager.createFile(appId, userId, fileName));
            // long length = NameMetaManagerLite.MAX_BATCH_WRITE_LENGTH*2 + NameMetaManagerLite.MAX_BATCH_WRITE_LENGTH/2;
            // long length = NameMetaManagerLite.MAX_BATCH_WRITE_LENGTH/2;
            // byte[] data = new byte[(int)length];
            String s = "test for test";
            long length = s.length();
            byte[] data = s.getBytes();
            String ss = "t";
            byte[] data2 = ss.getBytes();
            assertEquals(tfsManager.write(appId, userId, fileName, 0, data, 0, length), length);
            assertEquals(tfsManager.write(appId, userId, fileName, length + 1, data, 0, length), length);
            assertEquals(tfsManager.write(appId, userId, fileName, length, data, 0, 1), 1);
            try {
                FileOutputStream fos = new FileOutputStream(smallFileRet.getFileName());
                assertEquals(tfsManager.read(appId, userId, fileName, 0, length, fos), length);
                assertEquals(tfsManager.read(appId, userId, fileName, 1, length - 1, fos), length - 1);
                assertEquals(tfsManager.read(appId, userId, fileName, 0, 2* length, fos), 2 * length);
                assertEquals(tfsManager.read(appId, userId, fileName, 1, 2* length, fos), 2 * length);
                assertEquals(tfsManager.read(appId, userId, fileName, length + 2, length, fos), length - 1);
                assertEquals(tfsManager.read(appId, userId, wrongFileName, 1, 2 * length, fos), -1);
                assertEquals(tfsManager.read(appId, userId, newFileName, 1, 2 * length, fos), -1);
                //assertEquals(tfsManager.read(appId, userId, fileName, length, length, fos), length);
            //assertEquals(tfsManager.read(appId, userId, fileName, length*2, length, fos), 0);
            log.info("== file content" + fileName + "  "  + fos);
            Thread.sleep(5000);
        } catch (Exception e) {
        }
        //}
            assertTrue(tfsManager.rmFile(appId, userId, fileName));
            assertTrue(tfsManager.rmDir(appId, userId, dirName));
    }

    //@Test
      public void test_20_write_large_many_times_parts()  throws IOException {
        log.info("test_20_write_large_many_times_parts");
        tfsManager.createFile(appId, userId, "/text");
        byte data[]= new byte[3*1024*1024];
        long len = 3*(1<<20);
        long offset=0;
        long dataOffset=0;
        long  Ret;
        Ret=tfsManager.write(appId, userId, "/text", offset,data, dataOffset, len);
        assertEquals(Ret,3*(1<<20));

        len = 3*(1<<20);
        offset=3*(1<<20);
        dataOffset=0;
        Ret=tfsManager.write(appId, userId, "/text", offset,data, dataOffset, len);
        assertEquals(Ret,3*(1<<20));

        len = 3*(1<<20);
        offset=6*(1<<20)+1;
        dataOffset=0;
        Ret=tfsManager.write(appId, userId, "/text", offset,data, dataOffset, len);
        assertEquals(Ret,3*(1<<20));

        data=new byte[1];
        assertNotNull(data);
        len = 1;
        //offset=6*(1<<20);
        offset=6*(1<<20);
        dataOffset=0;
        Ret=tfsManager.write(appId, userId, "/text", offset,data, dataOffset, len);
        log.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"+Ret);
        assertEquals(Ret,1);

        data=new byte[1];
        len = 1;
        offset=6*(1<<20);
        dataOffset=0;
        Ret=tfsManager.write(appId, userId, "/text", offset,data, dataOffset, len);
        assertTrue(Ret==0);
        tfsManager.fetchFile(appId, userId, "temp", "/text");
        tfsManager.rmFile(appId, userId, "/text");
        new File("temp").delete();
      }

    //@Test
    public void TestWriteFile() {
        String name = largeFile.getFileName();
        String retName = largeFileRet.getFileName();
        tfsManager.rmFile(appId, userId, fileName);
        tfsManager.rmDir(appId, userId, dirName);
        assertFalse(tfsManager.saveFile(appId, userId, name, fileName));
        assertTrue(tfsManager.createDir(appId, userId, dirName));
        assertTrue(tfsManager.saveFile(appId, userId, name, fileName));
        assertTrue(tfsManager.fetchFile(appId, userId, retName, fileName));
        assertFalse(tfsManager.fetchFile(appId, userId, retName, wrongFileName));
        assertFalse(tfsManager.fetchFile(appId, userId, retName, newFileName));
        assertTrue(tfsManager.rmFile(appId, userId, fileName));
        assertTrue(tfsManager.rmDir(appId, userId, dirName));
    }

    //@Test
    public void Testls() {
        for (int i = 0; i < 4; i++) {
            tfsManager.rmDir(appId, userId, dirName + "/" + i + "/" + i + "dir");
            tfsManager.rmDir(appId, userId, dirName + "/" + i + "/" + i + "dir1");
            tfsManager.rmFile(appId, userId, dirName + "/" + i + "/" + i*5);
            tfsManager.rmDir(appId, userId, dirName + "/" + i);
            tfsManager.rmFile(appId, userId, fileName + i);
        }
        tfsManager.rmFile(appId, userId, fileName);
        tfsManager.rmDir(appId, userId, dirName);
        assertTrue(tfsManager.createDir(appId, userId, dirName));
        assertTrue(tfsManager.createFile(appId, userId, fileName));
        //TestWriteFile();
        for (int i = 0; i < 7; i++) {
            assertTrue(tfsManager.createFile(appId, userId, fileName + i));
            assertTrue(tfsManager.createDir(appId, userId, dirName + "/" + i));
            assertTrue(tfsManager.createFile(appId, userId, dirName + "/" + i + "/" + i*5));
            assertTrue(tfsManager.createDir(appId, userId, dirName + "/" + i + "/" + i + "dir"));
            assertTrue(tfsManager.createDir(appId, userId, dirName + "/" + i + "/" + i + "dir1"));
        }
        FileMetaInfo fileMetaInfo1 = tfsManager.lsFile(appId, userId, fileName);
        assertTrue(fileMetaInfo1 != null);
        log.info("== file " + fileName + fileMetaInfo1);
        for (int i = 0; i < 7; i++) {
            FileMetaInfo fileMetaInfo = tfsManager.lsFile(appId, userId, fileName+i);
            assertTrue(fileMetaInfo != null);
            log.info("== file " + fileName + i + " " + fileMetaInfo);
            fileMetaInfo = tfsManager.lsFile(appId, userId, dirName + "/" + i + "/" + i*5);
            assertTrue(fileMetaInfo != null);
            log.info("== file " + dirName + "/" + i + "/" + i*5 + " " + fileMetaInfo);
        }

        List<FileMetaInfo> metaInfoList = tfsManager.lsDir(appId, userId, dirName);
        assertTrue(metaInfoList != null);
        for (FileMetaInfo metaInfo : metaInfoList)  {
            log.info("[ " + metaInfo + " ]");
        }
        log.info("== ls dir count: " + metaInfoList.size());

        metaInfoList = tfsManager.lsDir(appId, userId, dirName, true);
        assertTrue(metaInfoList != null);
        for (FileMetaInfo metaInfo : metaInfoList)  {
            log.info("[ " + metaInfo + " ]");
        }
        log.info("== recursive ls dir count: " + metaInfoList.size());

        metaInfoList = tfsManager.lsDir(appId, userId, "/");
        assertTrue(metaInfoList != null);
        for (FileMetaInfo metaInfo : metaInfoList)  {
            log.info("[ " + metaInfo + " ]");
        }
        log.info("== ls root count: " + metaInfoList.size());

        metaInfoList = tfsManager.lsDir(appId, userId, "/", true);
        assertTrue(metaInfoList != null);
        for (FileMetaInfo metaInfo : metaInfoList)  {
            log.info("[ " + metaInfo + " ]");
        }
        log.info("== recursive ls root count: " + metaInfoList.size());

        for (int i = 0; i < 7; i++) {
            assertTrue(tfsManager.rmDir(appId, userId, dirName + "/" + i + "/" + i + "dir"));
            assertTrue(tfsManager.rmDir(appId, userId, dirName + "/" + i + "/" + i + "dir1"));
            assertTrue(tfsManager.rmFile(appId, userId, dirName + "/" + i + "/" + i*5));
            assertTrue(tfsManager.rmDir(appId, userId, dirName + "/" + i));
            assertTrue(tfsManager.rmFile(appId, userId, fileName + i));
        }
        assertTrue(tfsManager.rmFile(appId, userId, fileName));
        assertTrue(tfsManager.rmDir(appId, userId, dirName));
    }

    protected byte[]  getByte(String fileName) throws IOException {
      InputStream in = new FileInputStream(fileName);
      byte[] data= new byte[in.available()];
      in.read(data);
      return data;
    }
}

