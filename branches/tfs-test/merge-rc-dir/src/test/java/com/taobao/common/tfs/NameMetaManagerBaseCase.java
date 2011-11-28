package com.taobao.common.tfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.List;
import java.util.ArrayList;
import java.util.Stack;
import java.util.Random;
import java.util.zip.CRC32;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class NameMetaManagerBaseCase {

    protected static Logger log = Logger.getLogger(NameMetaManagerBaseCase.class.getName());
    final static ClassPathXmlApplicationContext clientFactory = new ClassPathXmlApplicationContext("tfsClient.xml");
    public static DefaultTfsManager tfsManager = (DefaultTfsManager) clientFactory.getBean("tfsManager");
    public static String resourcesPath="src/test/resources";

    public static long appId = tfsManager.getAppId();
    public static long userId = 361;

    @BeforeClass
    public  static void setUpOnce() throws Exception {
    }

    @AfterClass
    public static void tearDownOnce() throws Exception {
    }

    public static String getParentDir(String filePath) {
        if (null == filePath) {
            log.error("file name null!");
            return null;
        }
        String strSlash = "/";
        if (filePath.equals(strSlash)) {
            return filePath;
        }
        String tmpPath = filePath;
        if (filePath.endsWith(strSlash)) {
            tmpPath = filePath.substring(0, filePath.length() - 1);
            log.debug("filePath: " + filePath + ", ends with '/', remove the last '/'!, name now: " + tmpPath);
        }
        int lastPos = filePath.lastIndexOf(strSlash);
        if (-1 == lastPos) {
            log.error("invalid file name: " + filePath); 
            return null;
        }
        String parentDir = null;
        // parent is "/"
        if (0 == lastPos) {
            parentDir = strSlash;
        }
        else {
            parentDir = tmpPath.substring(0, lastPos);
        }
        log.debug("parent dir of: \"" + filePath + "\" is: " + parentDir);
        return parentDir;
    }

    public static String getBaseName(String filePath) {
        if (null == filePath) {
            log.error("file name null!");
            return null;
        }
        String strSlash = "/";
        if (filePath.equals(strSlash)) {
            return filePath;
        }
        String tmpPath = filePath;
        if (filePath.endsWith(strSlash)) {
            tmpPath = filePath.substring(0, filePath.length() - 1);
            log.debug("filePath: " + filePath + ", ends with '/', remove the last '/'!, name now: " + tmpPath);
        }
        int lastPos = filePath.lastIndexOf(strSlash);
        if (-1 == lastPos) {
            log.error("invalid file name: " + filePath); 
            return null;
        }
        String baseName = null;
        baseName = tmpPath.substring(lastPos + 1, tmpPath.length());
        log.debug("base name of: \"" + filePath + "\" is: " + baseName);
        return baseName;
    }

    public static ArrayList<String> split(String filePath) {
        if (null == filePath) {
            log.error("file name null!");
            return null;
        }
        ArrayList<String> result = new ArrayList<String>();
        String strSlash = "/";
        // in case of "/", will return "/" and ""
        if (filePath.equals(strSlash)) {
            result.add(strSlash);
            result.add(""); 
            return result;
        }
        String tmpPath = filePath;
        if (filePath.endsWith(strSlash)) {
            tmpPath = filePath.substring(0, filePath.length() - 1);
            log.debug("filePath: " + filePath + ", ends with '/', remove the last '/'!, name now: " + tmpPath);
        }
        int lastPos = filePath.lastIndexOf(strSlash);
        if (-1 == lastPos) {
            log.error("invalid file name: " + filePath); 
            return null;
        }
        String parentDir = null;
        // parent is "/"
        if (0 == lastPos) {
            parentDir = strSlash;
        }
        else {
            parentDir = tmpPath.substring(0, lastPos);
        }
        String baseName = null;
        baseName = tmpPath.substring(lastPos + 1, tmpPath.length());
        log.debug("parent dir of: \"" + filePath + "\" is: " + parentDir);
        log.debug("base name of: \"" + filePath + "\" is: " + baseName);
        result.add(parentDir);
        result.add(baseName);
        return result;
    }

    public static String join(String parentDir, String baseName) {
        if (null == parentDir || null == baseName) {
            log.error("parentDir or baseName null!");
            return null;
        }
        String strSlash = "/";
        if (parentDir.endsWith(strSlash)) {
            return parentDir + baseName;
        }
        else {
            return parentDir + "/" + baseName;
        }
    }

    //TODO: no deal with dir acutally is a file
    public static boolean rmDirRecursive(long appId, long userId, String dir) {
        boolean bRet = false;
        List<FileMetaInfo>  fileInfoList;
        FileMetaInfo  fileMetaInfo;
        // make sure this dir exist, check its parent
        boolean dirExist = false;
        ArrayList<String> dirComponent = split(dir);
        if (null == dirComponent || 2 != dirComponent.size()) {
            log.error("split dir: \"" + dir + "\" failed!");
            return false;
        }
        String parentDir = dirComponent.get(0);
        String baseName = dirComponent.get(1);
        if (null == parentDir || null == baseName) {
            log.error("parent dir or base name of dir: " + dir + "\" null!");
            return false;
        }
        fileInfoList = tfsManager.lsDir(appId, userId, parentDir);
        for (int i = 0; i < fileInfoList.size(); i++) {
            fileMetaInfo = fileInfoList.get(i);
            if (!fileMetaInfo.isFile() && fileMetaInfo.getFileName().equals(baseName)) {
                dirExist = true;
                break;
            }
        }
        if (false == dirExist) {
            log.warn("dir: \"" + dir + "\" not exists!");
            return true;
        }
        
        // do the things
        Stack<NameMetaDirHelper> dirStack = new Stack<NameMetaDirHelper>();
        NameMetaDirHelper dirHelper = new NameMetaDirHelper(dir);
        dirStack.push(dirHelper);
        while (!dirStack.empty()) {
            NameMetaDirHelper top = dirStack.peek();
            if (top.canBeDeleted()) {
                tfsManager.rmDir(appId, userId, top.getDirName()); 
                dirStack.pop();
            }
            else {
                fileInfoList = tfsManager.lsDir(appId, userId, top.getDirName());
                log.debug("fileInfoList size: " + fileInfoList.size());
                for (int i = 0; i < fileInfoList.size(); i++) {
                    fileMetaInfo = fileInfoList.get(i);
                    if (fileMetaInfo.isFile()) {
                        bRet = tfsManager.rmFile(appId, userId, top.getDirName() + "/" + fileMetaInfo.getFileName());
                        if (false == bRet) {
                            return bRet;
                        }
                    }
                    else {
                        dirHelper = new NameMetaDirHelper(top.getDirName() + "/" + fileMetaInfo.getFileName());
                        dirStack.push(dirHelper);                 
                    }
                }
                top.setCanBeDeleted(true);
            }
        }
        return true;
    }

    protected int getCrc(OutputStream opstream) {
        try {
            String str = opstream.toString();        
            byte [] data = str.getBytes();
            CRC32 crc = new CRC32();
            crc.reset();
            crc.update(data);
            System.out.println(crc.getValue());

            return (int)crc.getValue();
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    protected int getCrc(String fileName) {
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
            System.out.println(" file name crc " + fileName + " c " + crc.getValue());

            return (int)crc.getValue();
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    protected byte[] getByte(String fileName) throws IOException {
        InputStream in = new FileInputStream(fileName);
        byte[] data= new byte[in.available()];
        in.read(data);
        return data;
    }

    static protected void createFile(String name, long length) throws FileNotFoundException, IOException {
        Random rd = new Random();
        File file = new File(name);
        FileOutputStream output = new FileOutputStream(file);
        int len = 1024 * 1024 * 8;
        byte[] data = new byte[len];
        long totalLength=length;
        while (totalLength > 0) {
            len = (int)Math.min(len, totalLength);
            rd.nextBytes(data);
            output.write(data, 0, len);
            totalLength -= len;
        }
        System.out.println("Succeed create file"+name);
    }
    
    public void create_different_files(String rootDir, String localFile) throws Exception {    	 
        boolean ret = false;

        String dirpath = rootDir + "/" + String.valueOf(Thread.currentThread().getId());
        String filepath = dirpath + "/file";

        ret = tfsManager.createDir(appId, userId, dirpath);
        Assert.assertTrue(ret);
             
        ret = tfsManager.saveFile(appId, userId, localFile, filepath);
        Assert.assertTrue(ret);
             
        ret = tfsManager.rmFile(appId, userId, filepath);
        Assert.assertTrue(ret);

        ret = tfsManager.rmDir(appId, userId, dirpath);
        Assert.assertTrue(ret);
    }

    public void create_one_file(String rootDir, String localFile) throws Exception {
        String dirpath = rootDir + "/public";
        String filepath = dirpath + "/file";

        tfsManager.createDir(appId, userId, dirpath);

        tfsManager.saveFile(appId, userId, localFile, filepath);

        tfsManager.rmFile(appId, userId, filepath);

        tfsManager.rmDir(appId, userId, dirpath);
    }

    public void rename_different_files(String rootDir, String localFile) throws Exception {    	 
        boolean ret = false;
      
        String dirpath = rootDir + "/" + String.valueOf(Thread.currentThread().getId());
        String filepath = dirpath + "/file";
        String newfilepath = dirpath + "/renamed";

        ret = tfsManager.createDir(appId, userId, dirpath);
        Assert.assertTrue(ret);

        ret = tfsManager.createFile(appId, userId, filepath);
        Assert.assertTrue(ret);

        ret = tfsManager.mvFile(appId, userId, filepath, newfilepath);
        Assert.assertTrue(ret);

        ret = tfsManager.rmFile(appId, userId, newfilepath);
        Assert.assertTrue(ret);

        ret = tfsManager.rmDir(appId, userId, dirpath);
        Assert.assertTrue(ret);
  }

    public void rename_one_file(String rootDir, String localFile) throws Exception {
	      String dirpath = rootDir + "/public";
        String filepath = dirpath + "/file";
        String newfilepath = dirpath + "/renamed_file";

        tfsManager.createDir(appId, userId, dirpath);

        tfsManager.saveFile(appId, userId, localFile, filepath);

        tfsManager.mvFile(appId, userId, filepath, newfilepath);

        tfsManager.rmFile(appId, userId, newfilepath);

        tfsManager.rmDir(appId, userId, dirpath);
    }

    public void move_different_files(String rootDir, String localFile) {
        boolean ret = false;

        String dirpath = rootDir + "/" + String.valueOf(Thread.currentThread().getId());
        String filepath = dirpath + "/" + String.valueOf(Thread.currentThread().getId());
        String newfilepath = rootDir + "/moved_" + String.valueOf(Thread.currentThread().getId());

        ret = tfsManager.createDir(appId, userId, dirpath);
        Assert.assertTrue(ret);

        ret = tfsManager.createFile(appId, userId, filepath);
        Assert.assertTrue(ret);

        ret = tfsManager.mvFile(appId, userId, filepath, newfilepath);
        Assert.assertTrue(ret);

        ret = tfsManager.rmFile(appId, userId, newfilepath);
        Assert.assertTrue(ret);

        ret = tfsManager.rmDir(appId, userId, dirpath);
        Assert.assertTrue(ret);
    }

    public void move_one_file(String rootDir, String localFile) throws Exception {

        String dirpath = rootDir + "/public";
        String filepath = dirpath + "/file";
        String newfilepath = rootDir + "/moved_file";

        tfsManager.createDir(appId, userId, dirpath);

        tfsManager.saveFile(appId, userId, localFile, filepath);

        tfsManager.mvFile(appId, userId, filepath, newfilepath);

        tfsManager.rmFile(appId, userId, newfilepath);

        tfsManager.rmDir(appId, userId, dirpath);
    }
    /*  static // create resources files
    {
        try {
            createFile(resourcesPath+"100K",100*(1<<10));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        try {
            createFile(resourcesPath+"1G",1<<30);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        try {
            createFile(resourcesPath+"2M",2*(1<<20));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
         try {
            createFile(resourcesPath+"3M",3*(1<<20));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
          try {
            createFile(resourcesPath+"10K",10*(1<<10));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }*/

}
