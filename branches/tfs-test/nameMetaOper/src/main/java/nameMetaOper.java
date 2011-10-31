package com.taobao.common.tfs.function;

import java.util.concurrent.ExecutorService; 
import java.util.concurrent.Executors; 
import java.util.concurrent.TimeUnit; 
import java.util.concurrent.locks.ReadWriteLock; 
import java.util.concurrent.locks.ReentrantReadWriteLock; 

import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32;

import java.lang.Exception;

import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class nameMetaOper{

  // operation type
  final static int OPER_CREATE_DIR = 1;
  final static int OPER_LS_DIR = 2;
  final static int OPER_SAVE_SMALL_FILE = 4;
  final static int OPER_SAVE_LARGE_FILE = 8;
  final static int OPER_FETCH_FILE = 16;
  final static int OPER_LS_FILE = 32;
  final static int OPER_REMOVE_FILE = 64;
  final static int OPER_MIX = 128;

  public static void main(String[] args) { 

    if (args.length < 1) {
      System.out.println("usage: nameMetaClient operType userId");
      System.exit(1);
    }
    int operType = Integer.parseInt(args[0]);
    long userId = 0;
    if (args.length == 2) {
      userId = Long.parseLong(args[1]);
    }

    // input & output
    String inputFile;
    ArrayList<String> inputList = new ArrayList<String>();
    ArrayList<String> outputList = new ArrayList<String>();
 
    // new stat info object
    StatInfo gStatInfo = new StatInfo(); 
   // stat dump interval
    int dumpInterval = 10000;
    // new readwrite lock
    ReadWriteLock lock = new ReentrantReadWriteLock(false); 

    // create thread pool 
    ExecutorService pool = Executors.newFixedThreadPool(1); 
    // start thread
    Operation oper = null;
    switch (operType) {
      case OPER_CREATE_DIR:
        oper = new CreateDirOp(userId, gStatInfo, inputList, outputList, lock);
      break;
      case OPER_LS_DIR:
        inputFile = "oper_create_dir.fileList." + userId;
        readFromInputFile(inputFile, inputList);
        oper = new LsDirOp(userId, gStatInfo, inputList, outputList, lock);
      break;
      case OPER_SAVE_SMALL_FILE:
        oper = new SaveSmallFileOp(userId, gStatInfo, inputList, outputList, lock);
      break;
      case OPER_FETCH_FILE:
        inputFile = "oper_save_file.fileList." + userId;
        readFromInputFile(inputFile, inputList);
        oper = new FetchFileOp(userId, gStatInfo, inputList, outputList, lock);
      break;
      case OPER_SAVE_LARGE_FILE:
        oper = new SaveLargeFileOp(userId, gStatInfo, inputList, outputList, lock);
      break;
      case OPER_LS_FILE:
        inputFile = "oper_save_file.fileList." + userId;
        readFromInputFile(inputFile, inputList);
        oper = new LsFileOp(userId, gStatInfo, inputList, outputList, lock);
      break;
      case OPER_MIX:
        oper = new MixOp(userId, gStatInfo, inputList, outputList, lock);
      break;
      default:
    }

    pool.execute(oper); 
    //}
    //DumpStat dumpStat = new DumpStat(gStatInfo, dumpInterval, lock);
    //pool.execute(dumpStat); 
    // close thread pool
    pool.shutdown(); 
    try {
      while (!pool.isTerminated()) {
        Thread.sleep(6000);
      } 
    } catch (InterruptedException ie) {
      pool.shutdownNow();
      //Thread.currentThread().interrupt();
    }
  }

  public static void readFromInputFile(String inputFile, ArrayList<String> inputList) {
    BufferedReader buffReader = null;
    try {
      buffReader = new BufferedReader(new FileReader(inputFile));
      String line = null;
      while ((line = buffReader.readLine()) != null) {
        inputList.add(line);
      }
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
      try {
        buffReader.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
} 

class Operation implements Runnable {
  protected String operType;
  protected String outputFile;
  protected long userId = 381;
  protected StatInfo statInfo = new StatInfo();
  protected StatInfo gStatInfo;
  protected ReadWriteLock myLock;
  protected ArrayList<String> inputList;
  protected ArrayList<String> outputList;
  protected Logger log;
  protected ClassPathXmlApplicationContext clientFactory;
  protected DefaultTfsManager tfsManager;
  protected static Random random = new Random();
  protected static String dirNamePrefix = "dir_";
  protected int nameIndex = 0;
  protected String beginDirPath = "/dir_" + nameIndex;
  protected String currDirPath = beginDirPath;
  protected String currFilePath;
 
  void execute() {
  }

  Operation(long userId, StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
    if (userId != 0) {
      this.userId = userId;
    }
    this.myLock = myLock;
    this.gStatInfo = gStatInfo;
    this.inputList = inputList;
    this.outputList = outputList;
  }

  @Override
  public void run() {
    log = Logger.getLogger("mergeRcDirClient");
    clientFactory = new ClassPathXmlApplicationContext("tfs.xml");
    tfsManager = (DefaultTfsManager) clientFactory.getBean("tfsManager");
    execute();
    tfsManager.destroy();
  }

  void addStatInfo(boolean ret) {
    if (ret) {
      statInfo.succCount++;
    }
    else {
      statInfo.failCount++;
    }
    statInfo.totalCount++;
  }

  void doStat(StatInfo statInfo) {
    gStatInfo.totalCount += statInfo.totalCount;
    gStatInfo.succCount += statInfo.succCount;
    gStatInfo.failCount += statInfo.failCount;
    statInfo.clear();
    if (gStatInfo.totalCount != 0) {
      gStatInfo.succRate = ((float)gStatInfo.succCount/(float)gStatInfo.totalCount) * 100;
    }
    log.debug("@ " + this.operType + " stat info, succCount: " + gStatInfo.succCount + ", failCount: " + gStatInfo.failCount
      + ", totalCount: " + gStatInfo.totalCount + ", succRate: " + gStatInfo.succRate + "%");

  }

  public void genNextDirPath() {
    nameIndex++;
    int randomVal = random.nextInt(100);
    // if path is too long, back to the top level
    if (currDirPath.length() > 240) {
      currDirPath = Path.join("/", dirNamePrefix + nameIndex);
      // still too long, change the dirNamePrefix
      if (currDirPath.length() > 240) {
        dirNamePrefix += "e_";
        nameIndex = 0;
        currDirPath = dirNamePrefix + nameIndex;
      }
    }
    else if (randomVal <= 50) { // width expand
      String parentDir;
      parentDir = Path.getParentDir(currDirPath);
      if (parentDir == null) {
        parentDir = "/";
      }
      for (int i = 0; i < randomVal % 5; i++) {
        parentDir = Path.getParentDir(parentDir);
        if (parentDir == null) {
          parentDir = "/";
        }
      }
      currDirPath = Path.join(parentDir, dirNamePrefix + nameIndex);
    }
    else {  // depth expand
      currDirPath = Path.join(currDirPath, dirNamePrefix + nameIndex); 
    }
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

      return (int)crc.getValue();
    } 
    catch (Exception e) {
      e.printStackTrace();
    }
    return 0;
  }
}

class CreateDirOp extends Operation { 
 
  CreateDirOp(long userId, StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
     super(userId, gStatInfo, inputList, outputList, myLock);
     this.operType = "oper_create_dir";
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start create dir operation ==" + tid + "== thread");
    long count = 0;
    boolean ret = false;
    boolean autoGenDir = true;

    if (0 != inputList.size()) {
      autoGenDir = false;
    }
    if (autoGenDir) {
        outputFile = operType + ".fileList";
        BufferedWriter buffWriter = null;
      try {
        buffWriter = new BufferedWriter(new FileWriter(outputFile));
        while (true) {
          ret = tfsManager.createDir(appId, userId, currDirPath);
          log.debug("@@ create Dir dirPath: " + currDirPath + (ret ? " success": " failed"));
          addStatInfo(ret);
          if (ret) { // success
            String record = appId + " " + userId + " " + currDirPath + " " + Path.getBaseName(currDirPath);
            outputList.add(record); 
          }
          count++;
          genNextDirPath();
          if (count % 100 == 0) {
            for (int i = 0; i < outputList.size(); i++) {
              buffWriter.write(outputList.get(i));
              buffWriter.newLine();
            }
            buffWriter.flush();
            outputList.clear();
            myLock.writeLock().lock(); 
            doStat(statInfo);
            myLock.writeLock().unlock();
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          for (int i = 0; i < outputList.size(); i++) {
            buffWriter.write(outputList.get(i));
            buffWriter.newLine();
          }
          buffWriter.flush();
          buffWriter.close();
          outputList.clear();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    else {  // create the given dir list
      for (int i = 0; i < inputList.size(); i++) {
        ret = tfsManager.createDir(appId, userId, inputList.get(i));
        log.debug("@@ create Dir dirPath: " + currDirPath + (ret ? " success": " failed"));
        addStatInfo(ret);
        count++;
        if (count % 1000 == 0) {
          myLock.writeLock().lock(); 
          doStat(statInfo);
          myLock.writeLock().unlock();
        }
      }
    }
  }




}

class LsFileOp extends Operation { 

  FileMetaInfo fileMetaInfo = null;

  LsFileOp(long userId, StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
     super(userId, gStatInfo, inputList, outputList, myLock);
     this.operType = "oper_ls_file";
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start ls file operation ==" + tid + "== thread");
    long count = 0;
    boolean ret = false;

    for (int i = 0; i < inputList.size(); i++) {
      String record = inputList.get(i);
      String [] tmp = record.split(" ");
      if (tmp.length != 4) {
        continue;
      }
      appId = Long.parseLong(tmp[0]);
      userId = Long.parseLong(tmp[1]);
      String filePath = tmp[2];
      fileMetaInfo = tfsManager.lsFile(appId, userId, filePath);
      ret = (fileMetaInfo == null) ? false : true; 
      log.debug("@@ ls appId: " + appId + ", userId: " + userId + ", filePath: " + filePath + (ret ? " success": " failed"));
      addStatInfo(ret);
      count++;
      if (count % 100 == 0) {
        myLock.writeLock().lock(); 
        doStat(statInfo);
        myLock.writeLock().unlock();
      }
    }
    doStat(statInfo);
  }
}

class LsDirOp extends Operation { 

  List<FileMetaInfo> fileMetaInfoList = null;

  LsDirOp(long userId, StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
     super(userId, gStatInfo, inputList, outputList, myLock);
     this.operType = "oper_ls_dir";
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start ls dir operation ==" + tid + "== thread");
    long count = 0;
    boolean ret = false;

    for (int i = 0; i < inputList.size(); i++) {
      String record = inputList.get(i);
      String [] tmp = record.split(" ");
      if (tmp.length != 4) {
        continue;
      }
      appId = Long.parseLong(tmp[0]);
      userId = Long.parseLong(tmp[1]);
      String dirPath = tmp[2];
      fileMetaInfoList = tfsManager.lsDir(appId, userId, dirPath);
      ret = (fileMetaInfoList == null) ? false : true; 
      log.debug("@@ ls appId: " + appId + ", userId: " + userId + ", dirPath: " + dirPath + (ret ? " success": " failed"));
      addStatInfo(ret);
      count++;
      if (count % 100 == 0) {
        myLock.writeLock().lock(); 
        doStat(statInfo);
        myLock.writeLock().unlock();
      }
    }
    doStat(statInfo);
  }
}

class FetchFileOp extends Operation { 

  String fetchedFile = "/home/admin/workspace/tfs.dev/tfs-test/merge-rc-dir/fetchedFile";

  FetchFileOp(long userId, StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
     super(userId, gStatInfo, inputList, outputList, myLock);
     this.operType = "oper_fetch_file";
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start fetch file operation ==" + tid + "== thread");
    long count = 0;
    boolean ret = false;

    for (int i = 0; i < inputList.size(); i++) {
      String record = inputList.get(i);
      String [] tmp = record.split(" ");
      if (tmp.length != 5) {
        continue;
      }
      appId = Long.parseLong(tmp[0]);
      userId = Long.parseLong(tmp[1]);
      String filePath = tmp[2];
      int crcOrig = Integer.parseInt(tmp[4]);
      ret = tfsManager.fetchFile(appId, userId, fetchedFile, filePath);
      if (ret) {
        int crcFetched = getCrc(fetchedFile);
        ret = (crcOrig == crcFetched ? true : false);
      }
      log.debug("@@ fetchFile appId: " + appId + ", userId: " + userId + ", filePath: " + filePath + (ret ? " success": " failed"));
      addStatInfo(ret);
      count++;
      if (count % 100 == 0) {
        myLock.writeLock().lock(); 
        doStat(statInfo);
        myLock.writeLock().unlock();
      }
    }
    doStat(statInfo);
  }
}

class SaveSmallFileOp extends Operation { 
 
  public static String localSmallFile = "/home/admin/workspace/tfs.dev/tfs-test/merge-rc-dir/100K";
  public static String smallFileName = "smallfile";
  public static int crc = 0;

  SaveSmallFileOp(long userId, StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
     super(userId, gStatInfo, inputList, outputList, myLock);
     this.operType = "oper_save_small_file";
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start save small file operation ==" + tid + "== thread");
    long count = 0;
    boolean ret = false;
    boolean autoGenDir = true;

    if (0 != inputList.size()) {
      autoGenDir = false;
    }
    if (autoGenDir) {
        outputFile = "oper_save_file.fileList." + userId;
        BufferedWriter buffWriter = null;
      try {
        buffWriter = new BufferedWriter(new FileWriter(outputFile));
        while (true) {
          ret = tfsManager.createDir(appId, userId, currDirPath);
          log.debug("@@ create Dir dirPath: " + currDirPath + (ret ? " success": " failed"));
          if (ret) { // success
            currFilePath = currDirPath + "/" + smallFileName;
            ret = tfsManager.saveFile(appId, userId, localSmallFile, currFilePath);
            log.debug("@@ save local file: " + localSmallFile + " to: " + currFilePath + (ret ? " success": " failed"));
            if (ret) {
              crc = getCrc(localSmallFile);
              String record = appId + " " + userId + " " + currFilePath + " " + Path.getBaseName(currFilePath) + " " + crc;
              outputList.add(record); 
            }
          }
          addStatInfo(ret);
          count++;
          genNextDirPath();
          if (count % 100 == 0) {
            for (int i = 0; i < outputList.size(); i++) {
              buffWriter.write(outputList.get(i));
              buffWriter.newLine();
            }
            buffWriter.flush();
            outputList.clear();
            myLock.writeLock().lock(); 
            doStat(statInfo);
            myLock.writeLock().unlock();
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          for (int i = 0; i < outputList.size(); i++) {
            buffWriter.write(outputList.get(i));
            buffWriter.newLine();
          }
          buffWriter.flush();
          buffWriter.close();
          outputList.clear();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    else {  // TODO: save the given file list
      for (int i = 0; i < inputList.size(); i++) {
        ret = tfsManager.createDir(appId, userId, inputList.get(i));
        log.debug("@@ create Dir dirPath: " + currDirPath + (ret ? " success": " failed"));
        addStatInfo(ret);
        count++;
        if (count % 100 == 0) {
          myLock.writeLock().lock(); 
          doStat(statInfo);
          myLock.writeLock().unlock();
        }
      }
    }
  }

}

class SaveLargeFileOp extends Operation { 
 
  public static String localLargeFile = "/home/admin/workspace/tfs.dev/tfs-test/merge-rc-dir/2.1G";
  public static String largeFileName = "largefile";
  public static int crc = 0;

  SaveLargeFileOp(long userId, StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
     super(userId, gStatInfo, inputList, outputList, myLock);
     this.operType = "oper_save_large_file";
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    long tid = Thread.currentThread().getId();
    log.info("start save large file operation ==" + tid + "== thread");
    long count = 0;
    int fileIndex = 0;
    boolean ret = false;
    boolean autoGenDir = true;

    if (0 != inputList.size()) {
      autoGenDir = false;
    }
    if (autoGenDir) {
        outputFile = "oper_save_file.fileList." + userId;
        BufferedWriter buffWriter = null;
      try {
        buffWriter = new BufferedWriter(new FileWriter(outputFile));
        while (true) {
          ret = tfsManager.createDir(appId, userId, currDirPath);
          log.debug("@@ create Dir dirPath: " + currDirPath + (ret ? " success": " failed"));
          if (ret) { // success
            currFilePath = currDirPath + "/" + largeFileName + fileIndex;
            fileIndex++;
            ret = tfsManager.saveFile(appId, userId, localLargeFile, currFilePath);
            log.debug("@@ save local file: " + localLargeFile + " to: " + currFilePath + (ret ? " success": " failed"));
            if (ret) {
              crc = getCrc(localLargeFile);
              String record = appId + " " + userId + " " + currFilePath + " " + Path.getBaseName(currFilePath) + " " + crc;
              outputList.add(record); 
            }
          }
          addStatInfo(ret);
          count++;
          genNextDirPath();
          if (count % 5 == 0) {
            for (int i = 0; i < outputList.size(); i++) {
              buffWriter.write(outputList.get(i));
              buffWriter.newLine();
            }
            buffWriter.flush();
            outputList.clear();
            myLock.writeLock().lock(); 
            doStat(statInfo);
            myLock.writeLock().unlock();
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          for (int i = 0; i < outputList.size(); i++) {
            buffWriter.write(outputList.get(i));
            buffWriter.newLine();
          }
          buffWriter.flush();
          buffWriter.close();
          outputList.clear();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    else {  // TODO: save the given file list
      for (int i = 0; i < inputList.size(); i++) {
        ret = tfsManager.createDir(appId, userId, inputList.get(i));
        log.debug("@@ create Dir dirPath: " + currDirPath + (ret ? " success": " failed"));
        addStatInfo(ret);
        count++;
        if (count % 100 == 0) {
          myLock.writeLock().lock(); 
          doStat(statInfo);
          myLock.writeLock().unlock();
        }
      }
    }
  }

}

class MixOp extends Operation { 

  MixOp(long userId, StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
     super(userId, gStatInfo, inputList, outputList, myLock);
     this.operType = "oper_mix";
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();

    String localSmallFile = "1K.jpg"; 
    String retFile = "retFile"; 
    long tid = Thread.currentThread().getId();
    log.info("start mix operation ==" + tid + "== thread");
    long count = 0;
    boolean ret = false;
    while (true) {
      count++;
      String dirPath = "/dir_" + String.valueOf(tid) + "_" + String.valueOf(count);
      ret = tfsManager.createDir(appId, userId, dirPath);
      log.debug("@@ create Dir dirPath: " + dirPath + (ret ? "success": " failed"));
      addStatInfo(ret);
      if (ret) {
        for (int i = 0; i < 500; i++) {
          String filePath = dirPath + "/file_" + String.valueOf(tid) + "_" + String.valueOf(i);
          ret = tfsManager.saveFile(appId, userId, localSmallFile, filePath);
          log.debug("@@ save file: " + filePath + (ret ? "success": " failed"));
          addStatInfo(ret);
          if (ret)
          {
            ret = tfsManager.fetchFile(appId, userId, retFile, filePath);
            addStatInfo(ret);
            ret = tfsManager.rmFile(appId, userId, filePath);
            addStatInfo(ret);
          }
        }
        ret = tfsManager.rmDir(appId, userId, dirPath);
      }
      if (count % 1000 == 0)
      {
        myLock.writeLock().lock(); 
        doStat(statInfo);
        myLock.writeLock().unlock();
      }
    }
  }
} 

class DumpStat implements Runnable { 
  Logger log = Logger.getLogger("mergeRcDirClient");
  private int dumpInterval = 10000;
  private StatInfo gStatInfo;
  private ReadWriteLock myLock;

  DumpStat(StatInfo gStatInfo, int dumpInterval, ReadWriteLock myLock)
  {
    this.gStatInfo = gStatInfo;
    this.dumpInterval = dumpInterval;
    this.myLock = myLock;
  }

  @Override
    public void run() { 
      log.info("start dumpstat ===" + Thread.currentThread().getId() + " === thread");
      //for(;;) {
        myLock.readLock().lock();
        if (gStatInfo.totalCount != 0) {
          gStatInfo.succRate = ((float)gStatInfo.succCount/(float)gStatInfo.totalCount) * 100;
        }
        log.debug("@ client stat info, succCount: " + gStatInfo.succCount + ", failCount: " + gStatInfo.failCount
            + ", totalCount: " + gStatInfo.totalCount + ", succRate: " + gStatInfo.succRate + "%");
        myLock.readLock().unlock();
        try {
          Thread.sleep(dumpInterval);
        }
        catch(Exception e)
        {}
      //}
    }
} 

/** 
 * ÐÓ¿¨Õ»§£¬¿Éæ͸֧ 
 */ 
class StatInfo{ 
  public long totalCount;
  public long succCount;
  public long failCount;
  public float succRate;

  public void clear() {
    totalCount = 0;
    succCount = 0;
    failCount = 0;
    succRate = 0;
  }

  @Override 
    public String toString() { 
      return "StatInfo{" + 
        "totalCount='" + totalCount + '\'' + 
        "succCount='" + succCount + '\'' + 
        "succCount='" + failCount + '\'' + 
        "succRate='" + succRate + '\'' + 
        '}'; 
    } 
}


// path methods
class Path {
  public static String getBaseName(String filePath) {
      if (null == filePath) {
          return null;
      }
      String strSlash = "/";
      if (filePath.equals(strSlash)) {
          return filePath;
      }
      String tmpPath = filePath;
      if (filePath.endsWith(strSlash)) {
          tmpPath = filePath.substring(0, filePath.length() - 1);
      }
      int lastPos = filePath.lastIndexOf(strSlash);
      if (-1 == lastPos) {
          return null;
      }
      String baseName = null;
      baseName = tmpPath.substring(lastPos + 1, tmpPath.length());
      return baseName;
  }

  public static String getParentDir(String filePath) {
    if (null == filePath) {
      return null;
    }
    String strSlash = "/";
    if (filePath.equals(strSlash)) {
      return filePath;
    }
    String tmpPath = filePath;
    if (filePath.endsWith(strSlash)) {
      tmpPath = filePath.substring(0, filePath.length() - 1);
    }
    int lastPos = filePath.lastIndexOf(strSlash);
    if (-1 == lastPos) {
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
    return parentDir;
  }

  public static String join(String parentDir, String baseName) {
    if (null == parentDir || null == baseName) {
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

}
