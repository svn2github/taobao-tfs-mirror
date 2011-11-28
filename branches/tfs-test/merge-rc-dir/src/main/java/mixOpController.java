
import java.util.concurrent.ExecutorService; 
import java.util.concurrent.Executors; 
import java.util.concurrent.locks.ReadWriteLock; 
import java.util.concurrent.locks.ReentrantReadWriteLock; 

import java.io.File;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import java.lang.Exception;

import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.namemeta.FileMetaInfo;

public class mixOpController{

  public static void main(String[] args) { 

    // operation type
    final int OPER_CREATE_DIR = 1;
    final int OPER_LS_DIR = 2;
    final int OPER_FETCH = 4;
    final int OPER_SAVE = 8;
    final int OPER_UNLINK = 16;
    final int OPER_MIX_OP = 32;

    // new stat info object
    StatInfo gStatInfo = new StatInfo(); 
    ArrayList<String> inputList = new ArrayList<String>();
    ArrayList<String> outputList = new ArrayList<String>();
    // stat dump interval
    int dumpInterval = 10000;
    // new readwrite lock
    ReadWriteLock lock = new ReentrantReadWriteLock(false); 

    // create thread pool 
    ExecutorService pool = Executors.newFixedThreadPool(2); 
    // start thread
    //for (int i = 0; i < args[0]; i++)
    //{

    Operation oper = null;
    //int operType = OPER_MIX_OP;
    //int operType = OPER_CREATE_DIR;
    int operType = OPER_LS_DIR;
    switch (operType) {
      case OPER_CREATE_DIR:
        oper = new CreateDirOp(gStatInfo, inputList, outputList, lock);
      break;
      case OPER_LS_DIR:
        String inputFile = "created_dir_list.log";
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
        oper = new LsDirOp(gStatInfo, inputList, outputList, lock);
      break;
      case OPER_MIX_OP:
        oper = new MixOp(gStatInfo, inputList, outputList, lock);
      break;
      default:
    }

    pool.execute(oper); 
    //}
    DumpStat dumpStat = new DumpStat(gStatInfo, dumpInterval, lock);
    pool.execute(dumpStat); 
    // close thread pool
    pool.shutdown(); 

    // save outputList to file
    for (int i = 0; i < outputList.size(); i++) {
      System.out.println("index: " + outputList.get(i));
    }
  }
} 

class Operation implements Runnable {
  protected StatInfo statInfo = new StatInfo();
  protected StatInfo gStatInfo;
  protected ReadWriteLock myLock;
  protected ArrayList<String> inputList;
  protected ArrayList<String> outputList;
  protected Logger log;
  protected ClassPathXmlApplicationContext clientFactory;
  protected DefaultTfsManager tfsManager;

  void execute() {
  }

  Operation(StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
    this.myLock = myLock;
    this.gStatInfo = gStatInfo;
    this.inputList = inputList;
    this.outputList = outputList;
  }

  //@Override
  public void run() {
    log = Logger.getLogger("mergeRcDirClient");
    clientFactory = new ClassPathXmlApplicationContext("tfs.xml");
    tfsManager = (DefaultTfsManager) clientFactory.getBean("tfsManager");
    execute();
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
  }

}

class CreateDirOp extends Operation { 
 
  public static int nameIndex = 0;
  public static String dirNamePrefix = "dir_";
  public static String beginDirPath = "/dir_" + nameIndex;
  public static String currDirPath = beginDirPath;
  public static Random random = new Random();

  CreateDirOp(StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
     super(gStatInfo, inputList, outputList, myLock);
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();
    long userId = 361;

    long tid = Thread.currentThread().getId();
    log.info("start diroperation ==" + tid + "== thread");
    long count = 0;
    boolean ret = false;
    boolean autoGenDir = true;

    if (0 != inputList.size()) {
      autoGenDir = false;
    }
    if (autoGenDir) {
        String outputFile = "created_dir_list.log";
        BufferedWriter buffWriter = null;
      try {
        buffWriter = new BufferedWriter(new FileWriter(outputFile));
        while (true) {
          ret = tfsManager.createDir(appId, userId, currDirPath);
          log.debug("@@ create Dir dirPath: " + currDirPath + (ret ? " success": "failed"));
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
        log.debug("@@ create Dir dirPath: " + currDirPath + (ret ? " success": "failed"));
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


  public static void genNextDirPath() {
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

}

class LsDirOp extends Operation { 

  List<FileMetaInfo> fileMetaInfo = null;

  LsDirOp(StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
     super(gStatInfo, inputList, outputList, myLock);
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();
    long userId = 361;

    long tid = Thread.currentThread().getId();
    log.info("start diroperation ==" + tid + "== thread");
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
      fileMetaInfo = tfsManager.lsDir(appId, userId, dirPath);
      ret = (fileMetaInfo == null) ? false : true; 
      log.debug("@@ ls appId: " + appId + ", userId: " + userId + ", dirPath: " + dirPath + (ret ? " success": "failed"));
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

class MixOp extends Operation { 

  MixOp(StatInfo gStatInfo, ArrayList<String> inputList, ArrayList<String> outputList, ReadWriteLock myLock) {
     super(gStatInfo, inputList, outputList, myLock);
  }

  @Override
  void execute() { 
    long appId = tfsManager.getAppId();
    long userId = 61;

    String localSmallFile = "1K.jpg"; 
    String retFile = "retFile"; 
    long tid = Thread.currentThread().getId();
    log.info("start mixoperation ==" + tid + "== thread");
    long count = 0;
    boolean ret = false;
    while (true) {
      count++;
      String dirPath = "/dir_" + String.valueOf(tid) + "_" + String.valueOf(count);
      ret = tfsManager.createDir(appId, userId, dirPath);
      log.debug("@@ create Dir dirPath: " + dirPath + (ret ? "success": "failed"));
      addStatInfo(ret);
      if (ret) {
        for (int i = 0; i < 500; i++) {
          String filePath = dirPath + "/file_" + String.valueOf(tid) + "_" + String.valueOf(i);
          ret = tfsManager.saveFile(appId, userId, localSmallFile, filePath);
          log.debug("@@ save file: " + filePath + (ret ? "success": "failed"));
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

//  @Override
    public void run() { 
      log.info("start dumpstat ===" + Thread.currentThread().getId() + " === thread");
      //myLock.readLock().lock();
      if (gStatInfo.totalCount != 0) {
        gStatInfo.succRate = ((float)gStatInfo.succCount/(float)gStatInfo.totalCount) * 100;
      }
      log.debug("@ client stat info, succCount: " + gStatInfo.succCount + ", failCount: " + gStatInfo.failCount
          + ", totalCount: " + gStatInfo.totalCount + ", succRate: " + gStatInfo.succRate + "%");
      //myLock.readLock().unlock();
      try {
        log.debug("before sleep");
        Thread.sleep(dumpInterval);
        log.debug("sleep 10s");
      }
      catch(Exception e)
      {}
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
