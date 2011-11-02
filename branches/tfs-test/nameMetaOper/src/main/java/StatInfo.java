package com.taobao.common.tfs.nameMetaOper;

/** 
 * ÐÓ¿¨Õ»§£¬¿Éæ͸֧ 
 */ 
class StatInfo{ 
  public long totalCount;
  public long succCount;
  public long failCount;
  public float succRate;
  public long succTime;
  public long failTime;
  public long totalTime;
  public float succTPS;
  public float failTPS;
  public float totalTPS;

  public void clear() {
    totalCount = 0;
    succCount = 0;
    failCount = 0;
    succRate = 0;
    succTime = 0;
    failTime = 0;
    totalTime = 0;
    succTPS = (float)0;
    failTPS = (float)0;
    totalTPS = (float)0;
  }

  @Override 
    public String toString() { 
      return "StatInfo{" + 
        "totalCount='" + totalCount + '\'' + 
        "succCount='" + succCount + '\'' + 
        "succCount='" + failCount + '\'' + 
        "succRate='" + succRate + '\'' + 
        "succTime='" + totalTime + '\'' + 
        "failTime='" + totalTime + '\'' + 
        "totalTime='" + totalTime + '\'' + 
        "succTPS='" + succTPS + '\'' + 
        "failTPS='" + failTPS + '\'' + 
        "totalTPS='" + totalTPS + '\'' + 
        '}'; 
    } 
}
