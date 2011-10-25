
package com.taobao.common.tfs.function;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Before;

public class Function_name_meta_cache_test extends NameMetaBaseCase{

  //@Test
  public void Function_01_happy_path() {
    boolean bRet = false;
    caseName = "Function_01_happy_path";
    log.info(caseName + "====> start");

    bRet = createDirCmd();
    Assert.assertTrue(bRet);

    sleep(30);

    bRet = createDirCmdStop();
    Assert.assertTrue(bRet);

    bRet = queryDB("created_dir_list.log");
    Assert.assertTrue(bRet);

    log.info(caseName + "====> end");
  }

  @Test
  public void Function_02_clean_db_read() {
    boolean bRet = false;
    caseName = "Function_02_clean_db_read";
    log.info(caseName + "====> start");

    bRet = createDirCmd();
    Assert.assertTrue(bRet);

    sleep(30);

    bRet = createDirCmdStop();
    Assert.assertTrue(bRet);

    bRet = cleanDB("created_dir_list.log");
    Assert.assertTrue(bRet);

    bRet = lsDirCmd();
    Assert.assertTrue(bRet);

    bRet = lsDirMon();
    Assert.assertTrue(bRet);

    bRet = chkRateEnd(SUCCESS_RATE, OPER_LS_DIR);
    Assert.assertTrue(bRet);

    log.info(caseName + "====> end");
  }

  @After
  public void tearDown() {

  }

  @Before
  public void setUp() {

  }


}

