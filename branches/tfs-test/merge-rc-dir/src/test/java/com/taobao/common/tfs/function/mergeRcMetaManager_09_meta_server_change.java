/**
 * 
 */
package com.taobao.common.tfs.function;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.Ignore;
import java.io.File;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import com.taobao.gaia.KillTypeEnum;
import com.taobao.common.tfs.DefaultTfsManager; 


/**
 * @author Administrator/chuyu
 *
 */
public class mergeRcMetaManager_09_meta_server_change extends NameMetaBaseCase {

  @Before
    public void setUp(){

    }

  @After
    public void tearDown(){
    }

  @Test
    public void Function_00_remove_unservice_meta_server() {

/*      caseName = "Function_00_remove_other_meta_server";
      log.info(caseName + "===> start");
      long currentTime = 0;
      long failInterval = 0;

      mixOpCmd();
      currentTime = getClientCurrentTime();
      int msIndex = getUnServingMSIndex(appId, userId);
      Assert.assertTrue(msIndex > -1);
      killOneMetaserver(msIndex); 
      sleep(MAX_UPDATE_TIME);
     
      log.debug("operation faied between " + getFailedStartTime() + " and " + getFailedEndTime());
      //Assert.assertTrue(getFailedStartTime() - currentTime <= LEASE_TIME);
      //Assert.assertTrue(getFailedInterval());

      mixOpCmdStop();
*/
    } 
}
