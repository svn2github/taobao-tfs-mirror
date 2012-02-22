package com.taobao.common.tfs.nativetest;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

import com.taobao.common.tfs.tfsNameBaseCase;
import com.taobao.gaia.HelpProc;

public class bugVerificationSet extends tfsNameBaseCase {

	public String CLIENTIP = "10.232.36.206";
	public HelpProc Proc = new HelpProc();

	public String writeOneTfsFile(String clusterVipAddr, String szFileName){
		String strCmd = "/home/admin/tfs_bin/bin/tfstool -s " + clusterVipAddr + " -i 'put ";
//		strCmd += szFileName + "\' | sed \'s/.*\\(put.*\\)/\\1/g\'";
		strCmd += szFileName + "'";

		ArrayList<String> szOutputList = new ArrayList<String>();
		assertTrue(Proc.proStartBase(CLIENTIP, strCmd, szOutputList));
		assertTrue(szOutputList.size() > 0);

		for(String szLine : szOutputList ){
			log.info("----------------->" + szLine);
			if(szLine.endsWith("success.")){
				String szSubFields[] = szLine.split(" ");
				log.info("==================>" + szSubFields[3]);
				return szSubFields[3];
			}
		}

		return null;
	}

	public String getOneTfsFile(String clusterVipAddr, String tfsFileName){
		String strCmd = "/home/admin/tfs_bin/bin/tfstool -s " + clusterVipAddr + " -i 'get ";
		strCmd += tfsFileName + " test_file.jpg" + "'";

		ArrayList<String> szOutputList = new ArrayList<String>();
		assertTrue(Proc.proStartBase(CLIENTIP, strCmd, szOutputList));
		assertTrue(szOutputList.size() > 0);

		return szOutputList.get(0);
	}
	
	public boolean compareFiles(String szFileNameSrc, String szFileNameDst){
		String strCmd = "diff " + szFileNameSrc + " " + szFileNameDst;
		ArrayList<String> szOutputList = new ArrayList<String>();

		if(Proc.proStartBase(CLIENTIP, strCmd, szOutputList)){
			if(szOutputList.size() == 0){
				return true;
			}
		}

		return false;
	}

	@Test
	public void test_multi_cluster_read(){
		String szFileName = "/home/admin/running_server.lst";
		String clusterAAddr = "10.232.4.12:3215";
//		createFile("test_sync.jpg", 10 * (1<<10));
		
		String tfsFileName = writeOneTfsFile(clusterAAddr, szFileName);
		assertNotNull(tfsFileName);
		getOneTfsFile(clusterAAddr, tfsFileName);
		
		assertTrue(compareFiles(szFileName, "test_file.jpg"));
		/* we get two file, one is test_sync.jpg, one is test_file.jpg*/
		/* what we should do is to check the two file is the same */
	}
	
}
