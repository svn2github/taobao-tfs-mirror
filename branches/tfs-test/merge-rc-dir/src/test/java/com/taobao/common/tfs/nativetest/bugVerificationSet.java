package com.taobao.common.tfs.nativetest;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

import com.taobao.common.tfs.tfsNameBaseCase;
import com.taobao.gaia.HelpProc;

public class bugVerificationSet {

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
			//log.info("----------------->" + szLine);
			if(szLine.endsWith("success.")){
				String szSubFields[] = szLine.split(" ");
				//log.info("==================>" + szSubFields[3]);
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

	//@Test
	public void test_multi_cluster_read()
	{
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
	
	@Test
	public void test_compactblk()
	{
		String Addr="10.232.36.210:7271";
		String strCmd = "/home/admin/workspace/yiming/tfs_bin/bin/ssm -s " + Addr + " -i block";
		ArrayList<String> log = new ArrayList<String>();
        
        
		ArrayList<String> List_block_id = new ArrayList<String>();
		assertTrue(Proc.proStartBase("10.232.36.210", strCmd, List_block_id));
	    
		int n = List_block_id.size();
		int i;
		for (i=0;i<n;i++)
		{
			System.out.println(List_block_id.get(i));
		}
		
		
//		ArrayList<String> T_List_block_id= new ArrayList<String>();
//		for (i=0;i<n;i++)
//		{
//			String a [] = List_block_id.get(i).split(" ");
//			T_List_block_id.add(a[0]);
//		}
//		String strCmd2 = "/home/admin/workspace/yiming/tfs_bin/bin/./admintool -s" + Addr + " -i 'block";
//		String s = null;
//		n=T_List_block_id.size();
//		for (i=0;i<n;i++)
//		{
//			s=strCmd2+T_List_block_id.get(i)+"'";
//			assertTrue(Proc.proStartBase("10.232.36.210", s, log));
//		}
	}
	
}
