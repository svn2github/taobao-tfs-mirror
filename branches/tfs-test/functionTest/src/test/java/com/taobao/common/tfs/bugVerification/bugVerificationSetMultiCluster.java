package com.taobao.common.tfs.bugVerification;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.MultiClusterSyncBaseCase;
import com.taobao.gaia.AppGrid;
import com.taobao.gaia.AppServer;
import com.taobao.gaia.KillTypeEnum;

public class bugVerificationSetMultiCluster extends MultiClusterSyncBaseCase {

	public boolean killTfsGridNs(AppGrid tfsAppGrid, int type)
	{
		boolean bRet = false;
		List<AppServer> listNs = tfsAppGrid.getCluster(NSINDEX).getServerList();

		/* Find the master ns */
		if( (type & 0x01) != 0 )
		{
			bRet = listNs.get(0).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false) return bRet;
		} 
		if( (type & 0x02) != 0 ){
			bRet = listNs.get(1).stop(KillTypeEnum.NORMALKILL, WAITTIME);
			if (bRet == false) return bRet;
		}

		return bRet;
	}
	@Test
	public void test_148219_multi_cluster_sync_failure(){
		boolean bRet = false;

		/* Set loop flag */
		bRet = setSeedFlag(LOOPON);
		Assert.assertTrue(bRet);

		/* Set seed size */
		bRet = setSeedSize(1);
		Assert.assertTrue(bRet);

		/* Set unlink ratio */
		bRet = setUnlinkRatio(50);
		Assert.assertTrue(bRet);

		writeCmd();
		Assert.assertTrue(bRet);
		sleep(300);

		/* Stop write process */
		bRet = writeCmdStop();
		Assert.assertTrue(bRet);
	
		killTfsGridNs(tfsGrid2, 3);
	}
}
