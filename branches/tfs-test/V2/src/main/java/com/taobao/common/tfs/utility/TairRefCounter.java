package com.taobao.common.tfs.utility;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.taobao.common.tfs.DefaultTfsManager;
import com.taobao.common.tfs.TfsException;
import com.taobao.common.tfs.common.TfsConstant;
import com.taobao.common.tfs.common.TfsUtil;
import com.taobao.common.tfs.packet.RespRcLoginMessage;
import com.taobao.common.tfs.rc.RcBaseInfo;
import com.taobao.common.tfs.rc.RcClusterInfo;
import com.taobao.common.tfs.rc.RcClusterRackInfo;
import com.taobao.common.tfs.rc.RcSessionHelper;
import com.taobao.common.tfs.rc.TairConfigInfo;
import com.taobao.common.tfs.unique.TairUniqueStore;
import com.taobao.common.tfs.unique.UniqueValue;

public class TairRefCounter {
	private byte[] data;
	private int offset;
	private int length;
	private DefaultTfsManager tfsManager;

	private TairUniqueStore uniqueStore = null;
	private Logger logger = Logger.getLogger(TairRefCounter.class);

	public TairRefCounter(DefaultTfsManager tfsManager) {
		this.tfsManager = tfsManager;
		uniqueStore = new TairUniqueStore();
		initUniqueStore();
	}

	public int getRefCounter() {
		try {
			int ret = getUniqueValue().getReferenceCount();
			if (ret == TfsConstant.UNIQUE_QUERY_NOT_EXIST) {
				return 0;
			} else {
				return ret;
			}
		} catch (TfsException e) {
			logger.error("in getRefCounter exception: " + e.getMessage());
			return -1;
		}
	}

	public String getTfsName() {
		try {
			return getUniqueValue().getTfsName();
		} catch (TfsException e) {
			logger.error("in getTfsName exception: " + e.getMessage());
			return "";
		}
	}

	public void setKeyElement(byte[] data, int offset, int length) {
		this.data = data;
		this.offset = offset;
		this.length = length;
	}

	public void setFileInputStream(FileInputStream input) throws IOException {
		int length = (int) input.getChannel().size();
		byte[] data = new byte[length];

		if (input.read(data) != length) {
			logger.error("expected length not matching the read length");
			throw new IOException();
		}

		this.data = data;
		this.offset = 0;
		this.length = length;
		input.close();
	}

	private void initUniqueStore() {
		RespRcLoginMessage rrlm = getRrlm();

		RcBaseInfo baseInfo = rrlm.getBaseInfo();

		for (RcClusterRackInfo clusterRackInfo : baseInfo.getClusterRackInfos()) {
			boolean writable = false;
			for (RcClusterInfo clusterInfo : clusterRackInfo.getClusterInfos()) {
				if (clusterInfo.getAccessType() == RcClusterRackInfo.CLUSTER_ACCESS_TYPE_READ_WRITE) {
					writable = true;
				}
			}

			if (writable) {
				setUniqueStoreInfo(clusterRackInfo.getDuplicateInfo());
			}
		}
		uniqueStore.init();
	}

	private int getLocalIp() {
		String appIp = tfsManager.getAppIp();
		if (appIp == null) {
			appIp = TfsUtil.getLocalIpString();
		}

		return (int) (TfsUtil.hostToLong(appIp, 381) & 0xffffffffL);
	}

	private RespRcLoginMessage getRrlm() {
		RespRcLoginMessage rrlm = null;

		try {
			rrlm = RcSessionHelper.login(
					TfsUtil.hostToLong(tfsManager.getRcAddr()),
					tfsManager.getAppKey(), getLocalIp());
		} catch (TfsException e) {
			e.printStackTrace();
		}

		return rrlm;
	}

	private void setUniqueStoreInfo(TairConfigInfo duplicateInfo) {
		List<String> addrList = new ArrayList<String>();
		addrList.add(duplicateInfo.getMasterAddr());
		addrList.add(duplicateInfo.getSlaveAddr());
		((TairUniqueStore) uniqueStore).setConfigServerList(addrList);
		((TairUniqueStore) uniqueStore).setGroupName(duplicateInfo
				.getGroupName());
		((TairUniqueStore) uniqueStore).setNamespace(duplicateInfo
				.getNamespace());
	}

	private byte[] getKey() {
		return uniqueStore.getKey(data, offset, length);
	}

	private UniqueValue getUniqueValue() throws TfsException {
		return uniqueStore.query(getKey());
	}
}
