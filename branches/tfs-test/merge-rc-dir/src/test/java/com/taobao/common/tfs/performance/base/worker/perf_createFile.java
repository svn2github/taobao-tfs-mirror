package com.taobao.common.tfs.performance.base.worker;

import com.taobao.common.tfs.performance.base.WorkerConfig;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
//import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.*;

public class perf_createFile extends AbstractWorker {

	protected static Logger log = Logger.getLogger("tfs_perf_createFile");
	public long tid = 0;
	public String Ret = null;
	public String sDir = "";
	protected long num = 0;

	private DefaultTfsManager tfsManager = null;

	public long appId;
	public long userId = 12;
	public long flag = 0;

	public boolean before_work() {
		return true;
	}

	@Override
	public boolean do_work() {
		String path = "/" + tid;
		long count = num;
		while (count >= 100) {
			count = count / 100;
			path += "/" + count % 100;
			if ((num - flag) >= 100 && count <= 100) {
				log.info("Create dir :" + path);
				if (tfsManager.createDir(appId, userId, path) == false) {
					log.error("Create dir fail: " + path);
				}
			}
		}
		if ((num - flag) >= 100)
			flag = num;

		path += "/" + tid + "_" + num;
		boolean bRet = false;
		log.info("create file name:" + path);

		bRet = tfsManager.createFile(appId, userId, path);
		if (false == bRet) {
			log.error("create file name:" + path + " failed!");
		}
		++num;
		return bRet;
	}

	public boolean end_work() {
		return true;
	}

	@Override
	public boolean init(WorkerConfig config, long id) {
		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(
				new String[] { "tfs.xml" });
		tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
		appId = tfsManager.getAppId();
		userId = id;
		tid = id;

		/* create own operate dir */
		log.info("create own dir: " + "/" + tid);
		if (tfsManager.createDir(appId, userId, "/" + tid) == false) {
			log.error("create own dir fail: " + "/" + tid);
			return false;
		}
		return true;
	}

}
