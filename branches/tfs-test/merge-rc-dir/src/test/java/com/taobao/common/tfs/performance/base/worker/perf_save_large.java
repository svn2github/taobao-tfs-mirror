package com.taobao.common.tfs.performance.base.worker;

import com.taobao.common.tfs.performance.base.WorkerConfig;

import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.*;

public class perf_save_large extends AbstractWorker {

	protected static Logger log = Logger.getLogger("tfs_perf_save_large");
	public long tid = 0;
	protected long num = 0;
	public static String res_path = "src/main/resources/";

	private DefaultTfsManager tfsManager = new DefaultTfsManager();

	private long appId;
	private long userId = 12;
	private long flag = 0;

	public boolean before_work() {
		return true;
	}

	@Override
	public boolean do_work() {
		boolean bRet;
		String path = "/" + tid;
		long count = num;
		while (count >= 100) {
			count = count / 100;
			path += "/" + count % 100;
			if ((num - flag) >= 100 && count < 100) {
				if (tfsManager.createDir(appId, userId, path) == false) {
					log.error("Create dir fail: " + path);
				}
				log.info("Creat dir success:" + path);
			}
		}
		if ((num - flag) >= 100)
			flag = num;

		path += "/" + tid + "_" + num + ".jpg";
		bRet = tfsManager.saveFile(appId, userId, res_path + "100m.jpg", path);
		if (bRet == false) {
			log.info("save file fail,the file name is: " + path);
		}
		num++;
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
		tid = id;
		userId = id;

		/* create own operate dir */
		log.info("create own dir: " + "/" + tid);
		if (tfsManager.createDir(appId, userId, "/" + tid) == false) {
			log.error("create own dir fail: " + "/" + tid);
			return false;
		}
		return true;
	}
}
