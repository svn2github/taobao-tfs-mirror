package com.taobao.common.tfs.performance.base.worker;

import com.taobao.common.tfs.performance.base.WorkerConfig;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
//import org.springframework.context.support.ClassPathXmlApplicationContext;
import java.lang.System.*;

import com.taobao.common.tfs.*;

public class perf_createDir extends AbstractWorker {

	protected static Logger log = Logger.getLogger("tfs_perf_createDir");
	public long tid = 0;
	public String Ret = null;
	protected long num = 0;
	private DefaultTfsManager tfsManager = null;
	public static long appId;
	public long userId = 12;
	public long begin_time = 0;
	public long mid_time = 0;
	public String str = "";
	public String dir = "";
	public int inum = 0;
	public int sum = 100;
	public static int tcount = 0;
	public String strDir = "";

	public boolean before_work() {

		return true;
	}

	@Override
	public boolean do_work() {
		begin_time = System.currentTimeMillis();
		log.info("begin: " + begin_time);
		do {
			strDir = "/" + tid;
			long t_num = num;
			while (t_num > 0) {
				long tail = t_num - (t_num / sum) * sum;
				if (tail == 0) {
					strDir = "";
					num++;
					break;
				}
				strDir += tail;
				strDir += "/";

				t_num = t_num / sum;

			}
		} while ("" == strDir);

		boolean bRet = false;
		mid_time = System.currentTimeMillis();

		log.info("mid: " + (System.currentTimeMillis() - mid_time));
		log.info("Start to create dir dirname: " + strDir);
		bRet = true;
		bRet = tfsManager.createDir(appId, tid, strDir);
		if (false == bRet) {
			log.error("create dir dirname " + strDir + "failed");
			++num;
			return bRet;
		}
		++num;
		log.info("end: " + (System.currentTimeMillis() - mid_time));
		return true;
	}

	public boolean end_work() {
		return true;
	}

	@Override
	public boolean init(WorkerConfig config, long id) {
		tid = id;
		ClassPathXmlApplicationContext appContext = new ClassPathXmlApplicationContext(
				new String[] { "tfs.xml" });
		tfsManager = (DefaultTfsManager) appContext.getBean("tfsManager");
		appId = tfsManager.getAppId();
		userId = tid;
		return true;
	}
}
