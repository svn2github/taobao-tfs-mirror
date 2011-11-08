package com.taobao.common.tfs.performance.base.worker;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.taobao.common.tfs.performance.base.WorkerConfig;

import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.common.tfs.*;

public class perf_fetch extends AbstractWorker {

	protected static Logger log = Logger.getLogger("tfs_perf_save");
	public long tid = 0;
	public String Ret = null;
	public boolean Bret = false;
	protected static long num = 0;
	public static String res_path = "src/main/resources/";
	public List<String> Listname;

	private DefaultTfsManager tfsManager = new DefaultTfsManager();

	private static long appId;
	private static long userId = 12;

	public boolean before_work() {
		Random n = new Random();

		if (num == 0) {
			Listname = new ArrayList<String>();
			int i;
			String str;
			for (i = 1; i <= 50; i++) {
				str = "/test_" + tid + "thThread_" + i + "thFile";
				Bret = tfsManager.saveFile(appId, userId, res_path + "1m.jpg",
						str);
				Listname.add(str);
			}
			++num;
		} else {
			Ret = Listname.get(n.nextInt(49));
		}
		return true;
	}

	@Override
	public boolean do_work() {
		// System.out.println(Ret);
		Bret = tfsManager.fetchFile(appId, userId, res_path + "TEMP", Ret);
		if (false == Bret) {
			log.info("fetch file fail,the file name is " + Ret);
			return false;
		}
		return Bret;
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
		return true;
	}
}
