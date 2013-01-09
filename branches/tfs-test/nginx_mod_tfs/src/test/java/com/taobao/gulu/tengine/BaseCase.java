package com.taobao.gulu.tengine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.RequestEntity;
import org.apache.commons.httpclient.methods.multipart.FilePart;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

//import com.taobao.gulu.database.MySQLConnector;
import com.taobao.gulu.database.TFS;
import com.taobao.gulu.database.Tair;
import com.taobao.gulu.server.NginxServer;
import com.taobao.gulu.server.ShellServer;
import com.taobao.gulu.tools.VerifyTool;

//public class BaseCase extends TestCase {
public class BaseCase {
	public static final Log log = LogFactory.getLog(TestCase.class);
	protected static final ApplicationContext BEANFACTORY = new ClassPathXmlApplicationContext(
			"server.xml");
	protected static final NginxServer NGINX = (NginxServer) BEANFACTORY
			.getBean("nginxServer");
	protected static final ShellServer SERVER0432 = (ShellServer) BEANFACTORY
			.getBean("shellServer");

	protected static final ShellServer SERVER0429 = (ShellServer) BEANFACTORY
			.getBean("shellServer0429");
	
	protected static final ShellServer SERVER0435 = (ShellServer) BEANFACTORY
	.getBean("shellServer0435");

	protected static final ShellServer SERVER036202 = (ShellServer) BEANFACTORY
			.getBean("shellServer036202");

	protected static final ShellServer SERVER036209 = (ShellServer) BEANFACTORY
			.getBean("shellServer036209");

	protected static final ShellServer SERVER036210 = (ShellServer) BEANFACTORY
			.getBean("shellServer036210");

	protected static final TFS tfs_NginxA01 = (TFS) BEANFACTORY
			.getBean("tfs_NginxA01");
	protected static final TFS tfs_NginxA02 = (TFS) BEANFACTORY
			.getBean("tfs_NginxA02");
	protected static final TFS tfs_NginxA03 = (TFS) BEANFACTORY
			.getBean("tfs_NginxA03");
	protected static final TFS tfs_NginxB01 = (TFS) BEANFACTORY
			.getBean("tfs_NginxB01");
	protected static final TFS tfs_NginxB02 = (TFS) BEANFACTORY
			.getBean("tfs_NginxB02");
	protected static final TFS tfs_NginxB03 = (TFS) BEANFACTORY
			.getBean("tfs_NginxB03");
	protected static final TFS tfs_NginxC01 = (TFS) BEANFACTORY
			.getBean("tfs_NginxC01");
	protected static final TFS tfs_NginxD01 = (TFS) BEANFACTORY
			.getBean("tfs_NginxD01");
	
	protected static final TFS tfs_NginxIPMap01 = (TFS) BEANFACTORY
			.getBean("tfs_NginxIPMap01");
	protected static final TFS tfs_NginxIPMap02 = (TFS) BEANFACTORY
			.getBean("tfs_NginxIPMap02");
	
	protected static final Tair tair_01 = (Tair) BEANFACTORY
			.getBean("tfs_Tair");

	protected static final String jpgFile = "src/test/resources/image/jpg663x688.jpg";
	protected static final String bmpFile = "src/test/resources/image/bmp738x800.bmp";
	protected static final String tmpFile = "src\\test\\resources\\image\\tmp";
	protected static final String tmpFileDiff = "src\\test\\resources\\image\\tmp1";
	protected static final String swfFile = "src/test/resources/image/200881684353.swf";
	protected static final String cswFile = "src/test/resources/image/Effection_013.csw";
	protected static final String gifFile = "src/test/resources/image/gifNormal340x340.gif";
	protected static final String txtFile = "src/test/resources/image/gifNormal340x340.txt";
	protected static final String icoFile = "src/test/resources/image/ico128x128.ico";
	protected static final String pngFile = "src/test/resources/image/Problem151x168.png";
	protected static final String zipFile = "src/test/resources/image/zipFile.zip";
	protected static final String rarFile = "src/test/resources/image/rarFile.rar";
	protected static final String _2kFile = "src/test/resources/image/2kFile.asx";
	protected static final String _100kFile = "src/test/resources/image/ico128x128.ico";
	protected static final String _1MFile = "src/test/resources/image/1M.wma";
	protected static final String _14MFile = "src/test/resources/image/14M.exe";
	protected static final String _XXXX14MFile = "src/test/resources/image/xxxx14M.exe";
	protected static final String png10kFile = "src/test/resources/image/png10k.png";
	protected static final String wavFile = "src/test/resources/image/fenghuanghua.wav";
	protected static final String wmaFile = "src/test/resources/image/1M.wma";

	public List<String> put2TfsKeys = new ArrayList<String>();
		
	protected static final String CREATEMYSQLTABLES = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/create_table.sql";
	protected static final String INITMYSQLTABLES = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/init_table.sql";
	protected static final String INITMYSQLTABLES_META_ROOT_INFO = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/init_table_meta_root_info.sql";
	protected static final String DROPMYSQLTABLES = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/drop_database.sql";

	protected static final String UPDATE_LOGIC_A_READ = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/update_logic_a_read.sql";
	protected static final String UPDATE_LOGIC_A_UNUSE = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/update_logic_a_unuse.sql";
	protected static final String UPDATE_PHYSICS_READ = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/update_physics_read.sql";
	protected static final String UPDATE_PHYSICS_UNUSE = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/update_physics_unuse.sql";
	protected static final String UPDATE_PHYSICS_WRITE = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/update_physics_write.sql";
	protected static final String UPDATE_PHYSICS_T1B_WRITE_T1M_READ = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/update_physics_T1B_write_T1M_read.sql";
	protected static final String UPDATE_PHYSICS_T1B_WRITE_T1M_WRITE = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/update_physics_T1B_write_T1M_write.sql";
	protected static final String UPDATE_PHYSICS_T1B_READ_T1M_READ = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/update_physics_T1B_read_T1M_read.sql";
	
	protected static final String UPDATE_DUPLICATE_SERVER = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/update_duplicate_server.sql";
	protected static final String UPDATE_DUPLICATE_SERVER_UNUSE = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/update_duplicate_server_unuse.sql";
	protected static final String INIT_DUPLICATE_APP = "mysql -h 10.232.4.29 -u root -p123 < /home/gongyuan.cz/sql_for_restful/mysql/init_duplicate_app.sql";
	
	
	
	protected static final String STOP_NS_DS_036202 = "sh /home/gongyuan.cz/stopTFS.sh";
	protected static final String STOP_NS_DS_036209 = "sh /home/gongyuan.cz/stopTFS.sh";
	protected static final String STOP_NS_DS_036210 = "sh /home/gongyuan.cz/stopTFS.sh";
	protected static final String START_NS_DS_036202 = "sh /home/gongyuan.cz/startTFS.sh";
	protected static final String START_NS_DS_T2M_036202 = "sh /home/gongyuan.cz/startTFS_T2M.sh";
	protected static final String START_NS_DS_T2M_036202_2 = "sh /home/gongyuan.cz/startTFS_T2M2.sh";
	protected static final String START_NS_DS_T2M_036202_3 = "sh /home/gongyuan.cz/startTFS_T2M3.sh";
	protected static final String START_NS_DS_036209 = "sh /home/gongyuan.cz/startTFS.sh";
	protected static final String START_NS_DS_036210 = "sh /home/gongyuan.cz/startTFS.sh";
	protected static final String START_NS_DS_DOUBLE_036202 = "sh /home/gongyuan.cz/start_double_TFS.sh";
	protected static final String START_NS_DS_DOUBLE_036209 = "sh /home/gongyuan.cz/start_double_TFS.sh";
	protected static final String START_NS_DS_DOUBLE_036210 = "sh /home/gongyuan.cz/start_double_TFS.sh";
	protected static final String START_NS_DS_DOUBLE_BACKUP_036202 = "sh /home/gongyuan.cz/start_double_backup_TFS.sh";
	protected static final String START_NS_DS_DOUBLE_BACKUP_036209 = "sh /home/gongyuan.cz/start_double_backup_TFS.sh";
	protected static final String START_NS_DS_DOUBLE_BACKUP_036210 = "sh /home/gongyuan.cz/start_double_backup_TFS.sh";
	protected static final String START_NS_DS_T1B_BACKUP_036202 = "sh /home/gongyuan.cz/start_T1B_backup_TFS.sh";
	protected static final String START_NS_DS_T1B_BACKUP_036209 = "sh /home/gongyuan.cz/start_T1B_backup_TFS.sh";
	protected static final String START_NS_DS_T1B_BACKUP_036210 = "sh /home/gongyuan.cz/start_T1B_backup_TFS.sh";
	protected static final String RESTART_META = "sh /home/gongyuan.cz/restart_meta.sh";
	
	private static void init() {
		// init tfs
		tfs_NginxA01.init();
		tfs_NginxA02.init();
		tfs_NginxA03.init();
		tfs_NginxB01.init();
		tfs_NginxB02.init();
		tfs_NginxB03.init();
		tfs_NginxIPMap01.init();
		tfs_NginxIPMap02.init();
		//init tair
		tair_01.init();
	}

	static {
		init();
	}
	
	@Rule
	public TestWatcher watchman = new TestWatcher() {
		protected String caseIdentifier = "";

		protected void starting(Description d) {
			caseIdentifier = d.getClassName() + "." + d.getMethodName();
			System.out.println("starting: " + caseIdentifier);
		}

		protected void succeeded(Description d) {
			caseIdentifier = d.getClassName() + " " + d.getMethodName();
			System.out.println("succeeded: " + caseIdentifier);
		}

		protected void failed(Throwable e, Description d) {
			caseIdentifier = d.getClassName() + " " + d.getMethodName();
			System.out.println("failed: " + caseIdentifier);
		}

		protected void finished(Description d) {
			caseIdentifier = d.getClassName() + " " + d.getMethodName();
			System.out.println("finished: " + caseIdentifier);
		}

	};
	
	//@After
//	public void deleteDirandFile (){
//		TFS tfsServer = tfs_NginxA01;
//		tfsServer.rmDirRecursive( 1, 1, "/");
//		tfsServer.rmDirRecursive( 1, 2, "/");
//		TFS tfsServer1 = tfs_NginxB01;
//		tfsServer1.rmDirRecursive( 2, 1, "/");
//		tfsServer1.rmDirRecursive( 1, 2, "/");
//	}
	
	protected PostMethod setPostMethod(String url) {
		PostMethod postMethod = new PostMethod(url);

		return postMethod;
	}
	protected PostMethod setMvDirPostMethod(String url ,String srcDir) {
		PostMethod postMethod = new PostMethod(url);
		postMethod.setRequestHeader("x-tb-move-source", srcDir); 
		return postMethod;
	}
	protected PostMethod setMvFilePostMethod(String url ,String srcFile) {
		PostMethod postMethod = new PostMethod(url);
		postMethod.setRequestHeader("x-tb-move-source", srcFile); 
		return postMethod;
	}
	
	protected PostMethod setPostMethod(String url, String filePath) 
	{
		PostMethod postMethod = new PostMethod(url);
		File uploadFile = new File(filePath);

		try 
		{
			FileInputStream fileInputStream = new FileInputStream(uploadFile);
			InputStreamRequestEntity inputStreamRequestEntity = new InputStreamRequestEntity(fileInputStream);
			postMethod.setRequestEntity((RequestEntity) inputStreamRequestEntity);
		} 
		catch (Exception ex) 
		{
			ex.printStackTrace();
		}
		return postMethod;
	}

	protected PostMethod setPostMethodMultiPart(String url, String filePath) {
		PostMethod postMethod = new PostMethod(url);
		File uploadFile = new File(filePath);

		try {
			Part[] parts = { new FilePart(uploadFile.getName(), uploadFile) };
			postMethod.setRequestEntity(new MultipartRequestEntity(parts,
					postMethod.getParams()));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return postMethod;
	}

	protected DeleteMethod setDeleteMethod(String url) 
	{
		DeleteMethod deleteMethod = new DeleteMethod(url);

		return deleteMethod;
	}

	protected GetMethod setGetMethod(String url) 
	{
		GetMethod getMethod = new GetMethod(url);
		return getMethod;
	}
	
	
	
	protected HeadMethod headMethod(String url) {
		HeadMethod headMethod = new HeadMethod(url);

		return headMethod;
	}

	protected PutMethod setPutMethod(String url, String filePath) {
		PutMethod putMethod = new PutMethod(url);

		try {
			InputStreamRequestEntity inputStreamRequestEntity = new InputStreamRequestEntity(
					new FileInputStream(filePath));
			putMethod.setRequestEntity(inputStreamRequestEntity);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return putMethod;
	}

	protected String splitString(String tfsFileName) {
		String[] tmp = tfsFileName.split("\\.");
		String result = new String();
		for (int size = 0; size < (tmp.length - 1); size++) {
			result = result + tmp[size];
		}
		return result;
	}

	protected boolean deleteFile(String fileName) 
	{
		File file = new File(System.getProperty("user.dir") + "\\" + fileName);
		if (file.isFile() && file.exists()) 
		{
			System.gc();
			if (file.delete()) 
			{
				System.out.println("delete file : " + System.getProperty("user.dir") + "\\" + fileName + " success!");
				return true;
			} 
			else 
			{
				System.out.println("delete file : " + System.getProperty("user.dir") + "\\" + fileName + " fail in delete!");
				return false;
			}

		} 
		else 
		{
			System.out.println("delete file : " + System.getProperty("user.dir") + "\\" + fileName + " fail!");
			return false;
		}
	}
	
	protected void recoverTFS() throws Exception{
		VerifyTool tools = new VerifyTool();
		tools.verifyCMDWithDumbPTY(SERVER036202, STOP_NS_DS_036202, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036209, STOP_NS_DS_036209, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036210, STOP_NS_DS_036210, "", "");
		TimeUnit.SECONDS.sleep(15);
		tools.verifyCMDWithDumbPTY(SERVER036202, START_NS_DS_036202, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036209, START_NS_DS_036209, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036210, START_NS_DS_036210, "", "");
		TimeUnit.SECONDS.sleep(15);
	}
	
	protected void recoverTFStoDoubleWrite() throws Exception{
		VerifyTool tools = new VerifyTool();
		tools.verifyCMDWithDumbPTY(SERVER036202, STOP_NS_DS_036202, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036209, STOP_NS_DS_036209, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036210, STOP_NS_DS_036210, "", "");
		TimeUnit.SECONDS.sleep(15);
		tools.verifyCMDWithDumbPTY(SERVER036202, START_NS_DS_DOUBLE_036202, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036209, START_NS_DS_DOUBLE_036209, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036210, START_NS_DS_DOUBLE_036210, "", "");	
		TimeUnit.SECONDS.sleep(15);
	}
	
	protected void recoverTFStoDoubleBackup() throws Exception{
		VerifyTool tools = new VerifyTool();
		tools.verifyCMDWithDumbPTY(SERVER036202, STOP_NS_DS_036202, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036209, STOP_NS_DS_036209, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036210, STOP_NS_DS_036210, "", "");
		TimeUnit.SECONDS.sleep(15);
		tools.verifyCMDWithDumbPTY(SERVER036202, START_NS_DS_DOUBLE_BACKUP_036202, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036209, START_NS_DS_DOUBLE_BACKUP_036209, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036210, START_NS_DS_DOUBLE_BACKUP_036210, "", "");	
		TimeUnit.SECONDS.sleep(15);
	}
	
	protected void recoverTFStoT1BBackupToT1M() throws Exception{
		VerifyTool tools = new VerifyTool();
		tools.verifyCMDWithDumbPTY(SERVER036202, STOP_NS_DS_036202, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036209, STOP_NS_DS_036209, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036210, STOP_NS_DS_036210, "", "");
		TimeUnit.SECONDS.sleep(15);
		tools.verifyCMDWithDumbPTY(SERVER036202, START_NS_DS_T1B_BACKUP_036202, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036209, START_NS_DS_T1B_BACKUP_036209, "", "");
		tools.verifyCMDWithDumbPTY(SERVER036210, START_NS_DS_T1B_BACKUP_036210, "", "");	
		TimeUnit.SECONDS.sleep(15);
	}

	protected void restartMeta()throws Exception
	{
		VerifyTool tools = new VerifyTool();
		tools.verifyCMDWithDumbPTY(SERVER036202,RESTART_META,"","");
		TimeUnit.SECONDS.sleep(2);
	}
}
