package com.taobao.gulu.tengine.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONArray;
import org.json.*;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.junit.Assert;
import org.junit.Test;

import com.taobao.common.tfs.namemeta.FileMetaInfo;
import com.taobao.common.tfs.unique.UniqueStore;
import com.taobao.common.tfs.unique.UniqueValue;
import com.taobao.gulu.database.TFS;
import com.taobao.gulu.database.Tair;
import com.taobao.gulu.tengine.BaseCase;
import com.taobao.gulu.tools.VerifyTool;

/* web root server ceshi
 * 
 * 现在可以在配置文件中配置net_mask（网络掩码），
 * wrs会根据请求的ip地址与此网络掩码进行计算得到网络地址（网段），
 * 然后会根据后端健康检查的结果将状态正常，并且与该请求在同一个网段的nginx地址返回。
 * 注意，若符合要求的nginx地址太少（少于配置文件中配置的某个值least_rtn_addr_count，
 * 会返回所有状态正常的nginx地址）
 * 
 */
public class TFS_Restful_WebRootServer_Test extends BaseCase {
	/*
	 * 
	 * 
	 * 10.232.4.6和10.232.4.35
	 */

	/*
	 * create file 返回201
	 */
	@Test
	public void test_TFS_RestfulUserDefinedCreateFileReturn201Test() {

		VerifyTool tools = new VerifyTool();

		/* set base url */
		TFS tfsServer = tfs_NginxA01;
		String url = "http://10.232.4.35:3800";
		String urlWebRoot = url +  "/tfs.list";
		try {
			// set expect response message
			Map<String, String> expectMessage200 = new HashMap<String, String>();
			expectMessage200.put("body", "10.232.4.35:8032\n10.232.4.6:8032");
			expectMessage200.put("status", "200");
	
			System.out.println("the urlWebRoot   is : " + urlWebRoot);
			
			/* do post file action */
			System.out.println("creat dir  begin");
			tools.verifyResponse(setGetMethod(urlWebRoot), expectMessage200);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			/* do delete tfs file */
			if (put2TfsKeys.size() > 0) {
				for (String key : put2TfsKeys) {
					System.out.println("tfsFileName for delete is " + key);
					tfsServer.delete(key, null);
				}
			}
			put2TfsKeys.clear();
		}
	}

}
