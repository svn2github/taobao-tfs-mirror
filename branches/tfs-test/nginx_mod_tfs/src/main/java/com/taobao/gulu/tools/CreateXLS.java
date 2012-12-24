package com.taobao.gulu.tools;

import java.io.*;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.taobao.gulu.file.FileInfo;

import jxl.*; 
import jxl.write.*; 

public class CreateXLS {
	protected static final ApplicationContext BEANFACTORY = new ClassPathXmlApplicationContext("fileInfoForXLS.xml");
	protected static final FileInfo fileInfo = (FileInfo) BEANFACTORY.getBean("fileInfo");
	
	public static void Insert_data(WritableSheet sheet1, int lie, int inc, String file) throws Exception{
		
		///*Nginx_On_30K2_Perf_50_k
		//FileReader fr=new FileReader("D:\\workspace\\excel_test.java\\Nginx_On_30K2_Perf_50_k.log");  
		FileReader fr = new FileReader(file);    
		BufferedReader br = new BufferedReader(fr);      
		String line = "";       
		String[] arrs = null;   
		int count = 1;
		while ((line = br.readLine())!= null) {           
			arrs=line.split(":");
			if(arrs.length >= 2){  	//去掉没有 ：的行
				
				if(arrs[0].equals("Requests per second"))
				{
					System.out.println(arrs[0] + " : " + arrs[1] ); 
					arrs = arrs[1].split("\\[");
					if(arrs.length >= 2)	//去掉没有 [ 的行
					{
						System.out.println(arrs[0]); 
						double num = Double.parseDouble(arrs[0]);
						jxl.write.Number number = new jxl.write.Number(lie, 1+inc, num); 
						sheet1.addCell(number); 
					}
				}
				
				if(arrs[0].equals("Time per request"))
				{
					
					System.out.println(arrs[0] + " : " + arrs[1] ); 
					arrs = arrs[1].split("\\[");
					if(arrs.length >= 2)
					{
						System.out.println(arrs[0]); 
						double num = Double.parseDouble(arrs[0]);
						if(count == 1)
						{
							jxl.write.Number number1 = new jxl.write.Number(lie, 8+inc, num); 
							count++;
							sheet1.addCell(number1); 
						}
						else
						{
							jxl.write.Number number2 = new jxl.write.Number(lie, 15+inc, num); 
							count = 1;
							sheet1.addCell(number2); 
						}
					}
					
				}
				if(arrs[0].equals("Transfer rate"))
				{
					System.out.println(arrs[0] + " : " + arrs[1] ); 
					arrs = arrs[1].split("\\[");
					if(arrs.length >= 2)
					{
						System.out.println(arrs[0]); 
						double num = Double.parseDouble(arrs[0]);
						jxl.write.Number number = new jxl.write.Number(lie, 22+inc, num); 
						sheet1.addCell(number); 
					}
				}
			}
			
		}     
		br.close();     
		fr.close();   

	}
	
	public void test(){
		System.out.println(fileInfo.getFile2());
	}
	
	
//	public static void main(String args[]) 
//	{ 
//		try 
//		{ 
//			
//			//打开文件 
//			WritableWorkbook book = Workbook.createWorkbook(new File("测试_perf.xls")); 
//		
//			//生成名为“value”的工作表，参数0表示这是第一页 
//			WritableSheet sheet1 = book.createSheet("value",0); 
//			//生成名为“average_chart”的工作表，参数1表示这是第二页 
//			WritableSheet sheet2=book.createSheet("average_chart",1); 
//		
//			//在Label对象的构造子中指名单元格位置是从(0,0) 开始，表示第一列第一行
//			//以及单元格内容
//			Label RPS50=new Label(0,1,"Requests per second: 50");
//			Label RPS350=new Label(0,2,"350"); 
//			Label RPS650=new Label(0,3,"650"); 
//			Label RPS950=new Label(0,4,"950"); 
//		
//			
//			Label TPRC50=new Label(0,8,"Time per request: 50");
//			Label TPRC350=new Label(0,9,"350"); 
//			Label TPRC650=new Label(0,10,"650"); 
//			Label TPRC950=new Label(0,11,"950"); 
//		
//			
//			Label TPRS50=new Label(0,15,"Time per request: 50");
//			Label TPRS350=new Label(0,16,"350"); 
//			Label TPRS650=new Label(0,17,"650"); 
//			Label TPRS950=new Label(0,18,"950"); 
//		
//			
//			Label TR50=new Label(0,22,"Transfer rate: 50");
//			Label TR350=new Label(0,23,"350"); 
//			Label TR650=new Label(0,24,"650"); 
//			Label TR950=new Label(0,25,"950"); 
//		
//			
//			//将定义好的单元格添加到工作表中 
//			sheet1.addCell(RPS50);
//			sheet1.addCell(RPS350); 
//			sheet1.addCell(RPS650); 
//			sheet1.addCell(RPS950); 
//		
//			sheet1.addCell(TPRC50);
//			sheet1.addCell(TPRC350); 
//			sheet1.addCell(TPRC650); 
//			sheet1.addCell(TPRC950); 
//		
//			
//			sheet1.addCell(TPRS50);
//			sheet1.addCell(TPRS350); 
//			sheet1.addCell(TPRS650); 
//			sheet1.addCell(TPRS950); 
//		
//			
//			sheet1.addCell(TR50);
//			sheet1.addCell(TR350); 
//			sheet1.addCell(TR650); 
//			sheet1.addCell(TR950); 
//		
//			
//		/////SET X 坐标值
//			jxl.write.Number X50 = new jxl.write.Number(0,30,50);
//			jxl.write.Number X350 = new jxl.write.Number(0,31,350);
//			jxl.write.Number X650 = new jxl.write.Number(0,32,650);
//			jxl.write.Number X950 = new jxl.write.Number(0,33,950);
//		
//			sheet1.addCell(X50);
//			sheet1.addCell(X350); 
//			sheet1.addCell(X650); 
//			sheet1.addCell(X950); 
//			
//		/////SET NAME
//			Label Nginx_On_30K2_Perf=new Label(1,0,"Nginx_On_30K2_Perf");
//			sheet1.addCell(Nginx_On_30K2_Perf);
//			
//			Label Nginx_On_30K1_Perf=new Label(2,0,"Nginx_On_30K1_Perf");
//			sheet1.addCell(Nginx_On_30K1_Perf);
//			
//		//////////////////////////////////////////////////////////////////////// 
//			///*Nginx_On_30K2_Perf_50_k
//			Insert_data(sheet1,1,0,"D:\\workspace\\excel_test.java\\Nginx_On_30K2_Perf_50_k.log");
//			///*Nginx_On_30K2_Perf_350_k
//			Insert_data(sheet1,1,1,"D:\\workspace\\excel_test.java\\Nginx_On_30K2_Perf_350_k.log");
//			///*Nginx_On_30K2_Perf_650_k
//			Insert_data(sheet1,1,2,"D:\\workspace\\excel_test.java\\Nginx_On_30K2_Perf_650_k.log");
//			///*Nginx_On_30K2_Perf_950_k
//			Insert_data(sheet1,1,3,"D:\\workspace\\excel_test.java\\Nginx_On_30K2_Perf_950_k.log");
//
//			
//			///*Nginx_On_30K1_Perf_50_k
//			Insert_data(sheet1,2,0,"D:\\workspace\\excel_test.java\\Nginx_On_30K1_Perf_50_k.log");
//			///*Nginx_On_30K1_Perf_350_k
//			Insert_data(sheet1,2,1,"D:\\workspace\\excel_test.java\\Nginx_On_30K1_Perf_350_k.log");
//			///*Nginx_On_30K1_Perf_650_k
//			Insert_data(sheet1,2,2,"D:\\workspace\\excel_test.java\\Nginx_On_30K1_Perf_650_k.log");
//			///*Nginx_On_30K1_Perf_950_k
//			Insert_data(sheet1,2,3,"D:\\workspace\\excel_test.java\\Nginx_On_30K1_Perf_950_k.log");
//
//		/////////////////////////////////////////////////////////
//			/*生成一个保存数字的单元格 
//			必须使用Number的完整包路径，否则有语法歧义 
//			单元格位置是第二列，第二行，值为789.123*/ 
//		//	jxl.write.Number number = new jxl.write.Number(1,2,789.123); 
//		//	sheet1.addCell(number); 
//		
//			//写入数据并关闭文件 
//			book.write(); 
//			book.close(); 
//		}catch(Exception e) 
//		{ 
//			System.out.println(e); 
//		} 
//	} 
}
