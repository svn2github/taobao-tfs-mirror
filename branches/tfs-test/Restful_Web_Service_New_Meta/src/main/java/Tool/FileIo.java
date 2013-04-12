package Tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FileIo 
{
	
	public FileIo(String Path)
	{
		try 
		{
			_file = new File(Path);
			if(!_file.exists())
				_file.createNewFile();
			Fis = new FileWriter(_file);
			Fos = new BufferedReader(new FileReader(_file));
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void WriteLine(String line)
	{
		try 
		{
			Fis.write(line+"\n");
		} 
		catch (IOException e) 
		{
				// TODO Auto-generated catch block
				e.printStackTrace();
		}	
	}
	
	public String ReadLine()
	{
		String Ret = "";
		try 
		{
			Ret=Fos.readLine();
			
		} 
		catch (IOException e) 
		{
				// TODO Auto-generated catch block
				e.printStackTrace();
		}	
		return Ret;
	}
	
	public void CloseWirte()
	{
		try 
		{
			Fis.close();
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private  File _file;
	private FileWriter	Fis;
	private BufferedReader  Fos;
}
