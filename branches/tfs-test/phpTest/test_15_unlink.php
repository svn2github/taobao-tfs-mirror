<?php
function test_unlinkFile_01($tfsobj)
{
   echo "\n------------------case 1: test unlink right-----------------\n";
   $data="case 1:test unlink right";
   $data_len=strlen($data);
   echo "\ndatalength:$data_len, data:$data\n";

   echo "\nopen file(null) suffix:.txt\n";
   $fd=$tfsobj->open(NULL,".txt",1);
   echo "\nopen return fd:$fd\n";

   $ret=$tfsobj->write($fd, $data, $data_len);
   echo "\nwrite return:$ret\n";
   $filename=$tfsobj->close($fd);
   echo "\nfilelength:$filename[0], filename:$filename[1]\n";

   echo "\nopen file($filename[1]) suffix:.txt\n";
   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";

   $uRet=$tfsobj->unlink($filename[1], ".txt", 0);
   echo "\nunlink return:$uRet\n";

   $st=$tfsobj->stat($fd);
   echo "\nunlink stat: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
   $tfsobj->close($fd);
   echo "\n--------------------test unlink right end-------------------\n";
}

function test_unlinkFile_02($tfsobj)
{
   echo "\n---------case 2: test unlink with empty filename------------\n";
   echo "\nunlink empty fileName\n";
   $uRet=$tfsobj->unlink("", ".txt", 0);
   echo "\nunlink return:$uRet\n";
   echo "\n-----------test unlink with empty filename end--------------\n";
}

function test_unlinkFile_03($tfsobj)
{
   echo "\n---------case 3: test unlink with null filename-------------\n";
   echo "\nunlink null fileName\n";
   $uRet=$tfsobj->unlink(NULL, ".txt", 0);
   echo "\nunlink return:$uRet\n";
   echo "\n-----------test unlink with null filename end---------------\n";
}

function test_unlinkFile_04($tfsobj)
{
   echo "\n---------case 4: test unlink with filename(not open)-------------\n";
   $fileName="TFSaabbcc";
   $uRet=$tfsobj->unlink($fileName, ".txt", 0);
   echo "\nunlink return:$uRet\n";
   echo "\n-----------test unlink with filename(not open) end---------------\n";
}

function test_unlinkFile_05($tfsobj)
{
   echo "\n-----------case 5: test unlink with empty suffix------------\n";
   $data="case5:aaabbbbccccdddd112233";
   $data_len=strlen($data);
   echo "\ndatalength:$data_len, data:$data\n";

   echo "\nopen file(null) suffix:.txt\n";
   $fd=$tfsobj->open(NULL,".txt",1);
   echo "\nopen return fd:$fd\n";
   $ret=$tfsobj->write($fd, $data, $data_len);
   echo "\nwrite return:$ret\n";

   $filename=$tfsobj->close($fd);
   echo "\nfilelength:$filename[0], filename:$filename[1]\n";

   echo "\nopen file($filename[1]) suffix:.txt\n";
   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";
   
   echo "\nunlink empty suffix\n";
   $uRet=$tfsobj->unlink($filename[1], "", 0);
   echo "\nunlink return:$uRet\n";
   $st=$tfsobj->stat($fd);
   echo "\nunlink stat: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
   $tfsobj->close($fd);
   echo "\n---------------test unlink with empty suffix end------------\n";
}

function test_unlinkFile_06($tfsobj)
{
   echo "\n-----------case 6: test unlink with null suffix-------------\n";
   $data="case6:aabbccddeeffgg00112233hhiijj";
   $data_len=strlen($data);
   echo "\ndatalength:$data_len, data:$data\n";

   echo "\nopen file(null) suffix:.txt\n";
   $fd=$tfsobj->open(null,".txt",1);
   echo "\nopen return fd:$fd\n";
   $ret=$tfsobj->write($fd, $data, $data_len);
   echo "\nwrite return:$ret\n";

   $filename=$tfsobj->close($fd);
   echo "\nfilelength:$filename[0], filename:$filename[1]\n";

   echo "\nopen file($filename[1]) suffix:.txt\n";
   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";

   echo "\nunlink null suffix\n";
   $uRet=$tfsobj->unlink($filename[1], NULL, 0);
   echo "\nunlink return:$uRet\n";

   $st=$tfsobj->stat($fd);
   echo "\nunlink stat: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
   $tfsobj->close($fd);
   echo "\n---------------test unlink with null suffix end-------------\n";
}

function test_unlinkFile_07($tfsobj)
{
   echo "\n---------case 7: test unlink with different suffix----------\n";
   $data="case7:aabbccddeeffgg00112233hhiijj";
   $data_len=strlen($data);
   echo "\ndatalength:$data_len, data:$data\n";

   echo "\nopen file(null) suffix:.txt\n";
   $fd=$tfsobj->open(null,".txt",1);
   echo "\nopen return fd:$fd\n";
   $ret=$tfsobj->write($fd, $data, $data_len);
   echo "\nwrite return:$ret\n";

   $filename=$tfsobj->close($fd);
   echo "\nfilelength:$filename[0], filename:$filename[1]\n";

   echo "\nopen file($filename[1]) suffix:.txt\n";
   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";

   echo "\nunlink file($filename[1]) suffix:.doc\n";
   $uRet=$tfsobj->unlink($filename[1], ".doc", 0);
   echo "\nunlink return:$uRet\n";

   $st=$tfsobj->stat($fd);
   echo "\nunlink stat: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
   $tfsobj->close($fd);
   echo "\n------------test unlink with different suffix end-----------\n";
}

function test_unlinkFile_08($tfsobj)
{
   echo "\n-----------case 8: test unlink with wrong mode--------------\n";
   $data="case8:aabbccddeeffgg00112233hhiijj";
   $data_len=strlen($data);
   echo "\ndatalength:$data_len, data:$data\n";

   echo "\nopen file(null) suffix:.txt\n";
   $fd=$tfsobj->open(null,".txt",1);
   echo "\nopen return fd:$fd\n";

   $ret=$tfsobj->write($fd, $data, $data_len);
   echo "\nwrite return:$ret\n";

   $filename=$tfsobj->close($fd);
   echo "\nfilelength:$filename[0], filename:$filename[1]\n";

   echo "\nopen file($filename[1]) suffix:.txt\n";
   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";

   echo "\nunlink mode:-1\n";
   $uRet=$tfsobj->unlink($filename[1], ".txt", -1);
   echo "\nunlink return:$uRet\n";
   $st=$tfsobj->stat($fd);
   echo "\nunlink stat: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
   $tfsobj->close($fd);
   echo "\n--------------test unlink with wrong mod end---------------\n";
}

function test_unlinkFile_09($tfsobj)
{
   echo "\n---------case 9: test unlink with different mode-----------\n";
   $data="case9:aabbccddeeffgg00112233hhiijj";
   $data_len=strlen($data);
   echo "\ndatalength:$data_len, data:$data\n";
   
   echo "\nopen file(null) suffix:.txt\n";
   $fd=$tfsobj->open(null,".txt",1);
   echo "\nopen return fd:$fd\n";

   $ret=$tfsobj->write($fd, $data, $data_len);
   echo "\nwrite return:$ret\n";

   $filename=$tfsobj->close($fd);
   echo "\nfilelength:$filename[0], filename:$filename[1]\n";

   echo "\nopen file($filename[1]) suffix:.txt\n";
   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";

   echo "\nunlink mode:1\n";
   $uRet=$tfsobj->unlink($filename[1], ".txt", 1);
   echo "\nunlink return:$uRet\n";

   $st=$tfsobj->stat($fd);
   echo "\nunlink stat: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
   $tfsobj->close($fd);
   echo "\n------------test unlink with different mode end------------\n";
}

function test_unlinkFile_10($tfsobj)
{

}

/**
 *@param rc_ip: rc server_ip
 *@param app_key: application key
 *@param local ip : local ip
*/
    $tfsobj = new tfs_client($argv[1], $argv[2], $argv[3]);
    $app_id = $tfsobj->get_app_id();

    echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";

    switch($argv[4])
    {
      case '1':
        test_unlinkFile_01($tfsobj);
        break;
      case '2':
        test_unlinkFile_02($tfsobj);
        break;
      case '3':
        test_unlinkFile_03($tfsobj);
        break;
      case '4':
        test_unlinkFile_04($tfsobj);
        break;
      case '5':
        test_unlinkFile_05($tfsobj);
        break;
      case '6':
        test_unlinkFile_06($tfsobj);
        break;
      case '7':
        test_unlinkFile_07($tfsobj);
        break;
      case '8':
        test_unlinkFile_08($tfsobj);
        break;
      case '9':
        test_unlinkFile_09($tfsobj);
        break;
      default:
        echo "param invalid!";
        break;
     }
   
?>
