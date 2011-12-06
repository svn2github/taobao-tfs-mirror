<?php
function test_pwriteFile_01($tfsobj, $app_id, $uid)
{
   echo "\n---------case 1: write file right fd---------------------\n";
   $file_path="/testpwritefile1";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "fopen filePath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
   echo "fopen file fd return: $fd\n";

   $data='taobaofilesystemphpclient1111';
   $data_len = strlen($data);
   echo "datalength:$data_len , data:$data\n";
   $ret=$tfsobj->pwrite($fd, $data, $data_len, 0);
   echo "\nwrite data return: $ret\n";

   echo "rm filePath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "------------------------case 1 end----------------------------\n";
}

function test_pwriteFile_02($tfsobj, $app_id, $uid)
{
   echo "\n-----case 2: pwrite file with different write data length-----\n";
   $file_path="/testpwritefile2";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "fopen filPath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
   echo "fopen file fd return : $fd\n";

   $data='taobaofilesystemphpclient2222';
   $data_len = strlen($data);
   echo "datalength:$data_len , data:$data\n";

   echo "\nwrite data length:-1\n";
   $ret=$tfsobj->pwrite($fd, $data, -1, 0);
   echo "\nwrite data return: $ret\n";

   echo "\nwrite data length:0\n";
   $ret=$tfsobj->pwrite($fd, $data, 0, 0);
   echo "\nwrite data return: $ret\n";

   echo "\nwrite data length:16\n";
   $ret=$tfsobj->pwrite($fd, $data, 16, 0);
   echo "\nwrite data return: $ret\n";

   echo "\nwrite data length:50\n";
   $ret=$tfsobj->pwrite($fd, $data, 50, 0);
   echo "\nwrite data return: $ret\n";

   echo "rm filePath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "------------------------case 2 end----------------------------\n";

}

function test_pwriteFile_03($tfsobj, $app_id, $uid)
{

   echo "\n---------case 3: pwrite file with fd not fopen----------------\n";
   $data='taobaofilesystemphpclient3333';
   $data_len = strlen($data);
   echo "datalength:$data_len , data:$data\n";

   echo "\npwrite fd:-1";
   $ret=$tfsobj->pwrite(-1, $data, $data_len, 0);
   echo "\nwrite data return: $ret\n";

   echo "\npwrite fd:0";
   $ret=$tfsobj->pwrite(-1, $data, $data_len, 0);
   echo "\nwrite data return: $ret\n";

   echo "\npwrite fd:1";
   $ret=$tfsobj->pwrite(-1, $data, $data_len, 0);
   echo "\nwrite data return: $ret\n";
   echo "------------------------case 3 end----------------------------\n";
}

function test_pwriteFile_04($tfsobj, $app_id, $uid)
{
   echo "\n-------case 4: pwrite file with different mode----------------\n";
   $file_path="/testpwritefile1";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
   echo "fopen file fd return: $fd\n";

   $data='taobaofilesystemphpclient1111';
   $data_len = strlen($data);
   echo "datalength:$data_len , data:$data\n";

   echo "\npwrite mode:-1\n";
   $ret=$tfsobj->pwrite($fd, $data, $data_len, -1);
   echo "\nwrite data return: $ret\n";

   echo "\npwrite mode:2\n";
   $ret=$tfsobj->pwrite($fd, $data, $data_len, 2);
   echo "\nwrite data return: $ret\n";
    
   echo "rm filePath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "------------------------case 4 end----------------------------\n";
}

function test_pwriteFile_05($tfsobj, $app_id, $uid)
{
   $file_path="/testpwritefile2";
   $ret = $tfsobj->rm_file($uid, $file_path);
}

function test_pwriteFile_06($tfsobj, $app_id, $uid)
{

}
/**
 *@param rc_ip: rc server_ip
 *@param app_key: application key
 *@param local ip : local ip
*/
//    $rcServerIp="10.232.36.210:5632";
//    $appKey="tappkey00002";
//    $localIp="10.13.88.118";
//    $tfsobj = new tfs_client($rcServerIp, $appKey, $localIp);
   $tfsobj = new tfs_client($argv[1], $argv[2], $argv[3]);
   $app_id = $tfsobj->get_app_id();
   $uid = 15;
//   echo "\n-----rcServer:$rcServerIp , appkey:$appKey , app_id: $app_id-----\n";
   echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";

   switch($argv[4])
   {
      case '1':
        test_pwriteFile_01($tfsobj, $app_id, $uid);
        break;
      case '2':
        test_pwriteFile_02($tfsobj, $app_id, $uid);
        break;
      case '3':
        test_pwriteFile_03($tfsobj, $app_id, $uid);
        break;
      case '4':
        test_pwriteFile_04($tfsobj, $app_id, $uid);
        break;
      case '5':
        test_pwriteFile_05($tfsobj, $app_id, $uid);
        break;
      default:
        echo "param invalid!";
        break;
    }

?>
