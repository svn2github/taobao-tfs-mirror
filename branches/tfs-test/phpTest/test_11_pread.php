<?php
function test_preadFile_01($tfsobj, $app_id, $uid)
{
   echo "\n--------------------------case 1: pread file right ----------------------------------------\n";
   $file_path="/testpreadfile1";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";
   
   echo "fopen filPath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
   echo "fopen file fd return: $fd\n";

   $data='taobaofilesystemphpclient1111';
   $data_len = strlen($data);
   echo "datalength:$data_len , data:$data\n";
   $ret=$tfsobj->pwrite($fd, $data, $data_len, 0);
   echo "\nwrite data return: $ret\n";

   $ret=$tfsobj->pread($fd, $data_len, 0);
   echo "\nread data return: length : $ret[0], data: $ret[1]\n";

   echo "rm filPath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "--------------------------------case 1 end----------------------------------\n";
}

function test_preadFile_02($tfsobj, $app_id, $uid)
{
   echo "\n------------case 2: pread file with different datalength--------------------\n";
   $file_path="/testpreadfile2";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "fopen filPath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
   echo "fopen file fd return: $fd\n";

   $data='taobaofilesystemphpclient2222';
   $data_len = strlen($data);
   echo "datalength:$data_len , data:$data\n";
   $ret=$tfsobj->pwrite($fd, $data, $data_len, 0);
   echo "\nwrite data return: $ret\n";

   $data_len=-1;
   echo "\ndatalegth:$data_len\n";
   $ret=$tfsobj->pread($fd, $data_len, 0);
   echo "\nread data return: length : $ret[0], data: $ret[1]\n";

   $data_len=16;
   echo "\ndatalegth:$data_len\n";
   $ret=$tfsobj->pread($fd, $data_len, 0);
   echo "\nread data return: length : $ret[0], data: $ret[1]\n";


   $data_len=29;
   echo "\ndatalegth:$data_len\n";
   $ret=$tfsobj->pread($fd, $data_len, 0);
   echo "\nread data return: length : $ret[0], data: $ret[1]\n";

   $data_len=50;
   echo "\ndatalegth:$data_len\n";
   $ret=$tfsobj->pread($fd, $data_len, 0);
   echo "\nread data return: length : $ret[0], data: $ret[1]\n";

   echo "rm filPath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "\n--------------------------------case 2 end----------------------------------\n";
}

function test_preadFile_03($tfsobj, $app_id, $uid)
{
   echo "\n-----------------case 3: pread with fd not open file------------------------\n";
   $data_len=50;
   echo "\npread fd:-1\n";
   $ret=$tfsobj->pread(-1, $data_len, 0);
   echo "\nread data return: length : $ret[0], data: $ret[1]\n";

   echo "\npread fd:0\n";
   $ret=$tfsobj->pread(0, $data_len, 0);
   echo "\nread data return: length : $ret[0], data: $ret[1]\n";

   echo "\npread fd:2\n";
   $ret=$tfsobj->pread(2, $data_len, 0);
   echo "\n read data return: length : $ret[0], data: $ret[1]\n";
   echo "--------------------------------case 3 end----------------------------------\n";
}

function test_preadFile_04($tfsobj, $app_id, $uid)
{
   echo "\n------------------case 4: pread with different mode-------------------------\n";
   $file_path="/testpreadfile4";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "fopen filePath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
   echo "fopen file fd return: $fd\n";

   $data='taobaofilesystemphpclient44444';
   $data_len = strlen($data);
   echo "datalength:$data_len , data:$data\n";
   $ret=$tfsobj->pwrite($fd, $data, $data_len, 0);
   echo "\nwrite data return: $ret\n";

   echo "\npread mode:-1\n";
   $ret=$tfsobj->pread($fd, $data_len, -1);
   echo "\nread data return: length : $ret[0], data: $ret[1]\n";

   echo "\npread mode:2\n";
   $ret=$tfsobj->pread($fd, $data_len, 2);
   echo "\nread data return: length : $ret[0], data: $ret[1]\n";

   echo "rm filPath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "--------------------------------case 4 end----------------------------------\n";
}

function test_preadFile_05($tfsobj, $app_id, $uid)
{
   $file_path="/testpreadfile4";
   $ret = $tfsobj->rm_file($uid, $file_path);
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
   $uid = 11;
//   echo "\n-----rcServer:$rcServerIp , appkey:$appKey , app_id: $app_id-----\n";
   echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";

   switch($argv[4])
   {
      case '1':
        test_preadFile_01($tfsobj, $app_id, $uid);
        break;
      case '2':
        test_preadFile_02($tfsobj, $app_id, $uid);
        break;
      case '3':
        test_preadFile_03($tfsobj, $app_id, $uid);
        break;
      case '4':
        test_preadFile_04($tfsobj, $app_id, $uid);
        break;
      case '5':
        test_preadFile_05($tfsobj, $app_id, $uid);
        break;
      default:
        echo "param invalid!";
        break;
    }

?>
