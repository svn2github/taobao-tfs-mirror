<?php
function test_rmFile_01($tfsobj, $uid)
{
   echo "\n-------------case 1: rm file with right filePath-----------------------\n";
   $file_path="/testrmfile1";
   echo "create filePath: $file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "-----------------------case 1 end---------------------------------------\n";
}

function test_rmFile_02($tfsobj, $uid)
{
   echo "\n----------------case 2: rm file twice-----------------------------------\n";
   $file_path="/testrmfile2";
   echo "create filePath: $file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";

   echo "second rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "----------------------case 2 end----------------------------------------\n";
}

function test_rmFile_03($tfsobj, $uid)
{
   echo "\n-----------case 3: rm file with empty filePath--------------------------\n";
   echo "rm empty filePath\n";
   $ret = $tfsobj->rm_file($uid, "");
   echo "rm file return: $ret\n";
   echo "----------------------case 3 end----------------------------------------\n";
}

function test_rmFile_04($tfsobj, $uid)
{
   echo "\n-------------case 4: rm file with null filePath-------------------------\n";
   echo "rm null filePath\n";
   $ret = $tfsobj->rm_file($uid, NULL);
   echo "rm file return: $ret\n";
   echo "-----------------------case 4 end---------------------------------------\n";
}

function test_rmFile_05($tfsobj, $uid)
{
   echo "\n-------------case 5: rm file with wrong filePath 1---------------------\n";
   $file_path="/";
   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "-----------------------case 5 end---------------------------------------\n";
}

function test_rmFile_06($tfsobj, $uid)
{
   echo "\n-------------case 6: rm file with wrong filePath 1---------------------\n";
   $file_path="testrmfile6";
   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "-----------------------case 6 end---------------------------------------\n";
}

function test_rmFile_07($tfsobj, $uid)
{
   echo "\n-------------case 7: rm file with wrong filePath 1---------------------\n";
   $file_path="/testrmfile7";
   echo "create filePath: $file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   $file_path="/////testrmfile7/////";
   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "-----------------------case 7 end---------------------------------------\n";
}

function test_rmFile_08($tfsobj, $uid)
{
   echo "\n-----------------case 8: rm file with leap file--------------------------\n";
   $dir_path="/testrmfile8dir";
   echo "create dirPath: $dir_path\n";
   $ret=$tfsobj->create_dir($uid, $dir_path);
   echo "create dir return:$ret\n";

   $file_path="/testrmfile8dir/testrmfile8";
   echo "create filePath: $file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";

   echo "rm dirPath: $dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "---------------------------case 8 end-----------------------------------\n";
}

function test_rmFile_09($tfsobj, $uid)
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
    $uid = 17;
//   echo "\n<---------------rcServer:$rcServerIp , appkey:$appKey , app_id: $app_id--------->\n";
   echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";

   switch($argv[4])
   { 
      case '1':
        test_rmFile_01($tfsobj, $uid);
        break;
      case '2':
        test_rmFile_02($tfsobj, $uid);
        break;
      case '3':
        test_rmFile_03($tfsobj, $uid);
        break;
      case '4':
        test_rmFile_04($tfsobj, $uid);
        break;
      case '5':
        test_rmFile_05($tfsobj, $uid);
        break;
      case '6':
        test_rmFile_06($tfsobj, $uid);
        break;
      case '7':
        test_rmFile_07($tfsobj, $uid);
        break;
      case '8':
        test_rmFile_08($tfsobj, $uid);
        break;
      default:
        echo "param invalid!";
        break;
    }

?>
