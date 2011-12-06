<?php
function test_createdir_01($tfsobj, $uid)
{
   echo "\n-----------------case 1: create dir right---------------------------\n";
   $dir_path="/testcreatedir1";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "---------------------------case 1 end--------------------------------\n";
}

function test_createdir_02($tfsobj, $uid)
{
   echo "\n--------------case 2: create dir with empty dirPath-------------------\n";
      echo "create empty dirPath\n";
   $ret = $tfsobj->create_dir($uid, "");
   echo "create dir return: $ret\n";
   echo "---------------------------case 2 end---------------------------------\n";
}

function test_createdir_03($tfsobj, $uid)
{
   echo "\n-------------case 3: create dir with null dirPath---------------------\n";
   echo "create null dirPath\n";
   $ret = $tfsobj->create_dir($uid, NULL);
   echo "create dir return: $ret\n";
   echo "----------------------------case 3 end--------------------------------\n";   
}

function test_createdir_04($tfsobj, $uid)
{
   echo "\n-------------case 4: create dir with wrong dirPath 1--------------------\n";
   $dir_path="/";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";
   echo "-------------------------------case 4 end-------------------------------\n";
}

function test_createdir_05($tfsobj, $uid)
{
   echo "\n-------------case 5: create dir with wrong dirPath 2--------------------\n";
   $dir_path="testcreatedir5";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";
   echo "-----------------------------case 5 end-----------------------------\n";
}

function test_createdir_06($tfsobj, $uid)
{
   echo "\n-----------case 6: create dir with wrong dirPath 3--------------------\n";
   $dir_path="////testcreatedir6";
   echo "dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   $dir_path="/testcreatedir6";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "----------------------------case 6 end-------------------------------\n";
}

function test_createdir_07($tfsobj, $uid)
{
   echo "\n----------case 7: create dir with wrong dirPath 4--------------------\n";
   $dir_path="////testcreatedir7///";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   $dir_path="/testcreatedir7";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "----------------------------case 7 end-------------------------------\n";
}

function test_createdir_08($tfsobj, $uid)
{
   echo "\n---------case 8: create dir twice--------------------------\n";
   $dir_path="/testcreatedir8";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

      echo "second create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "---------------------case 8 end-----------------------------\n";
}

function test_createdir_09($tfsobj, $uid)
{
   echo "\n--------------case 9: create dir with same fileName-------------\n";
   $file_path="/testcreatedrisame9";
   echo "create filePath:$file_path\n";
   $ret = $tfsobj->create_file($uid, $file_path);
   echo "create file return: $ret\n";

   $dir_path="/testcreatedrisame9";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   echo "rm filePath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";

   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "---------------------------case 9 end-----------------------\n";
}

function test_createdir_10($tfsobj, $uid)
{
   echo "\n--------case 10: create dir with leap dirPath 1-------------\n";
   $dir_path="/testleap10/test";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";
   echo "-----------------------case 10 end---------------------------\n";
}

function test_createdir_11($tfsobj, $uid)
{
   echo "\n-----------------case 11: create dir with leap dirPath 2---------------\n";
   $dir_path="/testleap11";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   $dir_path="/testleap11/test";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";

   $dir_path="/testleap11";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "---------------------------------case 11 end----------------------------\n";
}

function test_createdir_12($tfsobj, $uid)
{
   $dir_path="/testcreatedir8";
   $tfsobj->rm_dir($uid, $dir_path);
}

/**
 *@param rc_ip: rc server_ip
 *@param app_key: application key
 *@param local ip : local ip
*/
//   $rcServerIp="10.232.36.210:5632";
//   $appKey="tappkey00002";
//   $localIp="10.13.88.118";
//   $tfsobj = new tfs_client($rcServerIp, $appKey, $localIp);
   $tfsobj = new tfs_client($argv[1], $argv[2], $argv[3]);
   $app_id = $tfsobj->get_app_id();
   $uid = 15;

//   echo "\n-----rcServer:$rcServerIp , appkey:$appKey , app_id: $app_id-----\n";
   echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";


   switch($argv[4])
   {
      case '1':
        test_createdir_01($tfsobj, $uid);
        break;
      case '2':
        test_createdir_02($tfsobj, $uid);
        break;
      case '3':
        test_createdir_03($tfsobj, $uid);
        break;
      case '4':
        test_createdir_04($tfsobj, $uid);
        break;
      case '5':
        test_createdir_05($tfsobj, $uid);
        break;
      case '6':
        test_createdir_06($tfsobj, $uid);
        break;
      case '7':
        test_createdir_07($tfsobj, $uid);
        break;
      case '8':
        test_createdir_08($tfsobj, $uid);
        break;
      case '9':
        test_createdir_09($tfsobj, $uid);
        break;
      case '10':
        test_createdir_10($tfsobj, $uid);
        break;
      case '11':
        test_createdir_11($tfsobj, $uid);
        break;
      case '12':
        test_createdir_12($tfsobj, $uid);
        break;

      default:
        echo "param invalid!";
        break;
    }
//   echo "<========================= create dir start==================================>";
?>
