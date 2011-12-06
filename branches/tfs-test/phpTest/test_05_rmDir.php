<?php
function test_rmDir_01($tfsobj, $uid)
{
   echo "\n--------------------case 1: rm dir right--------------------------\n";
   $dir_path="/testrmdir1";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";
   
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "-------------------------case 1 end-------------------------------\n";
}

function test_rmDir_02($tfsobj, $uid)
{
   echo "\n--------------------case 2: rm dir twice--------------------------\n";
   $dir_path="/testrmdir2";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";

   echo "second rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "------------------------case 2 end--------------------------------\n";
}

function test_rmDir_03($tfsobj, $uid)
{
   echo "\n----------------case 3: rm dir with null dirPath------------------\n";
   echo "rm null dirPath\n";
   $ret = $tfsobj->rm_dir($uid, NULL);
   echo "rm dir return: $ret\n";
   echo "------------------------case 3 end--------------------------------\n";
}

function test_rmDir_04($tfsobj, $uid)
{
   echo "\n--------------case 4: rm dir with empty dirPath-------------------\n";
   echo "rm empty dirPath\n";
   $ret = $tfsobj->rm_dir($uid, "");
   echo "rm dir return: $ret\n";
   echo "------------------------case 4 end--------------------------------\n";
}

function test_rmDir_05($tfsobj, $uid)
{
   echo "\n------------case 5: rm dir with wrong dirPath 1-------------------\n";
   $dir_path="/";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "------------------------case 5 end--------------------------------\n";
}

function test_rmDir_06($tfsobj, $uid)
{
   echo "\n------------case 6: rm dir with wrong dirPath 2-------------------\n";
   $dir_path="testrmdir6";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "------------------------case 6 end--------------------------------\n";
}

function test_rmDir_07($tfsobj, $uid)
{
   echo "\n------------case 7: rm dir with wrong dirPath 3-------------------\n";
   $dir_path="/testrmdir7";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   $dir_path="////testrmdir7////";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "------------------------case 7 end--------------------------------\n";
}

function test_rmDir_08($tfsobj, $uid)
{
   echo "\n------------case 8: rm dir with leap dirPath 1--------------------\n";
   $dir_path="/testrmdir8";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";
   $dir_path="/testrmdir8/testdir8";

   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   $file_path="/testrmdir8/testfile8";
   echo "create filePath:$file_path\n";
   $ret = $tfsobj->create_file($uid, $file_path);
   echo "create dir return: $ret\n";  
  
   $dir_path="/testrmdir8/testdir8";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";

   $file_path="/testrmdir8/testfile8";
   echo "rm filePath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";

   $dir_path="/testrmdir8";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "------------------------case 8 end--------------------------------\n"; 
}

function test_rmDir_09($tfsobj, $uid)
{
   echo "\n------------case 9: rm dir with leap dirPath 2--------------------\n";
   $dir_path="/testrmdir9";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";

   $dir_path="/testrmdir9/testdir9";
   echo "create dirPath:$dir_path\n";
   $ret = $tfsobj->create_dir($uid, $dir_path);
   echo "create dir return: $ret\n";
 
   $dir_path="/testrmdir9";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";

   $dir_path="/testrmdir9/testdir9";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";

   $dir_path="/testrmdir9";
   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "------------------------case 9 end--------------------------------\n";
}

function test_rmDir_10($tfsobj, $uid)
{
   $dir_path="/testrmdir9/testdir9";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   $dir_path="/testrmdir9";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
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
   $uid = 16;
//   echo "\n<---------------rcServer:$rcServerIp , appkey:$appKey , app_id: $app_id--------->\n";
   echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";

   switch($argv[4])
   {  
      case '1':
        test_rmDir_01($tfsobj, $uid);
        break;
      case '2':
        test_rmDir_02($tfsobj, $uid);
        break;
      case '3':
        test_rmDir_03($tfsobj, $uid);
        break;
      case '4':
        test_rmDir_04($tfsobj, $uid);
        break;
      case '5':
        test_rmDir_05($tfsobj, $uid);
        break;
      case '6':
        test_rmDir_06($tfsobj, $uid);
        break;
      case '7':
        test_rmDir_07($tfsobj, $uid);
        break;
      case '8':
        test_rmDir_08($tfsobj, $uid);
        break;
      case '9':
        test_rmDir_09($tfsobj, $uid);
        break;
      case '10':
        test_rmDir_10($tfsobj, $uid);
        break;
      default:
        echo "param invalid!";
        break;
    }

?>
