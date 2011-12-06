<?php
function test_fopenFile_01($tfsobj, $app_id, $uid)
{
   echo "\n---------case 1: open file right filePath---------------------\n";
   $file_path="/testopenfile1";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";
   
   echo "fopen filePath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
   echo "fd: $fd\n";
    
   echo "rm filePath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "------------------------case 1 end----------------------------\n";
}

function test_fopenFile_02($tfsobj, $app_id, $uid)
{
   echo "\n---------case 2: open file with empty filePath --------------\n";
   echo  "fopen empty filePath\n";
   $fd=$tfsobj->fopen($app_id, $uid, "", 4); 
   echo "fd: $fd\n";
   echo "------------------------case 2 end----------------------------\n";
}

function test_fopenFile_03($tfsobj, $app_id, $uid)
{
   echo "\n---------case 3: open file with null filePath-----------------\n";
   echo  "fopen null filePath\n";
   $fd=$tfsobj->fopen($app_id, $uid, NULL, 4);
   echo "fd: $fd\n";
   echo "------------------------case 3 end----------------------------\n";
}

function test_fopenFile_04($tfsobj, $app_id, $uid)
{
   echo "\n-----------case 4: open file with wrong filePath 1------------\n";
   $file_path="/";
   echo "fopen filPath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
   echo "fd: $fd\n";
   echo "-------------------------case 4 end---------------------------\n";
}

function test_fopenFile_05($tfsobj, $app_id, $uid)
{
   echo "\n-----------case 5: open file with wrong filePath 2------------\n";
   $file_path="testopenfile5";
   echo "fopen filPath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 5);
   echo "fd: $fd\n";
   echo "-------------------------case 5 end---------------------------\n";
}

function test_fopenFile_06($tfsobj, $app_id, $uid)
{
   echo "\n-----------case 6: open file with wrong filePath 3------------\n";
  $file_path="/testopenfile6";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";


   $file_path="////testopenfile6///";
   echo "fopen filPath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 6);
   echo "fd: $fd\n";

   $file_path="/testopenfile6";
   echo "rm filPath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "-------------------------case 6 end---------------------------\n";
}

function test_fopenFile_07($tfsobj, $app_id, $uid)
{
   echo "\n-------------case 7: open file with wrong Mode 1--------------\n";
   $file_path="/testopenfile7";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";
   
   echo "fopen mode:-1 , filePath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, -1);
   echo "fd: $fd\n";
   
   echo "rm  filePath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "-------------------------case 7 end---------------------------\n";
}

function test_fopenFile_08($tfsobj, $app_id, $uid)
{
   echo "\n-------------case 8: open file with wrong Mode 2--------------\n";
   $file_path="/testopenfile8";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "fopen mode:0 ,  filePath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 0);
   echo "fd: $fd\n";
   
   echo "rm  filePath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "-------------------------case 8 end---------------------------\n";
}

function test_fopenFile_09($tfsobj, $app_id, $uid)
{
   echo "\n-------------case 9: open file with wrong Mode 3--------------\n";
   $file_path="/testopenfile9";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "fopen mode:1 ,  filePath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 0);
   echo "fd: $fd\n";

   echo "rm  filePath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "-------------------------case 9 end---------------------------\n";
}

function test_fopenFile_10($tfsobj, $app_id, $uid)
{
    $file_path="/testopenfile6";
    $ret = $tfsobj->rm_file($uid, $file_path);
    $file_path="/testopenfile8";
    $ret = $tfsobj->rm_file($uid, $file_path);
    $file_path="/testopenfile9";
    $ret = $tfsobj->rm_file($uid, $file_path);
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
//   echo "\n<---------------rcServer:$rcServerIp , appkey:$appKey , app_id: $app_id--------->\n";
   echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";

   switch($argv[4])
   {
      case '1':
        test_fopenFile_01($tfsobj, $app_id, $uid);
        break;
      case '2':
        test_fopenFile_02($tfsobj, $app_id, $uid);
        break;
      case '3':
        test_fopenFile_03($tfsobj, $app_id, $uid);
        break;
      case '4':
        test_fopenFile_04($tfsobj, $app_id, $uid);
        break;
      case '5':
        test_fopenFile_05($tfsobj, $app_id, $uid);
        break;
      case '6':
        test_fopenFile_06($tfsobj, $app_id, $uid);
        break;
      case '7':
        test_fopenFile_07($tfsobj, $app_id, $uid);
        break;
      case '8':
        test_fopenFile_08($tfsobj, $app_id, $uid);
        break;
      case '9':
        test_fopenFile_09($tfsobj, $app_id, $uid);
        break;
      case '10':
        test_fopenFile_10($tfsobj, $app_id, $uid);
        break;
      default:
        echo "param invalid!";
        break;
    }


?>
