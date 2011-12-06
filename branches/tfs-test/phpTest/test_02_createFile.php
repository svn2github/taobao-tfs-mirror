<?php
function test_createfile_01($tfsobj, $uid)
{
   echo "\n--------------------case 1:create file right--------------------------\n";
   $file_path="/testcreatefile1";
   echo "create filePath: $file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm_file ret: $ret\n";
   echo "------------------------------case 1 end------------------------------\n";
}

function test_createfile_02($tfsobj, $uid)
{
   echo "\n----------case 2:create file with empty filePath----------------------\n";
   echo "create empty filePath\n";
   $file_path="";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";
   echo "----------------------------case 2 end--------------------------------\n";
}

function test_createfile_03($tfsobj, $uid)
{
   echo "\n-----------case 3:create file with null filePath---------------------\n";
   echo "create null filePath\n";
   $file_path=NULL;
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";
   echo "--------------------------case 3 end---------------------------------\n";
}

function test_createfile_04($tfsobj, $uid)
{
   echo "\n-----------case 4:create file with wrong filePath 1-------------------\n";
   $file_path="/";
   echo "create filePath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";
   echo "----------------------------case 4 end--------------------------------\n";
}

function test_createfile_05($tfsobj, $uid)
{
   echo "\n-----------case 5:create file with wrong filePath 2-------------------\n";
   $file_path="testcreatefile5";
   echo "create filePath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";
   echo "\n-------------------------case 5 end---------------------------------\n";
}

function test_createfile_06($tfsobj, $uid)
{
   echo "\n-----------case 6:create file with wrong filePath 3-------------------\n";
   $file_path="////testcreatefile6";
   echo "create filePath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   $file_path="/testcreatefile6";
   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm_file ret: $ret\n";
   echo "\n--------------------------case 6 end------------------------------\n";
}

function test_createfile_07($tfsobj, $uid)
{
   echo "\n-----------case 7:create file with wrong filePath 4-------------------\n";
   $file_path="////testcreatefile7////";
   echo "create filePath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   $file_path="/testcreatefile7";
   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm_file ret: $ret\n";
   echo "\n-------------------------case 7 end--------------------------------\n";
}

function test_createfile_08($tfsobj, $uid)
{
   echo "\n------------case 8:create file with leap filePath wrong----------------\n";
   $file_path="/text/text";
   echo "create filePath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";
   echo "-------------------------case 8 end-----------------------------------\n";
}

function test_createfile_09($tfsobj, $uid)
{
   echo "\n---------case 9:create file with leap filePath right--------------------\n";
   $dir_path="/testcreateFile9";
   echo "create dirPath:$dir_path\n";
   $ret=$tfsobj->create_dir($uid, $dir_path);
   echo "create dir return:$ret\n";

   $file_path="/testcreateFile9/text";
   echo "create filePath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm_file ret: $ret\n";

   echo "rm dirPath: $dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return: $ret\n";
   echo "\n-----------------------------case 9 end-----------------------------\n";
}

function test_createfile_10($tfsobj, $uid)
{
   echo "\n------------case 10:create file with same dirPath--------------------------\n";
   $dir_path="/testcreatefile10";
   echo "create dirPath:$dir_path\n";
   $ret=$tfsobj->create_dir($uid, $dir_path);
   echo "create dir return:$ret\n";

   $file_path="/testcreatefile10";
   echo "create filePath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "rm dirPath: $dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return:$ret\n";

   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return:$ret\n";
   echo "--------------------------------case 10 end------------------------------\n";
}

function test_createfile_11($tfsobj, $uid)
{
   echo "\n------------case 11:create file with same filePath--------------------------\n";
   $file_path="/testcreatefile11";
   echo "create filePath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "second create filePath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "rm filePath: $file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return:$ret\n";
   echo "---------------create file with same filePath end---------------------------\n";
}

function test_createfile_12($tfsobj, $uid)
{
    $file_path="/testcreatefile11";
    $tfsobj->rm_file($uid, $file_path);
}

function test_createfile_13($tfsobj, $uid)
{
}

/**
 *@param rc_ip: rc server_ip
 *@param app_key: application key
 *@param local ip : local ip
*/
   $tfsobj = new tfs_client($argv[1], $argv[2], $argv[3]);
   $app_id = $tfsobj->get_app_id();
   $uid = 15;
   echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";


   switch($argv[4])
   {
      case '1':
        test_createfile_01($tfsobj, $uid);
        break;
      case '2':
        test_createfile_02($tfsobj, $uid);
        break;
      case '3':
        test_createfile_03($tfsobj, $uid);
        break;
      case '4':
        test_createfile_04($tfsobj, $uid);
        break;
      case '5':
        test_createfile_05($tfsobj, $uid);
        break;
      case '6':
        test_createfile_06($tfsobj, $uid);
        break;
      case '7':
        test_createfile_07($tfsobj, $uid);
        break;
      case '8':
        test_createfile_08($tfsobj, $uid);
        break;
      case '9':
        test_createfile_09($tfsobj, $uid);
        break;
      case '10':
        test_createfile_10($tfsobj, $uid);
        break;
      case '11':
        test_createfile_11($tfsobj, $uid);
        break;
      case '12':
        test_createfile_12($tfsobj, $uid);
        break;
      case '13':
        test_createfile_13($tfsobj, $uid);
        break;
      default:
        echo "param invalid!";
        break;
    }
?>
