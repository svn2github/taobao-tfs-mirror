<?php
function test_lsDir_01($tfsobj, $app_id, $uid)
{

   echo "\n-----------------case 1: ls dir with right dirPath-------------------\n";
   $dir_path="/testlsdir1";
   echo "create dirPath:$dir_path\n";
   $ret=$tfsobj->create_dir($uid, $dir_path);
   echo "create dir return:$ret\n";

   echo "create dirPath:$dir_path/test1 \n";
   $ret=$tfsobj->create_dir($uid, $dir_path."/test1");
   echo "create dir return:$ret\n";

   echo "create dirPath:$dir_path/test2\n";
   $ret=$tfsobj->create_dir($uid, $dir_path."/test2");
   echo "create dir return:$ret\n";

   echo "create dirPath:$dir_path/test3\n";
   $ret=$tfsobj->create_dir($uid, $dir_path."/test3");
   echo "create dir return:$ret\n";

   echo "ls dirPath:$dir_path\n";
   $ary=$tfsobj->ls_dir($app_id, $uid, $dir_path);
   echo "\nls dir return:\n $ary[0], $ary[1], $ary[2], $ary[3], $ary[4]\n";
   echo " $ary[5], $ary[6], $ary[7], $ary[8], $ary[9]\n";
   echo " $ary[10], $ary[11], $ary[12], $ary[13], $ary[14]\n";

   echo "rm dirPath:$dir_path/test1 \n";
   $ret = $tfsobj->rm_dir($uid, $dir_path."/test1");
   echo "rm dir return:$ret\n";

   echo "rm dirPath:$dir_path/test2\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path."/test2");
   echo "rm dir return:$ret";

   echo "rm dirPath:$dir_path/test3\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path."/test3");
   echo "rm dir return:$ret\n";

   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return:$ret\n";
   echo "-----------------------------case 1 end------------------------------\n";
}

function test_lsDir_02($tfsobj, $app_id, $uid)
{
   echo "\n------------case 2: ls dir with empty dirPath------------------------\n";
   echo "ls empty dirPath";
   $ary=$tfsobj->ls_dir($app_id, $uid, "");
   echo "\n ls dir return: $ary[0], $ary[1], $ary[2]\n";
   echo "----------------------------case 2 end-------------------------------\n";
}

function test_lsDir_03($tfsobj, $app_id, $uid)
{
   echo "\n-----------------case 3: ls dir with null dirPath--------------------\n";
   echo "ls null dirPath";
   $ary=$tfsobj->ls_dir($app_id, $uid, NULL);
   echo "\n ls dir return: $ary[0], $ary[1], $ary[2]\n";
   echo "-----------------------------case 3 end------------------------------\n";
}

function test_lsDir_04($tfsobj, $app_id, $uid)
{
   echo "\n-----------------case 4: ls dir with wrong dirPath 1------------------\n";
   $dir_path="/";
   echo "ls dirPath:$dir_path\n";
   $ary=$tfsobj->ls_dir($app_id, $uid, $dir_path);
   echo "\n ls dir return: $ary[0], $ary[1], $ary[2]\n";
   echo "-----------------------------case 4 end------------------------------\n";
}

function test_lsDir_05($tfsobj, $app_id, $uid)
{
   echo "\n-----------------case 5: ls dir with wrong dirPath 2------------------\n";
   $dir_path="testlsdir5";
   echo "ls dirPath:$dir_path\n";
   $ary=$tfsobj->ls_dir($app_id, $uid, $dir_path);
   echo "\n ls dir return: $ary[0], $ary[1], $ary[2]\n";
   echo "-----------------------------case 5 end------------------------------\n";
}

function test_lsDir_06($tfsobj, $app_id, $uid)
{
   echo "\n-----------------case 6: ls dir with wrong dirPath 3------------------\n";
   $dir_path="/testlsdir6";
   echo "dirPath:$dir_path\n";
   $ret=$tfsobj->create_dir($uid, $dir_path);
   echo "create dir return:$ret\n";
   echo "dirPath:$dir_path/test1 \n";
   $ret=$tfsobj->create_dir($uid, $dir_path."/test1");
   echo "dirPath:$dir_path\n";
   echo "create dir return:$ret\n";
   echo "dirPath:$dir_path/test2\n";
   $ret=$tfsobj->create_dir($uid, $dir_path."/test2");
   echo "create dir return:$ret\n";
   echo "dirPath:$dir_path/test3\n";
   $ret=$tfsobj->create_dir($uid, $dir_path."/test3");
   echo "create dir return:$ret\n";

   $dir_path="////testlsdir6////";
   echo "ls dirPath:$dir_path\n";
   $ary=$tfsobj->ls_dir($app_id, $uid, $dir_path);
   echo "\nls dir return:\n $ary[0], $ary[1], $ary[2], $ary[3], $ary[4]\n";
   echo " $ary[5], $ary[6], $ary[7], $ary[8], $ary[9]\n";
   echo " $ary[10], $ary[11], $ary[12], $ary[13], $ary[14]\n";

   $dir_path="/testlsdir6";
   echo "rm dirPath:$dir_path/test1 \n";
   $ret = $tfsobj->rm_dir($uid, $dir_path."/test1");
   echo "rm dir return:$ret\n";

   echo "rm dirPath:$dir_path/test2\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path."/test2");
   echo "rm dir return:$ret\n";

   echo "rm dirPath:$dir_path/test3\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path."/test3");
   echo "rm dir return:$ret\n";

   echo "rm dirPath:$dir_path\n";
   $ret = $tfsobj->rm_dir($uid, $dir_path);
   echo "rm dir return:$ret\n";
   echo "-----------------------------case 6 end------------------------------\n";
}

function test_lsDir_07($tfsobj, $app_id, $uid)
{

}

/**
 *@param rc_ip: rc server_ip
 *@param app_key: application key
 *@param local ip : local ip
*/
   $tfsobj = new tfs_client($argv[1], $argv[2], $argv[3]);
   $app_id = $tfsobj->get_app_id();
   $uid = 10;
   echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";

   switch($argv[4])
   {
      case '1':
        test_lsDir_01($tfsobj, $app_id, $uid);
        break;
      case '2':
        test_lsDir_02($tfsobj, $app_id, $uid);
        break;
      case '3':
        test_lsDir_03($tfsobj, $app_id, $uid);
        break;
      case '4':
        test_lsDir_04($tfsobj, $app_id, $uid);
        break;
      case '5':
        test_lsDir_05($tfsobj, $app_id, $uid);
        break;
      case '6':
        test_lsDir_06($tfsobj, $app_id, $uid);
        break;
      default:
        echo "param invalid!";
        break;
    }

?>
