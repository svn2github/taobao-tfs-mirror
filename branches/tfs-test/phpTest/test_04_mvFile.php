<?php
function test_mvFile_01($tfsobj, $uid)
{
    echo "\n------------case 1: mv file with right filePath----------------\n";
    $file_path="/testmvfile1src";
    echo "create filePath: $file_path\n";
    $ret=$tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret\n";

    $file_path1="/testmvfile1tar";
    echo "srcfilePath:$file_path\n";
    echo "tarfilePath:$file_path1\n";
    $ret = $tfsobj->mv_file($uid, $file_path, $file_path1);
    echo "mv file return: $ret\n";
    
    echo "rm filePath: $file_path1\n";
    $ret = $tfsobj->rm_file($uid, $file_path1);
    echo "rm file return: $ret\n";
    echo "-----------------------case 1 end------------------------------\n";
}

function test_mvFile_02($tfsobj, $uid)
{
    echo "\n---------case 2: mv file with empty tarfilePath----------------\n";
    $file_path="/testmvfile2src";
    echo "create filePath: $file_path\n";
    $ret=$tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret\n";
    
    echo "mv file to empty filePath";
    $ret = $tfsobj->mv_file($uid, $file_path, "");
    echo "mv file return: $ret\n";

    echo "rm filePath:$file_path\n";
    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm file return: $ret\n";
    echo "----------------------case 2 end-------------------------------\n";
}

function test_mvFile_03($tfsobj, $uid)
{
    echo "\n----------case 3: mv file with null tarfilePath----------------\n";
    $file_path="/testmvfile3src";
    echo "create filePath: $file_path\n";
    $ret=$tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret\n";

    echo "mv file to null filePath";
    $ret = $tfsobj->mv_file($uid, $file_path, NULL);
    echo "mv file return: $ret\n";

    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm file return: $ret\n";
    echo "----------------------case 3 end-------------------------------\n";
}

function test_mvFile_04($tfsobj, $uid)
{
    echo "\n---------case 4: mv file with empty srcfilePath----------------\n";
    $file_path="/testmvfile4tar";
    echo "mv empty filePath to tarfilePath:$file_path\n";
    $ret = $tfsobj->mv_file($uid, "", $file_path);
    echo "mv file return: $ret\n";
    echo "-----------------------case 4 end------------------------------\n";
}

function test_mvFile_05($tfsobj, $uid)
{
    echo "\n----------case 5: mv file with null srcfilePath----------------\n";
    $file_path="/testmvfile4tar";
    echo "mv null filePath to tarfilePath:$file_path\n";
    $ret = $tfsobj->mv_file($uid, NULL, $file_path);
    echo "mv file return: $ret\n";
    echo "-----------------------case 5 end------------------------------\n";
}

function test_mvFile_06($tfsobj, $uid)
{
    echo "\n---------case 6: mv file with leap filePath--------------------\n";
    $file_path="/testmvfile6src";
    echo "create filePath: $file_path\n";
    $ret=$tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret\n";

    $file_path1="/testmvfile6dirtar/testfiletar1";
    echo "srcfilePath:$file_path\n";
    echo "tarfilePath:$file_path1\n";
    $ret = $tfsobj->mv_file($uid, $file_path, $file_path1);
    echo "mv file return: $ret\n";
    
    echo "rm filePath:$file_path";
    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm file return: $ret\n";
    echo "-----------------------case 6 end------------------------------\n";
}

function test_mvFile_07($tfsobj, $uid)
{
    echo "\n---------case 7: mv file with leap filePath--------------------\n";
    $dir_path="/testmvfile7dirsrc";
    echo "create dirPath:$dir_path\n";
    $ret=$tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";
    $file_path="/testmvfile7dirsrc/testmvfile";
    echo "create filePath: $file_path\n";
    $ret=$tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret\n";

    $dir_path="/testmvfile7dirtar";
    echo "create dirPath: $dir_path\n";
    $ret=$tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";

    $file_path1="/testmvfile7dirtar/testfiletar1";
    echo "srcfilePath:$file_path\n";
    echo "tarfilePath:$file_path1\n";
    $ret = $tfsobj->mv_file($uid, $file_path, $file_path1);
    echo "mv file return: $ret\n";

    echo "rm filePath:$file_path1";
    $ret = $tfsobj->rm_file($uid, $file_path1);
    echo "rm file return: $ret\n";

    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dir return: $ret\n";
    $dir_path="/testmvfile7dirsrc";
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dir return: $ret\n";
    echo "-----------------------case 7 end------------------------------\n";
}

function test_mvFile_08($tfsobj, $uid)
{
    echo "\n-------case 8: mv file with exist filePath---------------------\n";
    $file_path="/testmvfile8src";
    echo "create filePath: $file_path\n";
    $ret=$tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret\n";
    $file_path1="/testmvfile8tar";
    echo "create filePath: $file_path1\n";
    $ret=$tfsobj->create_file($uid, $file_path1);
    echo "create file return:$ret\n";

    echo "srcfilePath:$file_path\n";
    echo "tarfilePath:$file_path1\n";
    $ret = $tfsobj->mv_file($uid, $file_path, $file_path1);
    echo "mv file return: $ret\n";

    echo "rm filePath:$file_path1\n";
    $ret = $tfsobj->rm_file($uid, $file_path1);
    echo "rm file return: $ret\n";
    echo "rm filePath:$file_path\n";
    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm file return: $ret\n";
    echo "---------------------case 8 end--------------------------------\n";
}

function test_mvFile_09($tfsobj, $uid)
{
    echo "\n--------case 9: mv file with wrong filePath 1------------------\n";
    $file_path="/";
    $file_path1="/testmvfile9tar";
    echo "srcfilePath:$file_path\n";
    echo "tarfilePath:$file_path1\n";
    $ret = $tfsobj->mv_file($uid, $file_path, $file_path1);
    echo "mv file return: $ret\n";
    echo "---------------------case 9 end--------------------------------\n";
}

function test_mvFile_10($tfsobj, $uid)
{
    echo "\n--------case 10: mv file with wrong filePath 2------------------\n";
    $file_path="testmvfile10src";
    $file_path1="/testmvfile10tar";
    echo "srcfilePath:$file_path\n";
    echo "tarfilePath:$file_path1\n";
    $ret = $tfsobj->mv_file($uid, $file_path, $file_path1);
    echo "mv file return: $ret\n";
    echo "---------------------case 10 end--------------------------------\n";
}

function test_mvFile_11($tfsobj, $uid)
{
    echo "\n--------case 11: mv file with wrong filePath 3------------------\n";
    $file_path="/testmvfile11src";
    echo "create filePath: $file_path\n";
    $ret=$tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret\n";

    $file_path1="/";
    echo "srcfilePath:$file_path\n";
    echo "tarfilePath:$file_path1\n";
    $ret = $tfsobj->mv_file($uid, $file_path, $file_path1);
    echo "mv file return: $ret\n";

    echo "rm filePath:$file_path";
    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm file return: $ret\n";
    echo "---------------------case 11 end--------------------------------\n";
}

function test_mvFile_12($tfsobj, $uid)
{
    echo "\n--------case 12: mv file with wrong filePath 4------------------\n";
    $file_path="/testmvfile12src";
    echo "create filePath: $file_path\n";
    $ret=$tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret\n";

    $file_path1="testmvfile12tar";
    echo "srcfilePath:$file_path\n";
    echo "tarfilePath:$file_path1\n";
    $ret = $tfsobj->mv_file($uid, $file_path, $file_path1);
    echo "mv file return: $ret\n";

    echo "rm filePath:$file_path";
    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm file return: $ret\n";
    echo "---------------------case 12 end--------------------------------\n";
}

function test_mvFile_13($tfsobj, $uid)
{
    echo "\n--------case 13: mv file with wrong filePath 5------------------\n";
    $file_path="/testmvfile13src";
    echo "create filePath: $file_path\n";
    $ret=$tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret\n";

    $file_path="////testmvfile13src///";
    $file_path1="///testmvfile13tar//";
    echo "srcfilePath:$file_path\n";
    echo "tarfilePath:$file_path1\n";
    $ret = $tfsobj->mv_file($uid, $file_path, $file_path1);
    echo "mv file return: $ret\n";

    $file_path1="/testmvfile13tar";
    echo "rm filePath:$file_path1";
    $ret = $tfsobj->rm_file($uid, $file_path1);
    echo "rm file return: $ret\n";
    echo "---------------------case 13 end--------------------------------\n";
}

function test_mvFile_14($tfsobj, $uid)
{
    $file_path="/testmvfile6src";
    $ret = $tfsobj->rm_file($uid, $file_path);
    $file_path="/testmvfile11src";
    $ret = $tfsobj->rm_file($uid, $file_path);
    $file_path="/testmvfile12src";
    $ret = $tfsobj->rm_file($uid, $file_path);
    $file_path="/testmvfile13src";
    $ret = $tfsobj->rm_file($uid, $file_path);
}

/*
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
//    echo "\n<---------------rcServer:$rcServerIp , appkey:$appKey , app_id: $app_id--------->\n";
   echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";


   switch($argv[4])
   {
      case '1':
        test_mvFile_01($tfsobj, $uid);
        break;
      case '2':
        test_mvFile_02($tfsobj, $uid);
        break;
      case '3':
        test_mvFile_03($tfsobj, $uid);
        break;
      case '4':
        test_mvFile_04($tfsobj, $uid);
        break;
      case '5':
        test_mvFile_05($tfsobj, $uid);
        break;
      case '6':
        test_mvFile_06($tfsobj, $uid);
        break;
      case '7':
        test_mvFile_07($tfsobj, $uid);
        break;
      case '8':
        test_mvFile_08($tfsobj, $uid);
        break;
      case '9':
        test_mvFile_09($tfsobj, $uid);
        break;
      case '10':
        test_mvFile_10($tfsobj, $uid);
        break;
      case '11':
        test_mvFile_11($tfsobj, $uid);
        break;
      case '12':
        test_mvFile_12($tfsobj, $uid);
        break;
      case '13':
        test_mvFile_13($tfsobj, $uid);
        break;
      case '14':
        test_mvFile_14($tfsobj, $uid);
        break;
      default:
        echo "param invalid!";
        break;
    }

?>
