<?php
function test_mvDir_01($tfsobj, $uid)
{
    echo "\n-------------case 1: mv dir with right dirPath------------------\n";
    $dir_path="/testmvdir1src";
    echo "srcdirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create srcdir return: $ret\n";
    $dir_path1="/testmvdir1tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n"; 
    echo "dirPath:$dir_path1";
    $ret = $tfsobj->rm_dir($uid, $dir_path1);
    echo "rm tardirPath return:$ret\n";
    echo "-------------------------case 1 end-----------------------------\n";
}

function test_mvDir_02($tfsobj, $uid)
{
    echo "\n----------case 2: mv dir with srcdirPath not empty--------------\n";
    $dir_path="/testmvdir2src";
    echo "srcdirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create srcdir return: $ret\n";
    $file_path="/testmvdir2src/testfile";
    echo "filePath:$file_path\n";
    $ret = $tfsobj->create_file($uid, $file_path);
    echo "create file return: $ret\n";

    $dir_path1="/testmvdir2tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";

    $file_path="/testmvdir2tar/testfile";
    echo "rm filePath:$file_path\n";
    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm filePath return:$ret\n";
    echo "rm dirPath:$dir_path1";
    $ret = $tfsobj->rm_dir($uid, $dir_path1);
    echo "rm tardirPath return:$ret\n";

    $dir_path="/testmvdir2src";
    echo "create srcdirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create srcdir return: $ret\n";

    $file_path="/testmvdir2src/testfile2";
    echo "create filePath:$file_path\n";
    $ret = $tfsobj->create_file($uid, $file_path);
    echo "create file return: $ret\n";
    $dir_path2="/testmvdir2src/dir2";
    echo "create dirPath:$dir_path2\n";
    $ret = $tfsobj->create_dir($uid, $dir_path2);
    echo "create dir return: $ret\n";

    echo "srcdirPath:$dir_path\n";
    $dir_path1="/testmvdir2tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";

    $file_path="/testmvdir2tar/testfile2";
    echo "rm filePath:$file_path\n";
    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm filePath return:$ret\n";
    $dir_path2="/testmvdir2tar/dir2";
    echo "rm dirPath:$dir_path2\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path2);
    echo "rm tardirPath return:$ret\n";
    echo "rm dirPath:$dir_path1\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path1);
    echo "rm tardirPath return:$ret\n";
    echo "\n-------------------------case 2 end-----------------------------\n";
}

function test_mvDir_03($tfsobj, $uid)
{
    echo "\n---------case 3: mv dir with empty srcdirPath-------------------\n";
    $dir_path1="/testmvdir3tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, "", $dir_path1);
    echo "mv dir return:$ret\n";
    echo "-------------------------case 3 end-----------------------------\n";
}

function test_mvDir_04($tfsobj, $uid)
{
    echo "\n----------case 4: mv dir with null srcdirPath-------------------\n";
    $dir_path1="/testmvdir4tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, NULL, $dir_path1);
    echo "mv dir return:$ret\n";
    echo "-------------------------case 4 end-----------------------------\n";
}

function test_mvDir_05($tfsobj, $uid)
{
    echo "\n--------case 5: mv dir with empty tardirPath--------------------\n";
    $dir_path="/testmvdir5src";
    echo "create srcdirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create srcdir return: $ret\n";

    $ret = $tfsobj->mv_dir($uid, $dir_path, "");
    echo "mv dir return:$ret\n";

    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm srcdirPath return:$ret\n";
    echo "-------------------------case 5 end-----------------------------\n";
}

function test_mvDir_06($tfsobj, $uid)
{
    echo "\n--------case 6: mv dir with null tardirPath---------------------\n";
    $dir_path="/testmvdir6src";
    echo "create srcdirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create srcdir return: $ret\n";

    $ret = $tfsobj->mv_dir($uid, $dir_path, NULL);
    echo "mv dir return:$ret\n";

    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm srcdirPath return:$ret\n";
    echo "-------------------------case 6 end-----------------------------\n";
}

function test_mvDir_07($tfsobj, $uid)
{
 echo "\n---------case 7: mv dir with wrong dirPath 1--------------------\n";
    $dir_path="/";
    echo "srcdirPath:$dir_path\n";
    $dir_path1="/testmvdir7tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";
    echo "-------------------------case 7 end-----------------------------\n";
}

function test_mvDir_08($tfsobj, $uid)
{
    echo "\n---------case 8: mv dir with wrong dirPath 2--------------------\n";
    $dir_path="testmvdir8src";
    echo "srcdirPath:$dir_path\n";
    $dir_path1="/testmvdir8tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";
    echo "-------------------------case 8 end-----------------------------\n";
}

function test_mvDir_09($tfsobj, $uid)
{
    echo "\n---------case 9: mv dir with wrong dirPath 3--------------------\n";
    $dir_path="/testmvdir9src";
    echo "create dirPath:$dir_path\n";
    $ret=$tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";

    echo "srcdirPath:$dir_path\n";
    $dir_path1="testmvdir9tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";
    
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dir return:$ret\n";
    echo "-------------------------case 9 end-----------------------------\n";
}

function test_mvDir_10($tfsobj, $uid)
{
    echo "\n---------case 10: mv dir with wrong dirPath 4--------------------\n";
    $dir_path="/testmvdir10src";
    echo "create dirPath:$dir_path\n";
    $ret=$tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";

    echo "srcdirPath:$dir_path\n";
    $dir_path1="/";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";
    
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dir return:$ret\n";
    echo "-------------------------case 10 end-----------------------------\n";
}

function test_mvDir_11($tfsobj, $uid)
{
    echo "\n---------case 11: mv dir with wrong dirPath 5--------------------\n";
    $dir_path="/testmvdir11src";
    echo "create dirPath:$dir_path\n";
    $ret=$tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";

    $dir_path="////testmvdir11src//";
    echo "srcdirPath:$dir_path\n";
    $dir_path1="///testmvdir11tar///";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";

    $dir_path1="/testmvdir11tar";
    echo "rm dirPath:$dir_path1\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path1);
    echo "rm dirPath return:$ret\n";
    echo "-------------------------case 11 end-----------------------------\n";
}

function test_mvDir_12($tfsobj, $uid)
{
    echo "\n------------case 12: mv dir with exist tardirPath----------------\n";
    $dir_path="/testmvdir12src";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return: $ret\n";
    $dir_path1="/testmvdir12tar\n";
    echo "create dirPath:$dir_path1\n";
    $ret = $tfsobj->create_dir($uid, $dir_path1);
    echo "create dir return: $ret\n";

    echo "dirPath:$dir_path\n";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";

    echo "rm dirPath:$dir_path1\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path1);
    echo "rm tardirPath:$ret\n";
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm srcdirPath return:$ret\n";
    echo "-------------------------case 12 end-----------------------------\n";
}

function test_mvDir_13($tfsobj, $uid)
{
    echo "\n------------case 13: mv dir with exist filePath------------------\n";
    $dir_path="/testmvdir13src";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return: $ret\n";
    $file_path="/testmvdir13tar";
    echo "create filePath:$file_path\n";
    $ret = $tfsobj->create_file($uid, $file_path);
    echo "create file return: $ret\n";

    echo "dirPath:$dir_path\n";
    $dir_path1="/testmvdir13tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";

    echo "rm filePath:$file_path\n";
    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm filePath return:$ret\n";
    echo "rm dirPath:$dir_path1\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path1);
    echo "rm dirPath:$ret\n";
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dirPath return:$ret\n";
    echo "-------------------------case 13 end-----------------------------\n";
}

function test_mvDir_14($tfsobj, $uid)
{
    echo "\n------------case 14: mv dir with leap tardirPath--------------------\n";
    $dir_path="/testmvdir14src";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";
    
    echo "srcdirPath:$dir_path\n";
    $dir_path1="/testmvdir14tar/test";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";
    
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dirPath return:$ret\n";
    echo "-------------------------case 14 end-----------------------------\n";
}

function test_mvDir_15($tfsobj, $uid)
{
    echo "\n---------case 15: mv dir with srcdirPath depth 2-----------------\n";
    $dir_path="/testmvdir15src";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";
    $dir_path="/testmvdir15src/test1";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";
    $dir_path="/testmvdir15src/test1/test";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";
    $file_path="/testmvdir15src/test1/testfile";
    echo "create filePath:$file_path\n";
    $ret = $tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret";

    $dir_path="/testmvdir15src/test1";
    echo "srcdirPath:$dir_path\n";
    $dir_path1="/testmvdir15tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";

    $dir_path="/testmvdir15src";
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dirPath return:$ret\n";
    $dir_path="/testmvdir15tar/test";
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dirPath return:$ret\n";
    $file_path="/testmvdir15tar/testfile";
    echo "rm filePath:$file_path\n";
    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm filePath return:$ret\n";
    $dir_path="/testmvdir15tar";
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dirPath return:$ret\n";
    echo "-------------------------case 15 end-----------------------------\n";
}

function test_mvDir_16($tfsobj, $uid)
{
    echo "\n-------------case 16: mv dir with tardir depth 2-----------------\n";
    $dir_path="/testmvdir16src";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";
    $dir_path="/testmvdir16src/test";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";
    $file_path="/testmvdir16src/testfile";
    echo "create filePath:$file_path\n";
    $ret = $tfsobj->create_file($uid, $file_path);
    echo "create file return:$ret\n";

    $dir_path="/testmvdir16tar";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";

    $dir_path="/testmvdir16src";
    echo "srcdirPath:$dir_path\n";
    $dir_path1="/testmvdir16tar/test16tar";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";

    $file_path="/testmvdir16tar/test16tar/testfile";
    echo "rm filePath:$file_path\n";
    $ret = $tfsobj->rm_file($uid, $file_path);
    echo "rm filePath return:$ret\n";
    $dir_path="/testmvdir16tar/test16tar/test";
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dirPath return:$ret\n";
    $dir_path="/testmvdir16tar/test16tar";
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dirPath return:$ret\n";
    $dir_path="/testmvdir16tar";
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dirPath return:$ret\n";
    echo "-------------------------case 16 end-----------------------------\n";
}

function test_mvDir_17($tfsobj, $uid)
{
    echo "\n----------------case 17: mv dir with cricle----------------------\n";
    $dir_path="/testmvdir17src";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";
    $dir_path="/testmvdir17src/test17src";
    echo "create dirPath:$dir_path\n";
    $ret = $tfsobj->create_dir($uid, $dir_path);
    echo "create dir return:$ret\n";

    echo "srcdirPath:$dir_path\n";
    $dir_path1="/testmvdir17src/test17src/test";
    echo "tardirPath:$dir_path1\n";
    $ret = $tfsobj->mv_dir($uid, $dir_path, $dir_path1);
    echo "mv dir return:$ret\n";

    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dirPath return:$ret\n";
    $dir_path="/testmvdir17src";
    echo "rm dirPath:$dir_path\n";
    $ret = $tfsobj->rm_dir($uid, $dir_path);
    echo "rm dirPath return:$ret\n";
    echo "-------------------------case 17 end-----------------------------\n";
}

function test_mvDir_18($tfsobj, $uid)
{
   $dir_path="/testmvdir9src";
   $tfsobj->rm_dir($uid, $dir_path);
   $dir_path="/testmvdir10src";
   $tfsobj->rm_dir($uid, $dir_path);
   $dir_path="/testmvdir14src";
   $tfsobj->rm_dir($uid, $dir_path);
   $dir_path="/testmvdir17src/test17src";
   $tfsobj->rm_dir($uid, $dir_path);
   $dir_path="/testmvdir17src";
   $tfsobj->rm_dir($uid, $dir_path);
   $dir_path="/testmvdir13src";
   $tfsobj->rm_dir($uid, $dir_path);
}

/*
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
        test_mvDir_01($tfsobj, $uid);
        break;
      case '2':
        test_mvDir_02($tfsobj, $uid);
        break;
      case '3':
        test_mvDir_03($tfsobj, $uid);
        break;
      case '4':
        test_mvDir_04($tfsobj, $uid);
        break;
      case '5':
        test_mvDir_05($tfsobj, $uid);
        break;
      case '6':
        test_mvDir_06($tfsobj, $uid);
        break;
      case '7':
        test_mvDir_07($tfsobj, $uid);
        break;
      case '8':
        test_mvDir_08($tfsobj, $uid);
        break;
      case '9':
        test_mvDir_09($tfsobj, $uid);
        break;
      case '10':
        test_mvDir_10($tfsobj, $uid);
        break;
      case '11':
        test_mvDir_11($tfsobj, $uid);
        break;
      case '12':
        test_mvDir_12($tfsobj, $uid);
        break;
      case '13':
        test_mvDir_13($tfsobj, $uid);
        break;
      case '14':
        test_mvDir_14($tfsobj, $uid);
        break;
      case '15':
        test_mvDir_15($tfsobj, $uid);
        break;
      case '16':
        test_mvDir_16($tfsobj, $uid);
        break;
      case '17':
        test_mvDir_17($tfsobj, $uid);
        break;
      case '18':
        test_mvDir_18($tfsobj, $uid);
        break;
      default:
        echo "param invalid!";
        break;
    }

?>
