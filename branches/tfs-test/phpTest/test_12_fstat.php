<?php
function test_pstatFile_01($tfsobj, $app_id, $uid)
{
   echo "\n----------------case 1: pstat file with right filePath-----------------\n";
   $file_path="/testpstatfile1";
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
   echo "write data return: $ret\n";

   echo "fstat filePath:$file_path\n";
   $ary=$tfsobj->fstat($app_id, $uid, $file_path);
   echo "fstat return: $ary[0], $ary[1], $ary[2]\n";
    
   echo "rm filePath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "-------------------------------case 1 end------------------------------\n";
}

function test_pstatFile_02($tfsobj, $app_id, $uid)
{
   echo "\n---------------case 2: pstat file with empty filePath------------------\n";
   echo "fstat empty filePath\n";
   $ary=$tfsobj->fstat($app_id, $uid, "");
   echo "fstat return: $ary[0], $ary[1], $ary[2]\n";
   echo "-------------------------------case 2 end------------------------------\n";
}

function test_pstatFile_03($tfsobj, $app_id, $uid)
{
   echo "\n---------------case 3: pstat file with null filePath-------------------\n";
   echo "fstat full filePath\n";
   $ary=$tfsobj->fstat($app_id, $uid, NULL);
   echo "fstat return: $ary[0], $ary[1], $ary[2]\n";
   echo "-------------------------------case 3 end------------------------------\n";
}

function test_pstatFile_04($tfsobj, $app_id, $uid)
{
   echo "\n-------------case 4: pstat file with wrong filePath 1------------------\n";
   $file_path="/";
   echo "fstat filPath:$file_path\n";
   $ary=$tfsobj->fstat($app_id, $uid, $file_path);
   echo "fstat return: $ary[0], $ary[1], $ary[2]\n";
   echo "-------------------------------case 4 end------------------------------\n";
}

function test_pstatFile_05($tfsobj, $app_id, $uid)
{
   echo "\n-------------case 5: pstat file with wrong filePath 2------------------\n";
   $file_path="testpstatfile5";
   echo "fstat filPath:$file_path\n";
   $ary=$tfsobj->fstat($app_id, $uid, $file_path);
   echo "fstat return: $ary[0], $ary[1], $ary[2]\n";
   echo "-------------------------------case 5 end------------------------------\n";
}

function test_pstatFile_06($tfsobj, $app_id, $uid)
{
   echo "\n-------------case 6: pstat file with wrong filePath 3------------------\n";
   $file_path="/testpstatfile6";
   echo "create filPath:$file_path\n";
   $ret=$tfsobj->create_file($uid, $file_path);
   echo "create file return:$ret\n";

   echo "fopen filPath:$file_path\n";
   $fd=$tfsobj->fopen($app_id, $uid, $file_path, 4);
   echo "fopen file fd return: $fd\n";

   $data='taobaofilesystemphpclient6666';
   $data_len = strlen($data);
   echo "datalength:$data_len , data:$data\n";
   $ret=$tfsobj->pwrite($fd, $data, $data_len, 0);
   echo "write data return: $ret\n";

  $file_path="/////testpstatfile6////";
   echo "fstat filPath:$file_path\n";
   $ary=$tfsobj->fstat($app_id, $uid, $file_path);
   echo "fstat return: $ary[0], $ary[1], $ary[2]\n";

   $file_path="/testpstatfile6";
   echo "filPath:$file_path\n";
   $ret = $tfsobj->rm_file($uid, $file_path);
   echo "rm file return: $ret\n";
   echo "-------------------------------case 6 end------------------------------\n";
}


/**
 *@param rc_ip: rc server_ip
 *@param app_key: application key
 *@param local ip : local ip
*/
   $tfsobj = new tfs_client($argv[1], $argv[2], $argv[3]);
   $app_id = $tfsobj->get_app_id();
   $uid = 11;

   echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";

   switch($argv[4])
   {
      case '1':
        test_pstatFile_01($tfsobj, $app_id, $uid);
        break;
      case '2':
        test_pstatFile_02($tfsobj, $app_id, $uid);
        break;
      case '3':
        test_pstatFile_03($tfsobj, $app_id, $uid);
        break;
      case '4':
        test_pstatFile_04($tfsobj, $app_id, $uid);
        break;
      case '5':
        test_pstatFile_05($tfsobj, $app_id, $uid);
        break;
      case '6':
        test_pstatFile_06($tfsobj, $app_id, $uid);
        break;
      default:
        echo "param invalid!";
        break;
    }

?>
