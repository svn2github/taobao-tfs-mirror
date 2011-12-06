<?php
function test_readFile_01($tfsobj)
{
   echo "\n----------------case 1: test read right-----------------------------\n";
   echo "\nopen file suffix:.txt\n";
   $fd=$tfsobj->open(NULL ,".txt",1);
   echo "\nopen return fd:$fd\n";

   $data="fahdvjb11334434najfnsdoaidfhaklj";
   $data_len= strlen($data);
   echo "\ndatalength:$data_len, data:$data\n";
   $ret=$tfsobj->write($fd, $data, $data_len);
   echo "\nwrite file return:$ret\n";
   $filename=$tfsobj->close($fd);
   echo "\nfilelength:$filename[0], filename:$filename[1]\n";
   
   echo "\nopen fileName:$filename[1] , suffix: .txt\n";
   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";

   echo "read fd:$fd , count:$data_len\n";
   $ary=$tfsobj->read($fd , $data_len);
   echo "\nread result:\n  length: $ary[0], data: $ary[1]\n";
   $tfsobj->close($fd);
   echo "\n---------------------test read right end----------------------------\n";
}

function test_readFile_02($tfsobj)
{
   echo "\n-----------------case 2: test read with wrong fd(not open)---------------------\n";
   $fd=-1;
   echo "\nread fd:$fd\n";
   $ary=$tfsobj->read($fd, 10);
   echo "\nread result:\n  length: $ary[0], data: $ary[1]\n";
   $tfsobj->close($fd);

   $fd=0;
   echo "\nread fd:$fd\n";
   $ary=$tfsobj->read($fd, 10);
   echo "\nread result:\n  length: $ary[0], data: $ary[1]\n";
   $tfsobj->close($fd);

   $fd=1;
   echo "\nread fd:$fd\n";
   $ary=$tfsobj->read($fd, 10);
   echo "\nread result:\n  length: $ary[0], data: $ary[1]\n";
   $tfsobj->close($fd);
   echo "\n-------------------test read with wrong fd end-----------------------\n";
}

function test_readFile_03($tfsobj)
{
   echo "\n----------case 3: test read with different length----------------\n";
   echo "\nopen file suffix:.txt\n";
   $fd=$tfsobj->open(NULL ,".txt",1);
   echo "\nopen return fd:$fd\n";

   $data="case3:testreadfilewithdifferentdatalength";
   $data_len= strlen($data);
   echo "\ndatalength:$data_len, data:$data\n";
   $ret=$tfsobj->write($fd, $data, $data_len);
   echo "\nwrite file return:$ret\n";
   $filename=$tfsobj->close($fd);
   echo "\nfilelength:$filename[0], filename:$filename[1]\n";

   echo "\nopen fileName:$filename[1] , suffix: .txt\n";
   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";

   $length=-1;
   echo "read fd:$fd , count:$length\n";
   $ary=$tfsobj->read($fd , $length);
   echo "\nread result:\n  length: $ary[0], data: $ary[1]\n";
   $tfsobj->close($fd);

   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";
   $length=10;
   echo "read fd:$fd , count:$length\n";
   $ary=$tfsobj->read($fd , $length);
   echo "\nread result:\n  length: $ary[0], data: $ary[1]\n";
   $tfsobj->close($fd);

   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";
   $length=50;
   echo "read fd:$fd , count:$length\n";
   $ary=$tfsobj->read($fd , $length);
   echo "\nread result:\n  length: $ary[0], data: $ary[1]\n";
   $tfsobj->close($fd);
   echo "\n------------------------case 3 end----------------------------\n";
}

/**
 *@param rc_ip: rc server_ip
 *@param app_key: application key
 *@param local ip : local ip
*/
    $tfsobj = new tfs_client($argv[1], $argv[2], $argv[3]);
    $app_id = $tfsobj->get_app_id();

    echo "\n-----rcServer:$$argv[1] , appkey:$argv[2] , app_id: $app_id-----\n";

    switch($argv[4])
    {
      case '1':
        test_readFile_01($tfsobj);
        break;
      case '2':
        test_readFile_02($tfsobj);
        break;
      case '3':
        test_readFile_03($tfsobj);
        break;
      default:
        echo "param invalid!";
        break;
     }

?>
