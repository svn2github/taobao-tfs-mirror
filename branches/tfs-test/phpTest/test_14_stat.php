<?php
function test_statFile_01($tfsobj)
{
   echo "\n----------------case 1: test stat right-----------------------------\n";
   echo "open file(null) suffix:.txt\n";
   $fd=$tfsobj->open(NULL,".txt",1);
   echo "\nopen fd return:$fd\n";
   
   $data="fahdvjb11334434najfnsdoaidfhaklj";
   $data_len= strlen($data);
   echo "\ndatalength:$data_len, data:$data\n";
   $ret=$tfsobj->write($fd, $data, $data_len);
   echo "\nwrite return:$ret\n";
   $filename=$tfsobj->close($fd);
   echo "\nfilelength:$filename[0], filename:$filename[1]\n";

   echo "open file($filename[1]) suffix:.txt\n";
   $fd=$tfsobj->open($filename[1],".txt",2);
   echo "\nopen $filename[1] return fd:$fd\n";
   $ary=$tfsobj->read($fd, $data_len);
   echo "\nread result:\n  length: $ary[0], data: $ary[1]\n";
   $st=$tfsobj->stat($fd);
   echo "\nstat return: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
   $tfsobj->close($fd);
   echo "\n---------------------test stat right end----------------------------\n";
}

function test_statFile_02($tfsobj)
{
   echo "\n----------------case 2: test stat with wrong fd(not open)-----------\n";
   $fd=-1;
   echo "\nstat fd:$fd\n";
   $st=$tfsobj->stat($fd);
   echo "\nstat return: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
   $tfsobj->close($fd);
   $fd=0;
   echo "\nstat fd:$fd\n";
   $st=$tfsobj->stat($fd);
   echo "\nstat return: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
   $tfsobj->close($fd);
   $fd=1;
   echo "\nstat fd:$fd\n";
   $st=$tfsobj->stat($fd);
   echo "\nstat return: $st[0], $st[1], $st[2], $st[3], $st[4], $st[5], $st[6]\n";
   $tfsobj->close($fd);
   echo "\n-----------------test stat with wrong fd(not open) end--------------\n";
}

function test_statFile_03($tfsobj)
{

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
        test_statFile_01($tfsobj);
        break;
      case '2':
        test_statFile_02($tfsobj);
        break;
      default:
        echo "param invalid!";
        break;
     }

?>
