<?php
function test_writeFile_01($tfsobj)
{
    echo "\n-----------------case 1: write right-----------------\n";
    echo "\nopen file(null) suffix:.txt\n";
    $fd=$tfsobj->open(NULL,".txt",1);
    echo "\nopen return fd:$fd\n";

    $data="12234567890abcdjaca321";
    $data_len = strlen($data);
    echo "datalength:$data_len, data:$data\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";

    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n-------------------write right end-------------------\n";
}

function test_writeFile_02($tfsobj)
{
    echo "\n-----------------case 2: write with offset(>datalength)-----------------\n";
    echo "\nopen file(null) suffix:.txt\n";
    $fd=$tfsobj->open(NULL,".txt",1);
    echo "\nopen return fd:$fd\n";

    $data="12234567890abcdjaca321"; 
    $data_len = strlen($data);
    $offset = 100;
    echo "datalength:$data_len, data:$data \noffset:$offset\n";
    $ret=$tfsobj->write($fd, $data, $offset);
    echo "\nwrite return:$ret\n";

    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n----------write with offset(>datalength) end-----------\n";
}

function test_writeFile_03($tfsobj)
{
    echo "\n-----------------case 3: write with offset(< datalength)-----------------\n";
    echo "\nopen file(null) suffix:.txt\n";
    $fd=$tfsobj->open(NULL,".txt",1);
    echo "\nopen return fd:$fd\n";

    $data="12234567890abcdjaca321";
    $data_len = strlen($data);
    $offset = 10;
    echo "datalength:$data_len, data:$data \noffset:$offset\n";
    $ret=$tfsobj->write($fd, $data, $offset);
    echo "\nwrite return:$ret\n";

    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n----------write with offset(< datalength) end-----------\n";

}

function test_writeFile_04($tfsobj)
{
    echo "\n-----------------case 4: write with wrong offset(<0)-----------------\n";
    echo "\nopen file(null) suffix:.txt\n";
    $fd=$tfsobj->open(NULL,".txt",1);
    echo "\nopen return fd:$fd\n";

    $data="12234567890abcdjaca321";
    $data_len = strlen($data);
    $offset = -1;
    echo "datalength:$data_len, data:$data \noffset:$offset\n";
    $ret=$tfsobj->write($fd, $data, $offset);
    echo "\nwrite return:$ret\n";

    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n----------write with wrong offset(>0) end-----------\n";
}

function test_writeFile_05($tfsobj)
{
    echo "\n-----------------case 5: write with wrong offset(0)-----------------\n";
    echo "\nopen file(null) suffix:.txt\n";
    $fd=$tfsobj->open(NULL,".txt",1);
    echo "\nopen return fd:$fd\n";

    $data="12234567890abcdjaca321";
    $data_len = strlen($data);
    $offset = 0;
    echo "datalength:$data_len, data:$data \noffset:$offset\n";
    $ret=$tfsobj->write($fd, $data, $offset);
    echo "\nwrite return:$ret\n";

    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n----------write with wrong offset(0) end-----------\n";
}

function test_writeFile_06($tfsobj)
{
    echo "\n-----------------case 6: write with wrong fd(<0)-----------------\n";
    $data="12234567890abcdjaca321";
    $data_len = strlen($data);
    echo "datalength:$data_len, data:$data\n";
    $fd=-1;
    echo "\nopen return fd:$fd\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";
    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n----------write with wrong fd(<0) end-----------\n";
}

function test_writeFile_07($tfsobj)
{
    echo "\n-----------------case 7: write with wrong fd(0)-----------------\n";
    $data="12234567890abcdjaca321"; 
    $data_len = strlen($data);
    echo "datalength:$data_len, data:$data\n";
    $fd=0;
    echo "\nopen return fd:$fd\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";
    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n----------write with fd(0) end-----------\n"; 
}

function test_writeFile_08($tfsobj)
{
    echo "\n--------------case 8: write with wrong fd(not open)--------------\n";
    $data="12234567890abcdjaca321";
    $data_len = strlen($data);
    echo "datalength:$data_len, data:$data\n";
    $fd=1;
    echo "\nopen return fd:$fd\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";
    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n--------------------write with fd(not open) end--------------------\n";
}

function test_writeFile_09($tfsobj)
{
    echo "\n---------case 9: write with empty data-----------------\n";
    echo "\nopen file(null) suffix:.txt\n";
    $fd=$tfsobj->open(NULL,".txt",1);
    echo "\nopen return fd:$fd\n";

    $data="";
    $data_len = strlen($data);
    echo "datalength:$data_len, data:$data\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";

    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n---------------write with empty data end---------------\n";
}

function test_writeFile_10($tfsobj)
{
   echo "\n--------case 10: write with null data--------------\n";
    echo "\nopen file(null) suffix:.txt\n";
    $fd=$tfsobj->open(NULL,".txt",1);
    echo "\nopen return fd:$fd\n";

    $data=NULL;
    $data_len = strlen($data);
    echo "datalength:$data_len, data:$data\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";

    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n------------write with null data end-------------\n";
}

function test_writeFile_11($tfsobj)
{
    echo "\n-----------------case 11: write with 5 times-----------------\n";
    echo "\nopen file(null) suffix:.txt\n";
    $fd=$tfsobj->open(NULL,".txt",1);
    echo "\nopen return fd:$fd\n";

    $data="case11:writewith5times";
    $data_len = strlen($data);
    echo "datalength:$data_len, data:$data\n";

    echo "\nfrist write\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";

    echo "\nsecond write\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";

    echo "\nthird write\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";

    echo "\nfourth write\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";

    echo "\nfifth write\n";
    $ret=$tfsobj->write($fd, $data, $data_len);
    echo "\nwrite return:$ret\n";

    $filename=$tfsobj->close($fd);
    echo "\nfilelength:$filename[0], filename:$filename[1]\n";
    echo "\n----------write with 5 time end-----------\n";
}

function test_writeFile_12($tfsobj)
{

}
/*
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
        test_writeFile_01($tfsobj);
        break;
      case '2':
        test_writeFile_02($tfsobj);
        break;
      case '3':
        test_writeFile_03($tfsobj);
        break;
      case '4':
        test_writeFile_04($tfsobj);
        break;
      case '5':
        test_writeFile_05($tfsobj);
        break;
      case '6':
        test_writeFile_06($tfsobj);
        break;
      case '7':
        test_writeFile_07($tfsobj);
        break;
      case '8':
        test_writeFile_08($tfsobj);
        break;
      case '9':
        test_writeFile_09($tfsobj);
        break;
      case '10':
        test_writeFile_10($tfsobj);
        break;
      case '11':
        test_writeFile_11($tfsobj);
        break;
      default:
        echo "param invalid!";
        break;
     }

?>
