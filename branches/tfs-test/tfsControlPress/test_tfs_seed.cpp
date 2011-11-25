/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version: $Id: test_tfs_seed.cpp 155 2011-07-26 14:33:27Z mingyan.zc@taobao.com $
 *
 * Authors:
 *   mingyan.zc@taobao.com
 *      - initial release
 *
 */

#include "test_tfs_seed.h"
#include "func.h"
#include "test_gfactory.h"


////////////////////////////TestSeed///////////////////////////////
tbsys::CThreadMutex TestTfsSeed::_lock;
int TestTfsSeed::_have_display_statis = 0;

int TestTfsSeed::setUp()
{
	TestTfsCase::setUp();

  TestTfsSeed::_lock.lock();  
  TestGFactory::_index ++;
  /* Set localkey */
  _keyIndex = TestGFactory::_index;  
  _loopFlag = CConfig::getCConfig().getInt("tfsseed","loop_flag"); 
  _writeCount = CConfig::getCConfig().getInt("tfsseed","writeCount"); 
  _unitMin = CConfig::getCConfig().getInt("tfsseed","unit_min"); 
  _unitMax = CConfig::getCConfig().getInt("tfsseed","unit_max"); 
  _largeFlag = CConfig::getCConfig().getInt("tfsseed","largeFlag"); 
  _writeVerifyFlag = CConfig::getCConfig().getInt("tfsseed","writeVerifyFlag");
  _unlinkRatio = CConfig::getCConfig().getInt("tfsseed","unlinkRatio");

  /* Init */ 
  memset((char *)_localKey, 0, LOCALKEYLEN);
  sprintf( _localKey, "%s%d", "./localkey/", TestGFactory::_index );

  TestTfsSeed::_lock.unlock();

  /* choose a random number between _unitMin and _unitMax as write unit */
  srand(time(NULL)+rand()+pthread_self());
  _unit = _unitMin + (int) ((_unitMax - _unitMin + 1.0) * (rand() / (RAND_MAX + 1.0)));
  _data = new char [_unit + 1];

  TestCommonUtils::generateData(_data, _unit);

	return 0;
}

int TestTfsSeed::retryOpen(int retry_count)
{
	int retry = 0; 
	int ret = -1;

	TBSYS_LOG(DEBUG,"local key:%s",_localKey);

  /* Init */
  memset((char *)_fileName, 0x00, TFSNAMELEN);

	while (retry++ < retry_count) 
  {
    if (_largeFlag == 1)
    {
	  	ret = _tfsFile->open((char *)NULL, (char *)".jpg", (char *)NULL, T_WRITE|T_LARGE, _localKey);
    }
    else
    {
	  	ret = _tfsFile->open((char *)NULL, (char *)".jpg", (char *)NULL, T_WRITE);
    }
		if (ret > 0)
    {
      _fd = ret; 
      TBSYS_LOG(DEBUG, "_fd = %d", _fd);
			break;
    }
		sleep(1);
	}
	return ret;
}

int TestTfsSeed::writeData(char *data,int size)
{

	int left = size;
	int nwrite = 0; 
	int ret = -1;

	while(left > 0)
  {
		ret = _tfsFile->write(_fd, data + nwrite, left);
		if(ret < 0)
    {
			TBSYS_LOG(ERROR,"write failed:%d",ret);
			break;
		}
    else if(ret > left)
    {
			ret = left;
			break;
		}
    else 
    {
			left -= ret;
			nwrite += ret;
		}
	}

	return ret;
}

int TestTfsSeed::testWrite()
{
	int ret  = -1;
	_preCrc = 0;

	ret = retryOpen(3);
	if (ret <= 0)
  {
		TBSYS_LOG(ERROR,"open failed:%d",ret);
		return ret;
	}

  for(int iLoop = 0; iLoop < _writeCount; iLoop ++)
  {
	  _preCrc = Func::crc(_preCrc, _data, _unit);
    ret = writeData(_data, _unit);
    if (ret < 0)
    {
      TBSYS_LOG(ERROR,"tfs_write failed:%d",ret);
      return ret;
    }
  }

	ret = _tfsFile->close(_fd , _fileName, TFSNAMELEN);
  
	return ret;
}

int TestTfsSeed::testRead()
{
	char data[MAX_READ_SIZE];
	int nread = 0;
	uint32_t _crc = 0;
	int64_t _totalSize = 0;
	TfsFileStat info;

  memset((char *)&info, 0, sizeof(TfsFileStat));

  int ret;
  if (_largeFlag == 1)
    {
	  	ret = _tfsFile->open((char *)_fileName, (char *)".jpg", T_READ|T_LARGE);
    } else {
	  	ret = _tfsFile->open((char *)_fileName, (char *)".jpg", T_READ);
    }

	if(ret <= 0)
  {
		TBSYS_LOG(ERROR,"tfs_open failed:%d",ret);
		return ret;
	}
  else
  {
    _fd = ret;
  }

	ret = _tfsFile->fstat( _fd, &info, FORCE_STAT );
	if(ret != 0)
  {
		TBSYS_LOG(ERROR,"tfs_stat failed:%d",ret);
		return ret;
	}

	while(nread < info.size_) 
  {
		int rlen = _tfsFile->read( _fd, data, MAX_READ_SIZE );
		if (rlen < 0) 
    {
			TBSYS_LOG(DEBUG, "read tfsfile fail: %d\n", rlen);
			_tfsFile->close( _fd );
			return rlen;
		}

		if (rlen == 0) break;

		_crc = Func::crc(_crc, data, rlen);

		_totalSize += rlen;

		nread += rlen;

	}

	_tfsFile->close( _fd );
  
  if (_crc != _preCrc)
  {
    TBSYS_LOG( ERROR, "CRC is different: %u <> %u", _crc, _preCrc );
    ret = -1;
  }

	if (_totalSize != info.size_){
    TBSYS_LOG( ERROR, "File size is different: %d <> %d", _totalSize, info.size_ );
		ret = -1;
	}

	return ret;
}

int TestTfsSeed::testUnlink()
{
  int ret = 0;

  TfsFileStat info;
  memset((char *)&info, 0x00, sizeof(TfsFileStat));

  const char *postfix = strlen(_fileName) > 18 ? (char *)(_fileName + 18) : NULL;
  int64_t fileSize = 0;
#if defined(VER_132)
  ret = _tfsFile->unlink(_fileName, postfix);
#elif defined(VER_140)
  ret = _tfsFile->unlink(_fileName, postfix, fileSize);
#elif defined(VER_141)
  ret = _tfsFile->unlink(fileSize, _fileName, postfix);
#endif

  if (ret != 0)
  {
    TBSYS_LOG(ERROR, "Unlink file: %s failed: %d, filesize: %d", _fileName, ret, fileSize);
    return ret;
  }
#if 0
  int fd = 0;
  if (_largeFlag)
  {
    ret = _tfsFile->open((char *)_fileName,(char *)postfix, T_STAT | T_LARGE);                                                                     
  } else {
    ret = _tfsFile->open((char *)_fileName,(char *)postfix, T_STAT);
  }

  if (ret <= 0)
  {
    TBSYS_LOG(ERROR,"Open failed:%d",ret);
    return -1;
  } else {
    fd = ret;
  }

  sleep(2);

  ret = _tfsFile->fstat(fd, &info, FORCE_STAT);
  if(ret != 0)
  {
    TBSYS_LOG(ERROR,"Fstat failed:%d",ret);
    _tfsFile->close(fd);
    return ret;
  }
  _tfsFile->close(fd);

  if (info.flag_ == 0)
  {
    TBSYS_LOG(ERROR,"Unlink faild: delete falg = %d", info.flag_);
    return -1;
  }
#endif
  return ret;
}

int TestTfsSeed::run()
{
	int ret = TFS_ERROR;
	int tmp_ret = TFS_ERROR;
	int64_t start_time = 0;
	int64_t end_time = 0;
  char strCrc[CRCSIZE];
  unlinked_ = false;

  memset((char *)strCrc, 0, CRCSIZE);

  ret = testWrite();
	if (TFS_SUCCESS != ret)
  {
		TBSYS_LOG(ERROR,"TestSeed::testWrite: FAILED: %d", ret);
	}
  else
  {
		TBSYS_LOG(INFO,"TestSeed::testWrite: SUCCESSFUL(%s)", _fileName);
  }

	end_time = CTimeUtil::getTime();

	TestGFactory::_statisForWrite.addStatis(start_time, end_time, ret, "writeFile");

  if (TFS_SUCCESS == ret && _writeVerifyFlag)
  {
    tmp_ret = testRead();
    if (TFS_SUCCESS != tmp_ret)
    {
      TBSYS_LOG(ERROR, "TestSeed::testRead: FAILED: %d", tmp_ret);
    }
    else 
    {
      TBSYS_LOG(INFO, "TestSeed::testRead: SUCCESSFUL");
    }

    end_time = CTimeUtil::getTime();

    TestGFactory::_statisForRead.addStatis(start_time, end_time, tmp_ret, "readFile");
  }

  if (TFS_SUCCESS == ret && _unlinkRatio > 0 )
  {
    srand(time(NULL) + rand() + pthread_self());
    int randNo = rand() % 100;
    if (randNo < _unlinkRatio)
    {
      unlinked_ = true;
      std::string record = _fileName;
      record = record + ".jpg ";
      /* Save it */
      _unlinked_recordSet.insert(record);

      tmp_ret = testUnlink();
      if (TFS_SUCCESS != tmp_ret)
      {
        TBSYS_LOG(ERROR,"TestSeed::testUnlink: FAILED: %d", tmp_ret);
      }
      else
      {
        TBSYS_LOG(INFO,"TestSeed::testUnlink: SUCCESSFUL");
      }

      end_time = CTimeUtil::getTime();

      TestGFactory::_statisForUnlink.addStatis(start_time, end_time, tmp_ret, "unlinkFile");
    }
  }

  if (false == unlinked_ && TFS_SUCCESS == ret && 0 != _fileName[0])
  { 
    /* Edit the filename and crc */
    snprintf(strCrc, CRCSIZE, "%u", _preCrc);
    std::string record = _fileName;
    record = record + ".jpg " + strCrc;
    /* Save it */
    _recordSet.insert(record);
  }

  if (_loopFlag == 0)
  {
	  TestGFactory::_tfsTest.stop();
  }
  
  return ret;
}

int TestTfsSeed::saveFilename()
{
  FILE *fp = NULL;
  char *filelist = (char *)CConfig::getCConfig().getString("tfsseed",
         "seedlist_name");

  if (filelist == NULL)
  {
    filelist = "./tfsseed_file_list.txt";                                                                                                                
  }

  if((fp = fopen(filelist, "a")) == NULL)
  {
      TBSYS_LOG(ERROR,"open write_file_list failed.");
      return -1;
  }
  std::set<std::string>::iterator it=_recordSet.begin();
  for(; it != _recordSet.end(); it++)
  {
    fprintf(fp, "%s\n", it->c_str());
  }

  fflush(fp);
  fclose(fp);

  // write unlinked file list
  filelist = (char *)CConfig::getCConfig().getString("tfsseed",
         "unlinkedlist_name");
  if (filelist == NULL)
  {
    filelist = "./tfsunlinked_file_list.txt";                                                                                                                
  }

  if((fp = fopen(filelist, "a")) == NULL)
  {
      TBSYS_LOG(ERROR,"open unlinked_file_list failed.");
      return -1;
  }

  it=_unlinked_recordSet.begin();
  for(; it != _unlinked_recordSet.end(); it++)
  {
    fprintf(fp, "%s\n", it->c_str());
  }

  fflush(fp);
  fclose(fp);

  return 0;
}

void TestTfsSeed::tearDown()
{
	delete []  _data;
	_data = NULL;

	bool need_display = false;

	TestTfsSeed::_lock.lock();
	if (_have_display_statis == 0)
	{
		TBSYS_LOG(INFO,"Statis will be displayed.");
		_have_display_statis += 1;
		need_display = true;
	}

  /* Save the record to file */
  int iRet = saveFilename();  
  if (iRet != 0)
  {
    TBSYS_LOG(ERROR,"It's failed to save file name list !!!");
  }
	TestTfsSeed::_lock.unlock();

	if (need_display)
  {
    TBSYS_LOG(INFO,"-------------------------------write statis-------------------------------------");
  	TestGFactory::_statisForWrite.displayStatis("write statis");
    if (_writeVerifyFlag)
    {
	    TBSYS_LOG(INFO,"-------------------------------read statis-------------------------------------");
		  TestGFactory::_statisForRead.displayStatis("read statis");
    }
    if (_unlinkRatio)
    {
      TBSYS_LOG(INFO,"-------------------------------unlink statis-------------------------------------");
		  TestGFactory::_statisForUnlink.displayStatis("unlink statis");
    } 
  	TBSYS_LOG(INFO,"------------------------------------END-----------------------------------------");
	}

	TestTfsCase::tearDown();
}

