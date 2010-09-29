#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT([tfs], [1.3], [duolong@taobao.com])
AC_CONFIG_SRCDIR([src/nameserver/service.cpp])

AM_INIT_AUTOMAKE(tfs, 1.3)
# Checks for programs.
AC_PROG_AWK
AC_PROG_CXX
AC_PROG_CC
AC_PROG_CPP
AC_PROG_INSTALL
AC_PROG_LN_S
AC_PROG_MAKE_SET
AC_PROG_LIBTOOL

# Checks for libraries.
# FIXME: Replace `main' with a function in `-lWand':
AC_CHECK_LIB([Wand], [main])
# FIXME: Replace `main' with a function in `-lbz2':
AC_CHECK_LIB([bz2], [main])
# FIXME: Replace `main' with a function in `-lc':
AC_CHECK_LIB([c], [main])
# FIXME: Replace `main' with a function in `-lcrypt':
AC_CHECK_LIB([crypt], [main])
# FIXME: Replace `main' with a function in `-lcrypto':
AC_CHECK_LIB([crypto], [main])
# FIXME: Replace `main' with a function in `-ldl':
AC_CHECK_LIB([dl], [main])
# FIXME: Replace `main' with a function in `-lfreetype':
AC_CHECK_LIB([freetype], [main])
# FIXME: Replace `main' with a function in `-ljpeg':
AC_CHECK_LIB([jpeg], [main])
# FIXME: Replace `main' with a function in `-lm':
AC_CHECK_LIB([m], [main])
# FIXME: Replace `main' with a function in `-lnsl':
AC_CHECK_LIB([nsl], [main])
# FIXME: Replace `main' with a function in `-lpthread':
AC_CHECK_LIB([pthread], [main])
# FIXME: Replace `main' with a function in `-lrt':
AC_CHECK_LIB([rt], [main])
# FIXME: Replace `main' with a function in `-lssl':
AC_CHECK_LIB([ssl], [main])
# FIXME: Replace `main' with a function in `-ltiff':
AC_CHECK_LIB([tiff], [main])
# FIXME: Replace `main' with a function in `-lz':
AC_CHECK_LIB([z], [main])

AC_ARG_WITH([tblib-root],
           AS_HELP_STRING([--with-tblib-root],
           [tbnet installation path]),
           TBLIB_ROOT=${withval})
if  test -z "${TBLIB_ROOT}/include"; then
   dnl tbnet include path supplied
   TBLIB_ROOT="/usr/local" 
fi
if ! test -d "${TBLIB_ROOT}/include"; then
	AC_MSG_ERROR([tblib_root is not work])
	exit 1;
fi
AC_SUBST([TBLIB_ROOT])

AC_CACHE_CHECK(if fallocate is implemented, ac_cv_func_fallocate,
  AC_TRY_RUN([
    #include <fcntl.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <unistd.h>
    int main() 
    {
      char* file = "./test_fallocate";
      int fd = open(file, O_RDWR | O_CREAT, 0666);
      if (fd < 0)
      {
        exit(1);
      }
      if (fallocate(fd, 0, 0, 512)) 
      {
        perror("fallocate:");
        exit(1);
      }
      close(fd);
      unlink(file);
      exit(0);
    }
  ],ac_cv_func_fallocate="yes",ac_cv_func_fallocate="no")
)
if test "$ac_cv_func_fallocate" = "yes" ; then
  AC_DEFINE_UNQUOTED(TFS_DS_FALLOCATE, 1, [ac_cv_func_fallocate])
else
  AC_DEFINE_UNQUOTED(TFS_DS_FALLOCATE, 0, [ac_cv_func_fallocate])
fi

if test "$ac_cv_set_fallocate" = "no" ; then
  AC_MSG_ERROR([fallocate does not work.])
	exit 1;
fi

AC_ARG_WITH([release],
  AS_HELP_STRING([--with-release],
    [use optimize (default is NO)]),
    [   
      if test "$withval" = "yes"; then
        CXXFLAGS="-O2 -finline-functions -fno-strict-aliasing -Wall -fPIC -D__STDC_LIMIT_MACROS"
      fi  
    ],  
    [CXXFLAGS="-g -Wall -fPIC -D__STDC_LIMIT_MACROS"]
) 

ac_default_prefix=${HOME}/tfs_bin
# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_CHECK_HEADERS([arpa/inet.h fcntl.h inttypes.h limits.h netdb.h netinet/in.h stddef.h stdint.h stdlib.h string.h sys/ioctl.h sys/socket.h sys/time.h sys/timeb.h sys/vfs.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_CONST
AC_C_INLINE
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_PID_T
AC_TYPE_SIZE_T
AC_HEADER_TIME
AC_STRUCT_TM
AC_C_VOLATILE
AC_CHECK_TYPES([ptrdiff_t])

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_FORK
AC_PROG_GCC_TRADITIONAL
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MEMCMP
AC_FUNC_MKTIME
AC_FUNC_MMAP
AC_FUNC_SELECT_ARGTYPES
AC_FUNC_SETPGRP
AC_TYPE_SIGNAL
AC_FUNC_STAT
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_FUNC_STRFTIME
AC_CHECK_FUNCS([gethostbyname gettimeofday localtime_r memmove memset mkdir mkfifo munmap select socket strcasecmp strchr strdup strerror strncasecmp strrchr strstr strtol strtoul strtoull tzset fdatasync ftruncate rmdir])

AC_CONFIG_FILES([Makefile
                 conf/Makefile
                 scripts/Makefile
                 src/Makefile
                 src/common/Makefile
                 src/message/Makefile
                 src/client/Makefile
                 src/nameserver/Makefile
                 src/dataserver/Makefile
                 src/adminserver/Makefile
                 src/monitor/Makefile
                 src/tools/Makefile
                 src/tools/dataserver/Makefile
                 src/tools/nameserver/Makefile
                 tests/Makefile
                 tests/batch/Makefile
                 tests/client/Makefile
                 tests/dataserver/Makefile
                 tests/intergrate/Makefile
                 ])
AC_OUTPUT
