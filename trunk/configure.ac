#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT([tfs], 1.3, duolong@taobao.com)
AC_CONFIG_SRCDIR([src/common/config_item.h])

AM_INIT_AUTOMAKE(tfs, 1.3)
# Checks for programs.
AC_PROG_AWK
AC_PROG_CXX
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_MAKE_SET
AC_PROG_RANLIB
AC_PROG_LIBTOOL

# Checks for libraries.
# FIXME: Replace `main' with a function in `-lc':
AC_CHECK_LIB([c], [main])
# FIXME: Replace `main' with a function in `-ldl':
AC_CHECK_LIB([dl], [main])
# FIXME: Replace `main' with a function in `-lm':
AC_CHECK_LIB([m], [main])
# FIXME: Replace `main' with a function in `-lpthread':
AC_CHECK_LIB([pthread], [main])
# FIXME: Replace `main' with a function in `-lrt':
AC_CHECK_LIB([rt], [main])
# FIXME: Replace `main' with a function in `-lz':
AC_CHECK_LIB([z], [main])

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_CHECK_HEADERS([arpa/inet.h fcntl.h inttypes.h limits.h netdb.h netinet/in.h stdint.h stdlib.h string.h sys/ioctl.h sys/socket.h sys/time.h sys/vfs.h unistd.h])

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

# Checks for library functions.
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_ERROR_AT_LINE
AC_PROG_GCC_TRADITIONAL
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_MALLOC
AC_FUNC_MEMCMP
AC_FUNC_MMAP
AC_FUNC_REALLOC
AC_TYPE_SIGNAL
AC_FUNC_STAT
AC_CHECK_FUNCS([fdatasync ftruncate gethostbyname getpagesize gettimeofday memset mkdir munmap rmdir socket strcasecmp strchr strdup strerror strstr strtoul strtoull tzset])

AC_CONFIG_FILES([Makefile
                 conf/Makefile
                 scripts/Makefile
                 src/Makefile
                 src/adminserver/Makefile
                 src/client/Makefile
                 src/common/Makefile
                 src/dataserver/Makefile
                 src/message/Makefile
                 src/monitor/Makefile
                 src/nameserver/Makefile
                 src/tools/Makefile
                 src/tools/dataserver/Makefile
                 src/tools/nameserver/Makefile
                 tests/Makefile
                 tests/batch/Makefile
                 tests/client/Makefile
                 tests/dataserver/Makefile
                 tests/intergrate/Makefile])
AC_OUTPUT
