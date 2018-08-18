/* zmalloc - total amount of allocated memory aware version of malloc()
 *
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

 /* This function provide us access to the original libc free(). This is useful
  * for instance to free results obtained by backtrace_symbols(). We need
  * to define this function before including zmalloc.h that may shadow the
  * free implementation if we use jemalloc or another non standard allocator. */
#include "zmalloc.h"

void zlibc_free(void *ptr) {
	free(ptr);
}

#ifdef __APPLE__
#include <AvailabilityMacros.h>
#endif

#ifdef __linux__
#include <linux/version.h>
#include <features.h>
#endif

/* Define redis_fstat to fstat or fstat64() */
#if defined(__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
#define redis_fstat fstat64
#define redis_stat stat64
#else
#define redis_fstat fstat
#define redis_stat stat
#endif

/* Test for proc filesystem */
#ifdef __linux__
#define HAVE_PROC_STAT 1
#define HAVE_PROC_MAPS 1
#define HAVE_PROC_SMAPS 1
#define HAVE_PROC_SOMAXCONN 1
#endif

/* Test for task_info() */
#if defined(__APPLE__)
#define HAVE_TASKINFO 1
#endif

/* Test for backtrace() */
#if defined(__APPLE__) || (defined(__linux__) && defined(__GLIBC__))
#define HAVE_BACKTRACE 1
#endif

/* MSG_NOSIGNAL. */
#ifdef __linux__
#define HAVE_MSG_NOSIGNAL 1
#endif

/* Test for polling API */
#ifdef __linux__
#define HAVE_EPOLL 1
#endif

#if (defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined (__NetBSD__)
#define HAVE_KQUEUE 1
#endif

#ifdef __sun
#include <sys/feature_tests.h>
#ifdef _DTRACE_VERSION
#define HAVE_EVPORT 1
#endif
#endif

/* Define redis_fsync to fdatasync() in Linux and fsync() for all the rest */
#ifdef __linux__
#define redis_fsync fdatasync
#else
#define redis_fsync fsync
#endif

/* Define rdb_fsync_range to sync_file_range() on Linux, otherwise we use
 * the plain fsync() call. */
#ifdef __linux__
#if defined(__GLIBC__) && defined(__GLIBC_PREREQ)
#if (LINUX_VERSION_CODE >= 0x020611 && __GLIBC_PREREQ(2, 6))
#define HAVE_SYNC_FILE_RANGE 1
#endif
#else
#if (LINUX_VERSION_CODE >= 0x020611)
#define HAVE_SYNC_FILE_RANGE 1
#endif
#endif
#endif

#ifdef HAVE_SYNC_FILE_RANGE
#define rdb_fsync_range(fd,off,size) sync_file_range(fd,off,size,SYNC_FILE_RANGE_WAIT_BEFORE|SYNC_FILE_RANGE_WRITE)
#else
#define rdb_fsync_range(fd,off,size) fsync(fd)
#endif

 /* Check if we can use setproctitle().
  * BSD systems have support for it, we provide an implementation for
  * Linux and osx. */
#if (defined __NetBSD__ || defined __FreeBSD__ || defined __OpenBSD__)
#define USE_SETPROCTITLE
#endif

#if ((defined __linux && defined(__GLIBC__)) || defined __APPLE__)
#define USE_SETPROCTITLE
#define INIT_SETPROCTITLE_REPLACEMENT
void spt_init(int argc, char *argv[]);
void setproctitle(const char *fmt, ...);
#endif

/* Byte ordering detection */
#include <sys/types.h> /* This will likely define BYTE_ORDER */

#ifndef BYTE_ORDER
#if (BSD >= 199103)
# include <machine/endian.h>
#else
#if defined(linux) || defined(__linux__)
# include <endian.h>
#else
#define	LITTLE_ENDIAN	1234	/* least-significant byte first (vax, pc) */
#define	BIG_ENDIAN	4321	/* most-significant byte first (IBM, net) */
#define	PDP_ENDIAN	3412	/* LSB first in word, MSW first in long (pdp)*/

#if defined(__i386__) || defined(__x86_64__) || defined(__amd64__) || \
   defined(vax) || defined(ns32000) || defined(sun386) || \
   defined(MIPSEL) || defined(_MIPSEL) || defined(BIT_ZERO_ON_RIGHT) || \
   defined(__alpha__) || defined(__alpha)
#define BYTE_ORDER    LITTLE_ENDIAN
#endif

#if defined(sel) || defined(pyr) || defined(mc68000) || defined(sparc) || \
    defined(is68k) || defined(tahoe) || defined(ibm032) || defined(ibm370) || \
    defined(MIPSEB) || defined(_MIPSEB) || defined(_IBMR2) || defined(DGUX) ||\
    defined(apollo) || defined(__convex__) || defined(_CRAY) || \
    defined(__hppa) || defined(__hp9000) || \
    defined(__hp9000s300) || defined(__hp9000s700) || \
    defined (BIT_ZERO_ON_LEFT) || defined(m68k) || defined(__sparc)
#define BYTE_ORDER	BIG_ENDIAN
#endif
#endif /* linux */
#endif /* BSD */
#endif /* BYTE_ORDER */

/* Sometimes after including an OS-specific header that defines the
 * endianess we end with __BYTE_ORDER but not with BYTE_ORDER that is what
 * the Redis code uses. In this case let's define everything without the
 * underscores. */
#ifndef BYTE_ORDER
#ifdef __BYTE_ORDER
#if defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#ifndef LITTLE_ENDIAN
#define LITTLE_ENDIAN __LITTLE_ENDIAN
#endif
#ifndef BIG_ENDIAN
#define BIG_ENDIAN __BIG_ENDIAN
#endif
#if (__BYTE_ORDER == __LITTLE_ENDIAN)
#define BYTE_ORDER LITTLE_ENDIAN
#else
#define BYTE_ORDER BIG_ENDIAN
#endif
#endif
#endif
#endif

#if !defined(BYTE_ORDER) || \
    (BYTE_ORDER != BIG_ENDIAN && BYTE_ORDER != LITTLE_ENDIAN)
 /* you must determine what the correct bit order is for
  * your compiler - the next line is an intentional error
  * which will force your compiles to bomb until you fix
  * the above macros.
  */
#endif

#if (__i386 || __amd64 || __powerpc__) && __GNUC__
#define GNUC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)
#if defined(__clang__)
#define HAVE_ATOMIC
#endif
#if (defined(__GLIBC__) && defined(__GLIBC_PREREQ))
#if (GNUC_VERSION >= 40100 && __GLIBC_PREREQ(2, 6))
#define HAVE_ATOMIC
#endif
#endif
#endif

  /* Make sure we can test for ARM just checking for __arm__, since sometimes
   * __arm is defined but __arm__ is not. */
#if defined(__arm) && !defined(__arm__)
#define __arm__
#endif
#if defined (__aarch64__) && !defined(__arm64__)
#define __arm64__
#endif

   /* Make sure we can test for SPARC just checking for __sparc__. */
#if defined(__sparc) && !defined(__sparc__)
#define __sparc__
#endif

#if defined(__sparc__) || defined(__arm__)
#define USE_ALIGNED_ACCESS
#endif

#ifdef HAVE_MALLOC_SIZE
#define PREFIX_SIZE (0)
#else
#if defined(__sun) || defined(__sparc) || defined(__sparc__)
#define PREFIX_SIZE (sizeof(long long))
#else
#define PREFIX_SIZE (sizeof(size_t))
#endif
#endif

/* Explicitly override malloc/free etc when using tcmalloc. */
#if defined(USE_TCMALLOC)
#define malloc(size) tc_malloc(size)
#define calloc(count,size) tc_calloc(count,size)
#define realloc(ptr,size) tc_realloc(ptr,size)
#define free(ptr) tc_free(ptr)
#elif defined(USE_JEMALLOC)
#define malloc(size) je_malloc(size)
#define calloc(count,size) je_calloc(count,size)
#define realloc(ptr,size) je_realloc(ptr,size)
#define free(ptr) je_free(ptr)
#define mallocx(size,flags) je_mallocx(size,flags)
#define dallocx(ptr,flags) je_dallocx(ptr,flags)
#endif

/* To test Redis with Helgrind (a Valgrind tool) it is useful to define
 * the following macro, so that __sync macros are used: those can be detected
 * by Helgrind (even if they are less efficient) so that no false positive
 * is reported. */
 // #define __ATOMIC_VAR_FORCE_SYNC_MACROS

#if !defined(__ATOMIC_VAR_FORCE_SYNC_MACROS) && defined(__ATOMIC_RELAXED) && !defined(__sun) && (!defined(__clang__) || !defined(__APPLE__) || __apple_build_version__ > 4210057)
/* Implementation using __atomic macros. */

#define atomicIncr(var,count) __atomic_add_fetch(&var,(count),__ATOMIC_RELAXED)
#define atomicGetIncr(var,oldvalue_var,count) do { \
    oldvalue_var = __atomic_fetch_add(&var,(count),__ATOMIC_RELAXED); \
} while(0)
#define atomicDecr(var,count) __atomic_sub_fetch(&var,(count),__ATOMIC_RELAXED)
#define atomicGet(var,dstvar) do { \
    dstvar = __atomic_load_n(&var,__ATOMIC_RELAXED); \
} while(0)
#define atomicSet(var,value) __atomic_store_n(&var,value,__ATOMIC_RELAXED)
#define REDIS_ATOMIC_API "atomic-builtin"

#elif defined(HAVE_ATOMIC)
/* Implementation using __sync macros. */

#define atomicIncr(var,count) __sync_add_and_fetch(&var,(count))
#define atomicGetIncr(var,oldvalue_var,count) do { \
    oldvalue_var = __sync_fetch_and_add(&var,(count)); \
} while(0)
#define atomicDecr(var,count) __sync_sub_and_fetch(&var,(count))
#define atomicGet(var,dstvar) do { \
    dstvar = __sync_sub_and_fetch(&var,0); \
} while(0)
#define atomicSet(var,value) do { \
    while(!__sync_bool_compare_and_swap(&var,var,value)); \
} while(0)
#define REDIS_ATOMIC_API "sync-builtin"

#else
/* Implementation using pthread mutex. */

#ifdef _WIN32
#define atomicIncr(var,count) do { \
    std::unique_lock <std::mutex> lck(mutex); \
    var += (count); \
} while(0)
#define atomicGetIncr(var,oldvalue_var,count) do { \
    std::unique_lock <std::mutex> lck(mutex); \
    oldvalue_var = var; \
    var += (count); \
} while(0)
#define atomicDecr(var,count) do { \
    std::unique_lock <std::mutex> lck(mutex); \
    var -= (count); \
} while(0)
#define atomicGet(var,dstvar) do { \
    std::unique_lock <std::mutex> lck(mutex); \
    dstvar = var; \
} while(0)
#define atomicSet(var,value) do { \
    std::unique_lock <std::mutex> lck(mutex); \
    var = value; \
} while(0)
#else
#define atomicIncr(var,count) do { \
    pthread_mutex_lock(&var ## _mutex); \
    var += (count); \
    pthread_mutex_unlock(&var ## _mutex); \
} while(0)
#define atomicGetIncr(var,oldvalue_var,count) do { \
    pthread_mutex_lock(&var ## _mutex); \
    oldvalue_var = var; \
    var += (count); \
    pthread_mutex_unlock(&var ## _mutex); \
} while(0)
#define atomicDecr(var,count) do { \
    pthread_mutex_lock(&var ## _mutex); \
    var -= (count); \
    pthread_mutex_unlock(&var ## _mutex); \
} while(0)
#define atomicGet(var,dstvar) do { \
    pthread_mutex_lock(&var ## _mutex); \
    dstvar = var; \
    pthread_mutex_unlock(&var ## _mutex); \
} while(0)
#define atomicSet(var,value) do { \
    pthread_mutex_lock(&var ## _mutex); \
    var = value; \
    pthread_mutex_unlock(&var ## _mutex); \
} while(0)
#endif

#define REDIS_ATOMIC_API "pthread-mutex"
#endif

#define update_zmalloc_stat_alloc(__n) do { \
    size_t _n = (__n); \
    if (_n&(sizeof(long)-1)) _n += sizeof(long)-(_n&(sizeof(long)-1)); \
    atomicIncr(used_memory,__n); \
} while(0)

#define update_zmalloc_stat_free(__n) do { \
    size_t _n = (__n); \
    if (_n&(sizeof(long)-1)) _n += sizeof(long)-(_n&(sizeof(long)-1)); \
    atomicDecr(used_memory,__n); \
} while(0)

static size_t used_memory = 0;
#ifdef _WIN32
std::mutex mutex;
#else
pthread_mutex_t used_memory_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif

static void zmalloc_default_oom(size_t size) {
	fprintf(stderr, "zmalloc: Out of memory trying to allocate %zu bytes\n",
		size);
	fflush(stderr);
	abort();
}

static void(*zmalloc_oom_handler)(size_t) = zmalloc_default_oom;

void *zmalloc(size_t size) {
	void *ptr = malloc(size + PREFIX_SIZE);

	if (!ptr) zmalloc_oom_handler(size);
#ifdef HAVE_MALLOC_SIZE
	update_zmalloc_stat_alloc(zmalloc_size(ptr));
	return ptr;
#else
	*((size_t*)ptr) = size;
	update_zmalloc_stat_alloc(size + PREFIX_SIZE);
	return (char*)ptr + PREFIX_SIZE;
#endif
}

/* Allocation and free functions that bypass the thread cache
 * and go straight to the allocator arena bins.
 * Currently implemented only for jemalloc. Used for online defragmentation. */
#ifdef HAVE_DEFRAG
void *zmalloc_no_tcache(size_t size) {
	void *ptr = mallocx(size + PREFIX_SIZE, MALLOCX_TCACHE_NONE);
	if (!ptr) zmalloc_oom_handler(size);
	update_zmalloc_stat_alloc(zmalloc_size(ptr));
	return ptr;
}

void zfree_no_tcache(void *ptr) {
	if (ptr == NULL) return;
	update_zmalloc_stat_free(zmalloc_size(ptr));
	dallocx(ptr, MALLOCX_TCACHE_NONE);
}
#endif

void *zcalloc(size_t size) {
	void *ptr = calloc(1, size + PREFIX_SIZE);

	if (!ptr) zmalloc_oom_handler(size);
#ifdef HAVE_MALLOC_SIZE
	update_zmalloc_stat_alloc(zmalloc_size(ptr));
	return ptr;
#else
	*((size_t*)ptr) = size;
	update_zmalloc_stat_alloc(size + PREFIX_SIZE);
	return (char*)ptr + PREFIX_SIZE;
#endif
}

void *zrealloc(void *ptr, size_t size) {
#ifndef HAVE_MALLOC_SIZE
	void *realptr;
#endif
	size_t oldsize;
	void *newptr;

	if (ptr == NULL) return zmalloc(size);
#ifdef HAVE_MALLOC_SIZE
	oldsize = zmalloc_size(ptr);
	newptr = realloc(ptr, size);
	if (!newptr) zmalloc_oom_handler(size);

	update_zmalloc_stat_free(oldsize);
	update_zmalloc_stat_alloc(zmalloc_size(newptr));
	return newptr;
#else
	realptr = (char*)ptr - PREFIX_SIZE;
	oldsize = *((size_t*)realptr);
	newptr = realloc(realptr, size + PREFIX_SIZE);
	if (!newptr) zmalloc_oom_handler(size);

	*((size_t*)newptr) = size;
	update_zmalloc_stat_free(oldsize);
	update_zmalloc_stat_alloc(size + PREFIX_SIZE);
	return (char*)newptr + PREFIX_SIZE;
#endif
}

/* Provide zmalloc_size() for systems where this function is not provided by
 * malloc itself, given that in that case we store a header with this
 * information as the first bytes of every allocation. */
#ifndef HAVE_MALLOC_SIZE
size_t zmalloc_size(void *ptr) {
	void *realptr = (char*)ptr - PREFIX_SIZE;
	size_t size = *((size_t*)realptr);
	/* Assume at least that all the allocations are padded at sizeof(long) by
	 * the underlying allocator. */
	if (size&(sizeof(long) - 1)) size += sizeof(long) - (size&(sizeof(long) - 1));
	return size + PREFIX_SIZE;
}
size_t zmalloc_usable(void *ptr) {
	return zmalloc_size(ptr) - PREFIX_SIZE;
}
#endif

void zfree(void *ptr) {
#ifndef HAVE_MALLOC_SIZE
	void *realptr;
	size_t oldsize;
#endif

	if (ptr == NULL) return;
#ifdef HAVE_MALLOC_SIZE
	update_zmalloc_stat_free(zmalloc_size(ptr));
	free(ptr);
#else
	realptr = (char*)ptr - PREFIX_SIZE;
	oldsize = *((size_t*)realptr);
	update_zmalloc_stat_free(oldsize + PREFIX_SIZE);
	free(realptr);
#endif
}

char *zstrdup(const char *s) {
	size_t l = strlen(s) + 1;
	char *p = (char*)zmalloc(l);

	memcpy(p, s, l);
	return p;
}

size_t zmalloc_used_memory(void) {
	size_t um;
	atomicGet(used_memory, um);
	return um;
}

void zmalloc_set_oom_handler(void(*oom_handler)(size_t)) {
	zmalloc_oom_handler = oom_handler;
}

/* Get the RSS information in an OS-specific way.
 *
 * WARNING: the function zmalloc_get_rss() is not designed to be fast
 * and may not be called in the busy loops where Redis tries to release
 * memory expiring or swapping out objects.
 *
 * For this kind of "fast RSS reporting" usages use instead the
 * function RedisEstimateRSS() that is a much faster (and less precise)
 * version of the function. */

#if defined(HAVE_PROC_STAT)
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

size_t zmalloc_get_rss(void) {
	int page = sysconf(_SC_PAGESIZE);
	size_t rss;
	char buf[4096];
	char filename[256];
	int fd, count;
	char *p, *x;

	snprintf(filename, 256, "/proc/%d/stat", getpid());
	if ((fd = open(filename, O_RDONLY)) == -1) return 0;
	if (read(fd, buf, 4096) <= 0) {
		close(fd);
		return 0;
	}
	close(fd);

	p = buf;
	count = 23; /* RSS is the 24th field in /proc/<pid>/stat */
	while (p && count--) {
		p = strchr(p, ' ');
		if (p) p++;
	}
	if (!p) return 0;
	x = strchr(p, ' ');
	if (!x) return 0;
	*x = '\0';

	rss = strtoll(p, NULL, 10);
	rss *= page;
	return rss;
}
#elif defined(HAVE_TASKINFO)
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <mach/task.h>
#include <mach/mach_init.h>

size_t zmalloc_get_rss(void) {
	task_t task = MACH_PORT_NULL;
	struct task_basic_info t_info;
	mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

	if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS)
		return 0;
	task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);

	return t_info.resident_size;
}
#else
size_t zmalloc_get_rss(void) {
	/* If we can't get the RSS in an OS-specific way for this system just
	 * return the memory usage we estimated in zmalloc()..
	 *
	 * Fragmentation will appear to be always 1 (no fragmentation)
	 * of course... */
	return zmalloc_used_memory();
}
#endif

#if defined(USE_JEMALLOC)
int zmalloc_get_allocator_info(size_t *allocated,
	size_t *active,
	size_t *resident) {
	uint64_t epoch = 1;
	size_t sz;
	*allocated = *resident = *active = 0;
	/* Update the statistics cached by mallctl. */
	sz = sizeof(epoch);
	je_mallctl("epoch", &epoch, &sz, &epoch, sz);
	sz = sizeof(size_t);
	/* Unlike RSS, this does not include RSS from shared libraries and other non
	 * heap mappings. */
	je_mallctl("stats.resident", resident, &sz, NULL, 0);
	/* Unlike resident, this doesn't not include the pages jemalloc reserves
	 * for re-use (purge will clean that). */
	je_mallctl("stats.active", active, &sz, NULL, 0);
	/* Unlike zmalloc_used_memory, this matches the stats.resident by taking
	 * into account all allocations done by this process (not only zmalloc). */
	je_mallctl("stats.allocated", allocated, &sz, NULL, 0);
	return 1;
}
#else
int zmalloc_get_allocator_info(size_t *allocated,
	size_t *active,
	size_t *resident) {
	*allocated = *resident = *active = 0;
	return 1;
}
#endif

/* Get the sum of the specified field (converted form kb to bytes) in
 * /proc/self/smaps. The field must be specified with trailing ":" as it
 * apperas in the smaps output.
 *
 * If a pid is specified, the information is extracted for such a pid,
 * otherwise if pid is -1 the information is reported is about the
 * current process.
 *
 * Example: zmalloc_get_smap_bytes_by_field("Rss:",-1);
 */
#if defined(HAVE_PROC_SMAPS)
size_t zmalloc_get_smap_bytes_by_field(const char *field, long pid) {
	char line[1024];
	size_t bytes = 0;
	int flen = strlen(field);
	FILE *fp;

	if (pid == -1) {
		fp = fopen("/proc/self/smaps", "r");
	}
	else {
		char filename[128];
		snprintf(filename, sizeof(filename), "/proc/%ld/smaps", pid);
		fp = fopen(filename, "r");
	}

	if (!fp) return 0;
	while (fgets(line, sizeof(line), fp) != NULL) {
		if (strncmp(line, field, flen) == 0) {
			char *p = strchr(line, 'k');
			if (p) {
				*p = '\0';
				bytes += strtol(line + flen, NULL, 10) * 1024;
			}
		}
	}
	fclose(fp);
	return bytes;
}
#else
size_t zmalloc_get_smap_bytes_by_field(const char *field, long pid) {
	((void)field);
	((void)pid);
	return 0;
}
#endif

size_t zmalloc_get_private_dirty(long pid) {
	return zmalloc_get_smap_bytes_by_field("Private_Dirty:", pid);
}

/* Returns the size of physical memory (RAM) in bytes.
 * It looks ugly, but this is the cleanest way to achieve cross platform results.
 * Cleaned up from:
 *
 * http://nadeausoftware.com/articles/2012/09/c_c_tip_how_get_physical_memory_size_system
 *
 * Note that this function:
 * 1) Was released under the following CC attribution license:
 *    http://creativecommons.org/licenses/by/3.0/deed.en_US.
 * 2) Was originally implemented by David Robert Nadeau.
 * 3) Was modified for Redis by Matt Stancliff.
 * 4) This note exists in order to comply with the original license.
 */
size_t zmalloc_get_memory_size(void) {
#if defined(__unix__) || defined(__unix) || defined(unix) || \
    (defined(__APPLE__) && defined(__MACH__))
#if defined(CTL_HW) && (defined(HW_MEMSIZE) || defined(HW_PHYSMEM64))
	int mib[2];
	mib[0] = CTL_HW;
#if defined(HW_MEMSIZE)
	mib[1] = HW_MEMSIZE;            /* OSX. --------------------- */
#elif defined(HW_PHYSMEM64)
	mib[1] = HW_PHYSMEM64;          /* NetBSD, OpenBSD. --------- */
#endif
	int64_t size = 0;               /* 64-bit */
	size_t len = sizeof(size);
	if (sysctl(mib, 2, &size, &len, NULL, 0) == 0)
		return (size_t)size;
	return 0L;          /* Failed? */

#elif defined(_SC_PHYS_PAGES) && defined(_SC_PAGESIZE)
	/* FreeBSD, Linux, OpenBSD, and Solaris. -------------------- */
	return (size_t)sysconf(_SC_PHYS_PAGES) * (size_t)sysconf(_SC_PAGESIZE);

#elif defined(CTL_HW) && (defined(HW_PHYSMEM) || defined(HW_REALMEM))
	/* DragonFly BSD, FreeBSD, NetBSD, OpenBSD, and OSX. -------- */
	int mib[2];
	mib[0] = CTL_HW;
#if defined(HW_REALMEM)
	mib[1] = HW_REALMEM;        /* FreeBSD. ----------------- */
#elif defined(HW_PHYSMEM)
	mib[1] = HW_PHYSMEM;        /* Others. ------------------ */
#endif
	unsigned int size = 0;      /* 32-bit */
	size_t len = sizeof(size);
	if (sysctl(mib, 2, &size, &len, NULL, 0) == 0)
		return (size_t)size;
	return 0L;          /* Failed? */
#else
	return 0L;          /* Unknown method to get the data. */
#endif
#else
	return 0L;          /* Unknown OS. */
#endif
}


