/*
 * Copyright (c) 2016-present, Przemyslaw Skibinski, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */

#if defined (__cplusplus)
extern "C" {
#endif


/*-****************************************
*  Dependencies
******************************************/
#include "debug.h"
#include "util.h"       /* note : ensure that platform.h is included first ! */
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <time.h>

#if defined(_MSC_VER) || defined(__MINGW32__) || defined (__MSVCRT__)
#include <direct.h>     /* needed for _mkdir in windows */
#endif

/*-****************************************
*  count the number of physical cores
******************************************/

#if defined(_WIN32) || defined(WIN32)

#include <windows.h>

typedef BOOL(WINAPI* LPFN_GLPI)(PSYSTEM_LOGICAL_PROCESSOR_INFORMATION, PDWORD);

int UTIL_countAvailableCores(void)
{
    static int numLogicalCores = 0;
    static time_t lastTimeCached = 0;
    time_t currTime = time(NULL);
    int cachettl = 60;
    if (lastTimeCached && currTime-lastTimeCached>cachettl) numLogicalCores = 0;
    if (numLogicalCores != 0) return numLogicalCores;

    {   LPFN_GLPI glpi;
        BOOL done = FALSE;
        PSYSTEM_LOGICAL_PROCESSOR_INFORMATION buffer = NULL;
        PSYSTEM_LOGICAL_PROCESSOR_INFORMATION ptr = NULL;
        DWORD returnLength = 0;
        size_t byteOffset = 0;

#if defined(_MSC_VER)
/* Visual Studio does not like the following cast */
#   pragma warning( disable : 4054 )  /* conversion from function ptr to data ptr */
#   pragma warning( disable : 4055 )  /* conversion from data ptr to function ptr */
#endif
        glpi = (LPFN_GLPI)(void*)GetProcAddress(GetModuleHandle(TEXT("kernel32")),
                                               "GetLogicalProcessorInformation");

        if (glpi == NULL) {
            goto failed;
        }

        while(!done) {
            DWORD rc = glpi(buffer, &returnLength);
            if (FALSE == rc) {
                if (GetLastError() == ERROR_INSUFFICIENT_BUFFER) {
                    if (buffer)
                        free(buffer);
                    buffer = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION)malloc(returnLength);

                    if (buffer == NULL) {
                        perror("zstd");
                        exit(1);
                    }
                } else {
                    /* some other error */
                    goto failed;
                }
            } else {
                done = TRUE;
            }
        }

        ptr = buffer;

        while (byteOffset + sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION) <= returnLength) {

            if (ptr->Relationship == RelationProcessorCore) {
                numLogicalCores++;
            }

            ptr++;
            byteOffset += sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION);
        }

        free(buffer);
        lastTimeCached = time(NULL);
        return numLogicalCores;
    }

failed:
    /* try to fall back on GetSystemInfo */
    {   SYSTEM_INFO sysinfo;
        GetSystemInfo(&sysinfo);
        numLogicalCores = sysinfo.dwNumberOfProcessors;
        if (numLogicalCores == 0) numLogicalCores = 1; /* just in case */
    }
    lastTimeCached = time(NULL);
    return numLogicalCores;
}

#elif defined(__APPLE__)

#include <sys/sysctl.h>

/* Use apple-provided syscall
 * see: man 3 sysctl */
int UTIL_countAvailableCores(void)
{
    static int32_t numLogicalCores = 0; /* apple specifies int32_t */
    static time_t lastTimeCached = 0;
    time_t currTime = time(NULL);
    int cachettl = 60;
    if (lastTimeCached && currTime-lastTimeCached>cachettl) numLogicalCores = 0;
    if (numLogicalCores != 0) return numLogicalCores;

    {   size_t size = sizeof(int32_t);
        int const ret = sysctlbyname("hw.physicalcpu", &numLogicalCores, &size, NULL, 0);
        if (ret != 0) {
            if (errno == ENOENT) {
                /* entry not present, fall back on 1 */
                numLogicalCores = 1;
            } else {
                perror("zstd: can't get number of physical cpus");
                exit(1);
            }
        }

        lastTimeCached = time(NULL);
        return numLogicalCores;
    }
}

#elif defined(__linux__)

/* parse /proc/cpuinfo
 * siblings / cpu cores should give hyperthreading ratio
 * otherwise fall back on sysconf */
int UTIL_countAvailableCores(void)
{
    static int numLogicalCores = 0;
    static time_t lastTimeCached = 0;
    time_t currTime = time(NULL);
    int cachettl = 60;
    if (lastTimeCached && currTime-lastTimeCached>cachettl) numLogicalCores = 0;

    if (numLogicalCores != 0) {
        printdn("Stored static numLogicalCores: %d\n", numLogicalCores);
        return numLogicalCores;
    }

    numLogicalCores = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (numLogicalCores == -1) {
        /* value not queryable, fall back on 1 */
        printdn("Sysconf read fail. numLogicalCores: %d\n", numLogicalCores);
        lastTimeCached = time(NULL);
        return numLogicalCores = 1;
    }
	printdn("Sysconf readed. numLogicalCores: %d\n", numLogicalCores);

    /* try to determine if there's hyperthreading */
    {   FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
#define BUF_SIZE 80
        char buff[BUF_SIZE];

        int siblings = 0;
        int cpu_cores = 0;
        int procs = 0;
        int ratio = 1;

        if (cpuinfo == NULL) {
            /* fall back on the sysconf value, fallback to 1 */
            printdn("Cpuinfo not open. numLogicalCores: %d\n", numLogicalCores);
            lastTimeCached = time(NULL);
            return numLogicalCores = 1;
        }

        /* assume the cpu cores/siblings values will be constant across all
         * present processors, in vm/containers lxc/openvz it shows all physical cores/threads */
        while (!feof(cpuinfo)) {
            if (fgets(buff, BUF_SIZE, cpuinfo) != NULL) {
                if (strncmp(buff, "siblings", 8) == 0) {
                    const char* const sep = strchr(buff, ':');
                    if (sep == NULL || *sep == '\0') {
                        /* formatting was broken? */
                        goto failed;
                    }

                    siblings = atoi(sep + 1);
                    printdn("Cpuinfo: got siblings: %d\n", siblings);
                    break; // first found - go away
                }
                // here are stored count of physical cores
                if (strncmp(buff, "cpu cores", 9) == 0) {
                    const char* const sep = strchr(buff, ':');
                    if (sep == NULL || *sep == '\0') {
                        /* formatting was broken? */
                        goto failed;
                    }

                    cpu_cores = atoi(sep + 1);
                    printdn("Cpuinfo: got cpu-cores: %d\n", cpu_cores);
                    break; // first found - go away
                }
                // just do stupid line counting
                if (strncmp(buff, "processor", 9) == 0) {
                    const char* const sep = strchr(buff, ':');
                    if (sep == NULL || *sep == '\0') {
                        /* formatting was broken? */
                        goto failed;
                    }

                    procs++;
                }
            } else if (ferror(cpuinfo)) {
                /* fall back on the sysconf value */
                goto failed;
            }
        }
        if (siblings && cpu_cores) {
            ratio = siblings / cpu_cores;
        }
        fclose(cpuinfo); cpuinfo = NULL;
        if (procs){
            printdn("Cpuinfo found processor lines: %d\n", procs);
            lastTimeCached = time(NULL);
            return numLogicalCores = procs;
        }
failed:
        if (cpuinfo){ fclose(cpuinfo); cpuinfo = NULL;}
        lastTimeCached = time(NULL);
        return numLogicalCores = numLogicalCores / ratio;
    }
}

#elif defined(__FreeBSD__)

#include <sys/param.h>
#include <sys/sysctl.h>

/* Use physical core sysctl when available
 * see: man 4 smp, man 3 sysctl */
int UTIL_countAvailableCores(void)
{
    static int numLogicalCores = 0; /* freebsd sysctl is native int sized */
    static time_t lastTimeCached = 0;
    time_t currTime = time(NULL);
    int cachettl = 60;
    if (lastTimeCached && currTime-lastTimeCached>cachettl) numLogicalCores = 0;

    if (numLogicalCores != 0) return numLogicalCores;

#if __FreeBSD_version >= 1300008
    {   size_t size = sizeof(numLogicalCores);
        int ret = sysctlbyname("kern.smp.cores", &numLogicalCores, &size, NULL, 0);
        if (ret == 0) return numLogicalCores;
        if (errno != ENOENT) {
            perror("zstd: can't get number of Logical cpus");
            exit(1);
        }
        /* sysctl not present, fall through to older sysconf method */
    }
#endif

    numLogicalCores = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (numLogicalCores == -1) {
        /* value not queryable, fall back on 1 */
        numLogicalCores = 1;
    }
    lastTimeCached = time(NULL);
    return numLogicalCores;
}

#elif defined(__NetBSD__) || defined(__OpenBSD__) || defined(__DragonFly__)

/* Use POSIX sysconf
 * see: man 3 sysconf */
int UTIL_countAvailableCores(void)
{
    static int numLogicalCores = 0;
    static time_t lastTimeCached = 0;
    time_t currTime = time(NULL);
    int cachettl = 60;
    if (lastTimeCached && currTime-lastTimeCached>cachettl) numLogicalCores = 0;

    if (numLogicalCores != 0) return numLogicalCores;

    numLogicalCores = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (numLogicalCores == -1) {
        /* value not queryable, fall back on 1 */
        return numLogicalCores = 1;
    }
    lastTimeCached = time(NULL);
    return numLogicalCores;
}

#else

int UTIL_countAvailableCores(void)
{
    /* assume 1 */
    return 1;
}

#endif

#if defined (__cplusplus)
}
#endif
