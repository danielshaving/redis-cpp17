#ifndef _HELP_H_
#define _HELP_H_

#define TRACE(_s_, ...) do{\
        char szMsg[1024];\
        snprintf(szMsg, sizeof(szMsg)-1, _s_, ##__VA_ARGS__);\
        szMsg[1023] = 0;\
        printf("[TRACE]%s......[%s:%s():%d]\n", szMsg, __FILE__, __FUNCTION__, __LINE__);\
    }while(0)

#define TRACE_WARN(_s_, ...) do{\
        char szMsg[1024];\
        snprintf(szMsg, sizeof(szMsg)-1, _s_, ##__VA_ARGS__);\
        szMsg[1023] = 0;\
        printf("[TRACE_WARN]%s......[%s:%s():%d]\n", szMsg, __FILE__, __FUNCTION__, __LINE__);\
    }while(0)

#define TRACE_ERR(_s_, ...) do{\
        char szMsg[1024];\
        snprintf(szMsg, sizeof(szMsg)-1, _s_, ##__VA_ARGS__);\
        szMsg[1023] = 0;\
        printf("[TRACE_ERR]%s......[%s:%s():%d]\n", szMsg, __FILE__, __FUNCTION__, __LINE__);\
    }while(0)

#define TRACE_RUN(_s_, ...) do{\
        char szMsg[1024];\
        snprintf(szMsg, sizeof(szMsg)-1, _s_, ##__VA_ARGS__);\
        szMsg[1023] = 0;\
        printf("[TRACE_ERR]%s......[%s:%s():%d]\n", szMsg, __FILE__, __FUNCTION__, __LINE__);\
    }while(0)

#define SAFE_DELETE(p)  if (p != NULL ) { delete p; p = NULL;}
#define SAFE_DELETE_ARRAY(p)  if (p != NULL ) { delete[] p; p = NULL;}


#endif