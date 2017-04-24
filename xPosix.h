#ifndef _XMUTEX_LOCK_H_
#define _XMUTEX_LOCK_H_
#include "all.h"


class MutexLock : boost::noncopyable
{
 public:
  MutexLock()
  {
    pthread_mutex_init(&mutex,nullptr);
  }

  ~MutexLock()
  {
    pthread_mutex_destroy(&mutex);
  }

 
  void lock()
  {
    pthread_mutex_lock(&mutex);
   
  }

  void unlock()
  {
    pthread_mutex_unlock(&mutex);
  }

  pthread_mutex_t* getPthreadMutex() /* non-const */
  {
    return &mutex;
  }

private:
  pthread_mutex_t mutex;
 
};



class MutexLockGuard : boost::noncopyable
{
 public:
  explicit MutexLockGuard(MutexLock& mutex)
    : mutex(mutex)
  {
    mutex.lock();
  }

  ~MutexLockGuard()
  {
    mutex.unlock();
  }

 
 void lock()
 {
   mutex.lock();
  
 }
 
 void unlock()
 {
   mutex.unlock();
 }
 

 private:

  MutexLock& mutex;
};


#endif
