
#pragma once
#include "all.h"


class MutexLock : noncopyable
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

public:
  pthread_mutex_t mutex;
 
};



class MutexLockGuard : noncopyable
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


class CondVar
{
public:
	explicit CondVar(MutexLock *mu): mu(mu)
	{
		pthread_cond_init(&cv, nullptr);
	}
	~CondVar()
	{
		pthread_cond_destroy(&cv); 
	}
	void Wait()
	{
		pthread_cond_wait(&cv, &mu->mutex);
	}
	void Signal()
	{
		pthread_cond_signal(&cv);
	}
	void SignalAll()
	{
		pthread_cond_broadcast(&cv);
	}
	
private:
	pthread_cond_t cv;
	MutexLock *mu;	
};

