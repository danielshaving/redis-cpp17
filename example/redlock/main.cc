#include "redlock.h"

int main()
{
	RedLock dlm;
	dlm.syncAddServerUrl("127.0.0.1", 7000);
	dlm.syncAddServerUrl("127.0.0.1", 7001);
	dlm.syncAddServerUrl("127.0.0.1", 7002);

	while (1)
	{
		Lock myLock;
		bool flag = dlm.lock("foo", 100000, myLock);
		if (flag)
		{
			printf("get lock sucess, Acquired by client name:%s, res:%s, vttl:%d\n",
				myLock.val, myLock.resource, myLock.validityTime);
			dlm.unlock(myLock);
			usleep(500000);
		}
		else
		{
			printf("get lock failure, lock not acquired, name:%s\n", myLock.val);
			usleep(rand() % 3);
		}
	}
	return 0;
}
