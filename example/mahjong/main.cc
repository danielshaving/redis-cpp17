#include <iostream>
#include <vector>
#include <algorithm> // std::move_backward
#include <random> // std::default_random_engine
#include <chrono> // std::chrono::system_clock
#include <map>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

char getRemainRand(std::map<char,char> &brand)
{
	char remain = 0;
	for(auto &it : brand)
	{
		remain += it.second;
	}
	return remain;
}

bool huBrand(std::map<char,char> &brand,bool will = false)
{
	char remain = getRemainRand(brand);
	if(remain <= 0 )
	{
		return true;
	}

	for(auto it = brand.begin(); it != brand.end(); ++it)
	{
		if(it->second == 0)
		{
			continue;
		}

		if(it->second >= 3)
		{
			it->second -= 3;
			if(huBrand(brand,will))
			{
				it->second += 3;
				return true;
			}
			it->second += 3;
		}

		if(!will && it->second >= 2)
		{
			will = true;
			it->second -= 2;
			if(huBrand(brand,will))
			{
				it->second += 2;
				return true;
			}
			it->second += 2;
			will = false;
		}

		if(it->first > 30)
		{
			return false;
		}

		if ((it->first % 10 != 8) && (it->first % 10 != 9))
		{
			auto nextIter = it;
			auto next1 = ++nextIter;
			if(next1 == brand.end() || next1->second <= 0)
			{
				return false;
			}

			auto next2 = ++nextIter;
			if(next2 == brand.end() || next2->second <= 0)
			{
				return false;
			}

			char node1 = it->first;
			char node2 = next1->first;
			char node3 = next2->first;

			if(++node1 != node2)
			{
				return false;
			}

			if(++node2 != node3)
			{
				return false;
			}

			it->second--;
			next1->second--;
			next2->second--;
			if(huBrand(brand,will))
			{
				it->second++;
				next1->second++;
				next2->second++;
				return true;
			}
			it->second++;
			next1->second++;
			next2->second++;
		}
	}
	return false;
}

int64_t ustime(void)
{
	struct timeval tv;
	int64_t ust;

	gettimeofday(&tv, nullptr);
	ust = ((int64_t)tv.tv_sec)*1000000;
	ust += tv.tv_usec;
	return ust;
}

int64_t mstime(void)
{
	return ustime()/1000;
}

int main()
{
	std::vector<char> brands;
	for(int i = 1; i <= 9; i++)
	{
		for(int j = 1; j <= 4; j++)
		{
			brands.push_back(i);
		}
	}

	for(int i = 11; i <= 19; i++)
	{
		for(int j = 1; j <= 4; j++)
		{
			brands.push_back(i);
		}
	}

	for(int i = 21; i <= 29; i++)
	{
		for(int j = 1; j <= 4; j++)
		{
			brands.push_back(i);
		}
	}

	for(int i = 31; i <= 37; i++)
	{
		for(int j = 1; j <= 4; j++)
		{
			brands.push_back(i);
		}
	}

	srand(time(0));
	std::map<char,char> handBrands;
	int64_t startTime = ustime();
	int i = 0;
	const int k = 500000;
	for(i = 0; i < k; i++)
	{
		random_shuffle(brands.begin(),brands.end());
		for(int j = 0; j < 14; j++)
		{
			handBrands[brands[j]]++;
		}

		huBrand(handBrands);
		handBrands.clear();
	}

	int64_t endTime = ustime();
	double dff = endTime - startTime;
	double elapsed = dff / (1000 * 1000);
	printf("%.3f seconds %ld\n",elapsed,k);
	return 0;
}
