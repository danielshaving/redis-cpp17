#include "xSortSet.h"

void echo_ranking(std::pair<int, double>& i)
{
    std::cout << "Key :" << i.first << " Score:" << i.second << std::endl;
}

int main()
{
	unsigned long rank;
    double score;
    xSortedSet<int> sortedSet;
    sortedSet.zadd(300001, 300);
    sortedSet.zadd(300002, 299.9);
    sortedSet.zadd(300003, 100000);

    std::vector< std::pair<int, double> > result;

    sortedSet.zrange_withscores(0, -1, result);
    std::for_each(result.begin(), result.end(), echo_ranking);
    std::cout << std::endl;

    sortedSet.zadd(300004, 40);
    sortedSet.zrange_withscores(0, -1, result);
    std::for_each(result.begin(), result.end(), echo_ranking);
    std::cout << std::endl;

    sortedSet.zadd(300005, 40);
    sortedSet.zrange_withscores(0, -1, result);
    std::for_each(result.begin(), result.end(), echo_ranking);
    std::cout << std::endl;

    std::cout << sortedSet.zcount(40, 300, true, true) << std::endl;
    std::cout << std::endl;

    sortedSet.zrem(300005);
    sortedSet.zrange_withscores(0, -1, result);
    std::for_each(result.begin(), result.end(), echo_ranking);
    std::cout << std::endl;

    sortedSet.zincrby(300004, 500);
    sortedSet.zrange_withscores(0, -1, result);
    std::for_each(result.begin(), result.end(), echo_ranking);
    std::cout << std::endl;

    if (sortedSet.zscore(300003, score))
        std::cout << score << std::endl;
    else
        std::cout << "nil" << std::endl;
    std::cout << std::endl;

    if (sortedSet.zrank(300002, rank))
        std::cout << rank << std::endl;
    else
        std::cout << "nil" << std::endl;
    if (sortedSet.zrevrank(300002, rank))
        std::cout << rank << std::endl;
    else
        std::cout << "nil" << std::endl;
    std::cout << std::endl;

    sortedSet.zrevrange_withscores(0, 1, result);
    std::for_each(result.begin(), result.end(), echo_ranking);
    std::cout << std::endl;

    sortedSet.zrangebyscore_withscores(300, 1000, result);
    std::for_each(result.begin(), result.end(), echo_ranking);
    std::cout << std::endl;

    sortedSet.zremrangebyrank(1, 2);
    sortedSet.zrange_withscores(0, -1, result);
    std::for_each(result.begin(), result.end(), echo_ranking);
    std::cout << std::endl;
	return 0;
}