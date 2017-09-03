single core absolute fair environment  less than 64k set get bench: redis overall performance faster than xredis 10 - 50% range qps （because of the memory management cost of using STL,the smaller the memory, the greater the gap）

cd xredis  mkdir obj  cd ./src/    
make 
./redis-server 0.0.0.0 6379 0 0 0

xredis is s c++  compatible with redis or hiredis_client multithreading  , in-memory data structure store, used as a database, cache  It supports data structures such as set get hset hget hkeys keys  hgetall dbsize flushdb sync slaveof save quit info auth config  zadd zrange sadd scard zrevragne publish unsubscribe subscribe

detailed test method reference redis benchmark 
https://redis.io/topics/benchmarks 
https://stackoverflow.com/questions/2873249/is-memcached-a-dinosaur-in-comparison-to-redis 

