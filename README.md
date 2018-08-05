
fragment time(minute) single core absolute fair environment  less than 100k set get bench: redis overall performance faster than redis-cpp17 10 - 25% range qps 
（because of the memory management cost of using STL and redis-cpp17 thread safe） kernel bottleneck 80-90% cpu benchmark.pdf 

cd redis-cpp17/src/    
make ./redis-server

detailed test method reference redis benchmark  
https://redis.io/topics/benchmarks 
https://stackoverflow.com/questions/2873249/is-memcached-a-dinosaur-in-comparison-to-redis 

redis-cpp17 qq group: 324186207   qq 474711079
