single core absolute fair environment  less than 100k set get bench: redis overall performance faster than xredis 10 - 25% range qps 
（because of the memory management cost of using STL,the smaller the memory, the greater the gap） kernel bottleneck 80-90% cpu benchmark.pdf 

cd xredis  mkdir obj  cd ./src/    
make 
./redis-server 0.0.0.0 6379 0 0 

xredis is a compatible redis protocol with multithreaded client server

detailed test method reference redis benchmark  
https://redis.io/topics/benchmarks 
https://stackoverflow.com/questions/2873249/is-memcached-a-dinosaur-in-comparison-to-redis 

