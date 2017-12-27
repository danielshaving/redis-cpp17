#xredis

xredis is a compatible redis protocol with multithreaded client server


#Performace

single core absolute fair environment  less than 100k set get bench: redis overall performance faster than xredis 10 - 25% range qps 
（because of the memory management cost of using STL and xredis thread safe） kernel bottleneck 80-90% cpu benchmark.pdf 

#How to build

```console
`cd xredis  mkdir obj  cd ./src/`    
make ./redis-server
```


#Further reading
detailed test method reference redis benchmark
  
https://redis.io/topics/benchmarks
 
https://stackoverflow.com/questions/2873249/is-memcached-a-dinosaur-in-comparison-to-redis 

