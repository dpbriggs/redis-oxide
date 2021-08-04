#!/bin/bash
redis-benchmark -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset,spop,zadd,zpopmin,lrange,lrange_100,lrange_300,lrange_500,lrange_600,lrange,lrange_100,lrange,lrange_300,lrange,lrange_500,lrange,lrange_600,mset
