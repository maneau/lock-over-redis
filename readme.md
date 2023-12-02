Lock Over Redis
=======================

## Origin of the project

Synchronizing many JVM is not easy specially to accessing to an none transactional system as filesystem, search engine.
In some cases even database locks can be limited because it waiting for locks to be release. In this case, no waiting time. 
If your lock cannot be place, then you now it immediately. Very usefull for database table used for jobs. 

Finally lock are ephemera and they vanish by itself.

## How to use it
 
Take a look on the Unit Tests LockFactoryTest for exemple. 

LockFactory can be initiate with a Jedis or a JedisPool.

> JedisPool pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, 20000);
> LockFactory lockFactory = new LockFactory(pool);

Note that you can initiate using Jedis but it can not be fast enough for multi-thread environment.  

The you just need to create a lock

> LockFactory.MyLock myLock = lockFactory.getLock("lock collection name", "id of the lock");

Then do what you want this your lock 
 - tryLock() is used for adding a lock (it return false when operation failed) 
 - tryUnlock()
 - expire()
 - getTTL() 
 - isLocked() to check if my lock is valid

## Docker

Some usefull command for starting docker redis :
```
docker run --rm -p 6379:6379 redis
```
 
## Very powerfull

It can manage over 600 locks per seconds and even in rough situation no conflicts detected.

By Maneau(c)