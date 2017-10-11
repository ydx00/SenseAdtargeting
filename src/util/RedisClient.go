package util

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"fmt"
	"set"
	"git.apache.org/thrift.git/lib/go/thrift"
	"log"
)

type RedisClient struct {
	senseARPool     *redis.Pool
	bdpOfflinePool  *redis.Pool
	bdpRealtimePool *redis.Pool
	dmPool          *redis.Pool
}

/**
  redis配置
 */
const(
	senseARRedisServer = "10.0.8.81"
	senseARRedisPort = 6379
	senseARRedisTimeout = 30
	senseARRedisPass = ""

	bdpOfflineRedisServer = "10.0.8.81"
	bdpOfflineRedisPort = 6379
	bdpOfflineRedisTimeout = 30
	bdpOfflineRedisPass = ""

	dmRedisServer = "10.0.8.81"
	dmRedisPort = 6379
	dmRedisTimeout = 30
	dmRedisPass = ""

	bdpRealtimeRedisServer = "10.0.8.81"
	bdpRealtimeRedisPort = 6379
	bdpRealtimeRedisTimeout = 30
	bdpRealtimeRedisPass = ""

	//redis配置信息
	redisMaxTotal= 1024
	redisMaxIdle=200
    redisMaxWaitTime=30

    //redis连接池标志
	REDIS_SENSEAR = "SENSEAR"
	REDIS_BDP_OFFLINE = "BDP_OFFLINE"
	REDIS_BDP_REALTIME = "BDP_REALTIME"
	REDIS_DM = "DM"
)

/**
   新建连接池
 */
func newPool(server string,port int,password string,timeout int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     redisMaxIdle,
		MaxActive:   redisMaxTotal,
		IdleTimeout: redisMaxWaitTime * time.Second,


		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server + ":" + string(port))
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", password); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		    },

		   TestOnBorrow: func(c redis.Conn, t time.Time) error {
			   if time.Since(t).Seconds() < float64(timeout) {
			   	 return nil
			   }
			   _, err := c.Do("PING")
				return err
		   },
		}
}

/**
   新建redis客户端
 */
func NewRedisClient() *RedisClient{
    return &RedisClient{
    	senseARPool:newPool(senseARRedisServer,senseARRedisPort,senseARRedisPass,senseARRedisTimeout),
    	bdpOfflinePool:newPool(bdpOfflineRedisServer,bdpOfflineRedisPort,bdpOfflineRedisPass,bdpOfflineRedisTimeout),
    	bdpRealtimePool:newPool(bdpRealtimeRedisServer,bdpRealtimeRedisPort,bdpRealtimeRedisPass,bdpRealtimeRedisTimeout),
    	dmPool:newPool(dmRedisServer,dmRedisPort,dmRedisPass,dmRedisTimeout),
	}
}

/**
	* 将 Redis 连接返回到 Pool 中
	* @param conn 返回到 Pool 的连接
	*/
func (client *RedisClient) ReturnConn(conn redis.Conn){
	if conn != nil {
       err := conn.Close()
		if err != nil {
			panic("redis连接归还失败")
		}
	}
}

/**
	* 从 Pool 中 Borrow 连接
	* @return Redis 连接对象
*/
func (client *RedisClient) GetConnection(redisInstance string) (conn redis.Conn){
	switch redisInstance {
		case REDIS_SENSEAR:
			conn = client.senseARPool.Get()
		case REDIS_BDP_OFFLINE:
		    conn = client.bdpOfflinePool.Get()
	    case REDIS_BDP_REALTIME:
		    conn = client.bdpRealtimePool.Get()
	    case REDIS_DM:
		    conn = client.dmPool.Get()
	    default:
			conn = nil
	}
	return conn
}

/**
	* 向 redis 数据库添加值（带超时设置）
	* @param dbId
	* @param key
	* @param value
	* @return 是否成功
	*/
func (client *RedisClient) Add(redisInstance string,dbId int,key string,value string,seconds int){
	conn := client.GetConnection(redisInstance)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	_,select_err := conn.Do("SELECT",dbId)
	if select_err != nil {
		panic(select_err)
	}
	_,setx_err := conn.Do("SETEX",key,seconds,value)
	if setx_err != nil {
		panic(setx_err)
	}
}


func (client *RedisClient) Expire(redisInstance string,dbId int,key string,seconds int){
	conn := client.GetConnection(redisInstance)
	_,select_err := conn.Do("SELECT",dbId)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if select_err != nil {
		panic(select_err)
	}
	_,expire_err := conn.Do("EXPIRE",key,seconds)
	if expire_err != nil {
		panic(expire_err)
	}
}

func (client *RedisClient) Hmset(redisInstance string,dbId int,key string,dict map[string]string){
	conn := client.GetConnection(redisInstance)
	_,select_err := conn.Do("SELECT",dbId)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if select_err != nil {
		panic(select_err)
	}
	_,hmset_err := conn.Do("HMSET",key,dict)
	if hmset_err != nil {
		panic(hmset_err)
	}
}

func (client *RedisClient) Lset(redisInstance string,dbId int, key string,list []string){
	conn := client.GetConnection(redisInstance)
	_,select_err := conn.Do("SELECT",dbId)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if select_err != nil {
		panic(select_err)
	}
	_,lset_err := conn.Do("LPUSH",key,list)
	if lset_err != nil {
		panic(lset_err)
	}
}

func (client *RedisClient) LsetByPipeline(redisInstance string,dbId int,resultMap map[string]([]string),expireTime int){
	conn := client.GetConnection(redisInstance)
	_,select_err := conn.Do("SELECT",dbId)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if select_err != nil {
		panic(select_err)
	}
	for key,value := range resultMap{
		if err := conn.Send("LPUSH",key,value); err != nil {
			panic(err)
		}
		if expireTime > 0 {
			if err := conn.Send("EXPIRE",key,expireTime); err != nil {
				panic(err)
			}
		}
	}
	if err := conn.Flush(); err != nil {
		panic(err)
	}
}

func (client *RedisClient) LGetAllTargetAdWhithPipeline(dbId int,keys set.Set){
	conn := client.GetConnection(REDIS_DM)
	_,select_err := conn.Do("SELECT",dbId)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if select_err != nil {
		panic(select_err)
	}
	//result := set.NewHashSet()
	for _,key := range keys.Elements(){
        conn.Send("LRANGE",key,0,-1)
	}
	err := conn.Flush()
	if err != nil {
		panic(err)
	}
	for i := 0; i < keys.Len(); i++ {
		reply,err := conn.Receive()
		if err != nil {
			panic(err)
		}

	}


}

