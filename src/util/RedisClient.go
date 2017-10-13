package util

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"fmt"
	"set"
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
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	_,setx_err := conn.Do("SETEX",key,seconds,value)
	if setx_err != nil {
		panic(setx_err)
	}
}


func (client *RedisClient) Expire(redisInstance string,dbId int,key string,seconds int){
	conn := client.GetConnection(redisInstance)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	_,expire_err := conn.Do("EXPIRE",key,seconds)
	if expire_err != nil {
		panic(expire_err)
	}
}

func (client *RedisClient) Hmset(redisInstance string,dbId int,key string,dict map[string]string){
	conn := client.GetConnection(redisInstance)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	_,hmset_err := conn.Do("HMSET",dict)
	if hmset_err != nil {
		panic(hmset_err)
	}
}

func (client *RedisClient) Lset(redisInstance string,dbId int, key string,list []string){
	conn := client.GetConnection(redisInstance)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	args := []interface{}{key}
	for _,v := range list {
		args = append(args,v)
	}
	_,lset_err := conn.Do("LPUSH",args)
	if lset_err != nil {
		panic(lset_err)
	}
}

func (client *RedisClient) LsetByPipeline(redisInstance string,dbId int,resultMap map[string]([]string),expireTime int){
	conn := client.GetConnection(redisInstance)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
    r := NewRunner(conn)
	for key,value := range resultMap {
		r.send <- command{name: "LPUSH", args:[]interface{}{key,value},result:make(chan result,1)}
		if expireTime > 0 {
			r.send <- command{name: "EXPIRE", args: []interface{}{key,expireTime},result:make(chan result,1)}
		}
	}
	close(r.stop)
	<-r.done
}

func (client *RedisClient) HmsetByPipeline(redisInstance string,dbId int,resultMap map[string](map[string]string),expireTime int){
	conn := client.GetConnection(redisInstance)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	r := NewRunner(conn)
	for key,value := range resultMap {
		args := []interface{}{key}
		for k,v := range value {
			args = append(args,k,v)
		}
		r.send <- command{name: "HMSET", args:args,result:make(chan result,1)}
		if expireTime > 0 {
			r.send <- command{name: "EXPIRE", args: []interface{}{key,expireTime},result:make(chan result,1)}
		}
	}
	close(r.stop)
	<-r.done
}

func (client *RedisClient) LGetAllTargetAdWithPipeLine(dbId int,keys set.Set) (result set.Set) {
	conn := client.GetConnection(REDIS_DM)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	r := NewRunner(conn)
	for _,key := range keys.Elements(){
		r.send <- command{name:"LRANGE",args:[]interface{}{key,0,-1},result:make(chan result,1)}
	}
	close(r.stop)
	<-r.done
	for _,v := range r.last{
		result.Add(v.(string))
	}
	return result
}

func (client *RedisClient) Get(redisInstance string,dbId int,key string) string{
	conn := client.GetConnection(redisInstance)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId); select_err != nil {
		panic(select_err)
	}
	value,err := redis.String(conn.Do("SELECT",key))
	if err != nil {
		panic(err)
	}
	return value
}

func (client *RedisClient) HGetAll(redisInstance string,dbId int,key string) map[string]string{
	conn := client.GetConnection(redisInstance)
	defer func() {
		client.ReturnConn(conn)
	}()
	defer func() {
		if err := recover(); err != nil {
			fmt.Fprintln(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	values,err := redis.StringMap(conn.Do("HGETALL",key))
	if err != nil {
		panic(err)
	}
	return values
}









