package util

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"set"
	"log"
	"sync"
)

var redisInstance *RedisClient = nil
var redisOnce sync.Once


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
    //redis连接池标志
	REDIS_SENSEAR = "SENSEAR"
	REDIS_BDP_OFFLINE = "BDP_OFFLINE"
	REDIS_BDP_REALTIME = "BDP_REALTIME"
	REDIS_DM = "DM"
)

var senseARRedisServer = NewConfigHelper().ConfigMap["SENSEAR_REDIS_SERVER_HOST"]
var senseARRedisPort = StringToInt(NewConfigHelper().ConfigMap["SENSEAR_REDIS_SERVER_PORT"])
var senseARRedisTimeout = StringToInt(NewConfigHelper().ConfigMap["SENSEAR_REDIS_TIMEOUT"])
var senseARRedisPass = NewConfigHelper().ConfigMap["SENSEAR_REDIS_PASS"]

var bdpOfflineRedisServer = NewConfigHelper().ConfigMap["BDP_OFFLINE_REDIS_SERVER_HOST"]
var bdpOfflineRedisPort = StringToInt(NewConfigHelper().ConfigMap["BDP_OFFLINE_REDIS_SERVER_PORT"])
var bdpOfflineRedisTimeout = StringToInt(NewConfigHelper().ConfigMap["BDP_OFFLINE_REDIS_TIMEOUT"])
var bdpOfflineRedisPass = NewConfigHelper().ConfigMap["BDP_OFFLINE_REDIS_PASS"]

var dmRedisServer = NewConfigHelper().ConfigMap["BDP_REALTIME_REDIS_SERVER_HOST"]
var dmRedisPort = StringToInt(NewConfigHelper().ConfigMap["BDP_REALTIME_REDIS_SERVER_PORT"])
var dmRedisTimeout = StringToInt(NewConfigHelper().ConfigMap["BDP_REALTIME_REDIS_TIMEOUT"])
var dmRedisPass = NewConfigHelper().ConfigMap["BDP_REALTIME_REDIS_PASS"]

var bdpRealtimeRedisServer = NewConfigHelper().ConfigMap["DM_REDIS_SERVER_HOST"]
var bdpRealtimeRedisPort = StringToInt(NewConfigHelper().ConfigMap["DM_REDIS_SERVER_PORT"])
var bdpRealtimeRedisTimeout = StringToInt(NewConfigHelper().ConfigMap["DM_REDIS_TIMEOUT"])
var bdpRealtimeRedisPass = NewConfigHelper().ConfigMap["DM_REDIS_PASS"]

var redisMaxTotal= StringToInt(NewConfigHelper().ConfigMap["REDIS_MAX_TOTAL"])
var redisMaxIdle=StringToInt(NewConfigHelper().ConfigMap["REDIS_MAX_IDLE"])
var redisMaxWaitTime=StringToInt(NewConfigHelper().ConfigMap["REDIS_MAX_WAIT_MILLS"])


/**
   新建连接池
 */
func newPool(server string,port int,password string,timeout int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     redisMaxIdle,
		MaxActive:   redisMaxTotal,
		IdleTimeout: time.Duration(redisMaxWaitTime) * time.Second,

		Dial: func() (redis.Conn, error) {
			finalServer := server + ":" + IntToString(port)
			c, err := redis.Dial("tcp", finalServer)
			if err != nil {
				log.Println("failed to connect:",err)
				return nil, err
			}
			if password != ""{
				if _, err := c.Do("AUTH", password); err != nil {
					log.Println("密码验证失败",err)
					c.Close()
					return nil, err
				}
			}
			    return c, err
		    },
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			   if time.Since(t).Seconds() > float64(timeout) {
				   log.Println("测试连接超时")
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
	 redisOnce.Do(func() {
		 redisInstance = &RedisClient{
			 senseARPool:newPool(senseARRedisServer,senseARRedisPort,senseARRedisPass,senseARRedisTimeout),
			 bdpOfflinePool:newPool(bdpOfflineRedisServer,bdpOfflineRedisPort,bdpOfflineRedisPass,bdpOfflineRedisTimeout),
			 bdpRealtimePool:newPool(bdpRealtimeRedisServer,bdpRealtimeRedisPort,bdpRealtimeRedisPass,bdpRealtimeRedisTimeout),
			 dmPool:newPool(dmRedisServer,dmRedisPort,dmRedisPass,dmRedisTimeout),
		 }
	 })
    return redisInstance
}

/**
	* 将 Redis 连接返回到 Pool 中
	* @param conn 返回到 Pool 的连接
	*/
func (client *RedisClient) ReturnConn(conn redis.Conn){
	if conn != nil {
       err := conn.Close()
		if err != nil {
			log.Fatal("redis连接归还失败",err)
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

func (client *RedisClient) Expire(redisInstance string,dbId int,key string,seconds int){
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	if _,expire_err := conn.Do("EXPIRE",key,seconds);expire_err != nil{
		panic(expire_err)
	}
}

func (client *RedisClient) Hmset(redisInstance string,dbId int,key string,dict map[string]string){
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	if _,hmset_err := conn.Do("HMSET",redis.Args{}.Add(key).AddFlat(dict)...); hmset_err != nil{
		panic(hmset_err)
	}
}


func (client *RedisClient) LsetByPipeline(redisInstance string,dbId int,resultMap map[string]([]string),expireTime int){
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Fatal(err)
		}
	}()
	if _,err := conn.Do("SELECT",dbId);err != nil {
		panic(err)
	}
	for key,value := range resultMap{
		log.Println(key,value)
		pushArgs := redis.Args{}.Add(key).AddFlat(value)
		conn.Send("LPUSH",pushArgs...)
		if expireTime > 0{
			expireArgs := redis.Args{}.Add(key).AddFlat(expireTime)
			conn.Send("EXPIRE",expireArgs...)
		}
	}
	if err := conn.Flush(); err != nil{
		panic(err)
	}
	if _,err := conn.Receive(); err != nil {
		panic(err)
	}
}

func (client *RedisClient) HmsetByPipeline(redisInstance string,dbId int,resultMap map[string](map[string]string),expireTime int){
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,err := conn.Do("SELECT",dbId);err != nil {
		panic(err)
	}
    for key,value := range resultMap{
		args := redis.Args{}.Add(key).AddFlat(value)
		conn.Send("HMSET",args...)
		if expireTime > 0{
			expireArgs := redis.Args{}.Add(key).AddFlat(expireTime)
			conn.Send("EXPIRE",expireArgs...)
		}
	}
	if err := conn.Flush(); err != nil{
		panic(err)
	}
	if _,err := conn.Receive(); err != nil {
		panic(err)
	}
}

func (client *RedisClient) LGetAllTargetAdWithPipeLine(dbId int,keys set.Set) set.Set {
	conn := client.GetConnection(REDIS_DM)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,err := conn.Do("SELECT",dbId); err != nil {
		panic(err)
	}
    result := set.NewSimpleSet()
	conn.Send("MULTI")
	for _,key := range keys.Elements(){
		conn.Send("LRANGE",key,0,-1)
	}
	if err := conn.Flush(); err != nil{
		panic(err)
	}
	if pipe_prox ,err := conn.Do("EXEC"); err != nil {
		panic(err)
	}else {
		for _,value := range pipe_prox.([]interface{}){
			if res,err := redis.Strings(value,nil); err != nil{
				panic(err)
			}else {
				if len(res) > 0 {
					for _,item := range res{
						result.Add(item)
					}
				}
			}
		}
	}
	return result
}

func (client *RedisClient) Get(redisInstance string,dbId int,key string) string{
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,err := conn.Do("SELECT",dbId); err != nil {
		panic(err)
	}
	if value,err := redis.String(conn.Do("GET",key)); err != nil{
		panic(err)
	}else {
		return value
	}
}

func (client *RedisClient) HGetAll(redisInstance string,dbId int,key string) map[string]string{
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	if values,err := redis.StringMap(conn.Do("HGETALL",key)); err != nil{
		panic(err)
	}else {
		return values
	}
}

func (client *RedisClient) HGetAllAdWithPipeline(dbId int,keys []string) [](map[string]string){
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}

	result := make([](map[string]string),0)
	conn.Send("MULTI")
	for _,adId := range keys {
		key := SARA_KEY_AD_BASEDATA + adId
		conn.Send("HGETALL",key)
	}
	if err := conn.Flush(); err != nil{
		panic(err)
	}
	if pipe_prox ,err := conn.Do("EXEC"); err != nil {
		panic(err)
	}else {
		for i,value := range pipe_prox.([]interface{}){
			if res,err := redis.StringMap(value,nil); err != nil{
				panic(err)
			}else {
				res["advertisement_id"] = keys[i]
				result = append(result,res)
			}
		}
	}
	return result
}

func (client *RedisClient) HGetAllAdExInfoWhithPipeline(dbId int,keys []string) map[string](map[string]string){
	conn := client.GetConnection(REDIS_DM)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	result := make(map[string](map[string]string))
	conn.Send("MULTI")
	for _,adId := range keys{
		key := SARA_KEY_AD_EX_INFO + adId
		conn.Send("HGETALL",key)
	}
	if err := conn.Flush(); err != nil{
		panic(err)
	}
	if pipe_prox ,err := conn.Do("EXEC"); err != nil {
		panic(err)
	}else {
		for i,value := range pipe_prox.([]interface{}){
			if res,err := redis.StringMap(value,nil); err != nil{
				panic(err)
			}else {
				if len(res) > 0 {
					result[keys[i]] = res
				}
			}
		}
	}
	return result
}

func (client *RedisClient) HGetAllAdPlanWhithPipeline(dbId int,keys map[string]string) map[string](map[string]string){
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	result := make(map[string](map[string]string))
	keySet := []string{}
	for key,_ := range keys{
		keySet = append(keySet,key)
	}
	conn.Send("MULTI")
    for i := 0; i < len(keySet); i++{
		key := SARA_KEY_ADPLAN_COST_DATA + keys[keySet[i]]
		conn.Send("HGETALL",key)
	}
	if err := conn.Flush(); err != nil{
		panic(err)
	}
	if pipe_prox ,err := conn.Do("EXEC"); err != nil {
		panic(err)
	}else {
		for i,value := range pipe_prox.([]interface{}) {
			if res,err := redis.StringMap(value,nil); err != nil{
				panic(err)
			}else {
				if len(res) > 0 {
					result[keySet[i]] = res
				}
			}
		}
	}
	return result
}

func (client *RedisClient) HGetAllAdvertiserInfoWhithPipeline(dbId int,keys map[string]string) map[string](map[string]string) {
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	result := make(map[string](map[string]string))
	keySet := []string{}
	for key,_ := range keys{
		keySet = append(keySet,key)
	}
	conn.Send("MULTI")
	for i := 0; i < len(keySet); i++{
		key := SARA_KEY_ADVERTISER_COST_DATA + keys[keySet[i]]
		conn.Send("HGETALL",key)
	}
	if err := conn.Flush(); err != nil{
		panic(err)
	}
	if pipe_prox ,err := conn.Do("EXEC"); err != nil {
		panic(err)
	}else {
		for i,value := range pipe_prox.([]interface{}) {
			if res,err := redis.StringMap(value,nil); err != nil{
				panic(err)
			}else {
				if len(res) > 0 {
					result[keySet[i]] = res
				}
			}
		}
	}
	return result
}

func (client *RedisClient) HGetAllAdPlanCostWhithPipeline(dbId int,keys map[string]string) map[string](map[string]string){
	conn := client.GetConnection(REDIS_BDP_REALTIME)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	result := make(map[string](map[string]string))
	keySet := []string{}
	for key,_ := range keys{
		keySet = append(keySet,key)
	}
	conn.Send("MULTI")
	for i := 0; i < len(keySet); i++{
		key := AD_BDP_SENSEAR_ADPLAN_COST_DATA + keys[keySet[i]]
		conn.Send("HGETALL",key)
	}
	if err := conn.Flush(); err != nil{
		panic(err)
	}
	if pipe_prox ,err := conn.Do("EXEC"); err != nil {
		panic(err)
	}else {
		for i,value := range pipe_prox.([]interface{}) {
			if res,err := redis.StringMap(value,nil); err != nil{
				panic(err)
			}else {
				if len(res) > 0 {
					result[keySet[i]] = res
				}
			}
		}
	}
	return result
}

func (client *RedisClient) HGetAllAdvertiserCostInfoWhithPipeline(dbId int,keys map[string]string) map[string](map[string]string)  {
	conn := client.GetConnection(REDIS_BDP_REALTIME)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	result := make(map[string](map[string]string))
	keySet := []string{}
	for key,_ := range keys{
		keySet = append(keySet,key)
	}
	conn.Send("MULTI")
	for i := 0; i < len(keySet); i++{
		key := AD_BDP_SENSEAR_ADVERTISER_COST_DATA + keys[keySet[i]]
		conn.Send("HGETALL",key)
	}
	if err := conn.Flush(); err != nil{
		panic(err)
	}
	if pipe_prox ,err := conn.Do("EXEC"); err != nil {
		panic(err)
	}else {
		for i,value := range pipe_prox.([]interface{}) {
			if res,err := redis.StringMap(value,nil); err != nil{
				panic(err)
			}else {
				if len(res) > 0 {
					result[keySet[i]] = res
				}
			}
		}
	}
	return result
}

func (client *RedisClient) HGetAllBroadcasterTagsWhithPipeline(dbId int,appId string) map[string]string{
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,err := conn.Do("SELECT",dbId);err != nil {
		panic(err)
	}
	key := SARA_KEY_USER_TAGS + appId
	conn.Send("HGETALL",key)
	if err := conn.Flush(); err != nil{
		panic(err)
	}
	if err := conn.Flush(); err != nil {
		panic(err)
	}
	if value, err := conn.Receive(); err != nil{
		 panic(err)
	}else {
		if result, reerr := redis.StringMap(value,nil); reerr != nil{
			panic(reerr)
		}else {
			return result
		}
	}
}

func (client *RedisClient) HGetAllUserCoverageWhithPipeline(dbId int,appId string,keys []string)[]string{
	conn := client.GetConnection(REDIS_DM)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
    key := FANS_COUNT + appId
    args := redis.Args{}.Add(key).AddFlat(keys)
    conn.Send("HMSET",args...)
	if value, err := conn.Receive(); err != nil{
		panic(err)
	}else {
		if result, reerr := redis.Strings(value,nil); reerr != nil{
			panic(reerr)
		}else {
			return result
		}
	}
}

func (client *RedisClient) LRange(redisInstance string,dbId int,key string,start int,end int) []string{
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	if value, err := redis.Strings(conn.Do("LRANGE",key,start,end)); err != nil{
		panic(err)
	}else {
		return value
	}
}

func (client *RedisClient) HGetAllApps(dbId int) set.Set{
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
    result := set.NewSimpleSet()
	if value,err := redis.Strings(conn.Do("LRANGE","SARA_KEY_APP_LIST",0,-1)); err != nil{
        panic(err)
	}else {
		for _,v := range value{
			result.Add(v)
		}
		return result
	}

}

