package util

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"fmt"
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
				   fmt.Println("测试连接超时")
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
			fmt.Errorf("redis连接归还失败",err)
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
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			//fmt.Fprintln(string(err))
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	if _,setx_err := conn.Do("SETEX",key,seconds,value);setx_err != nil{
		panic(setx_err)
	}
}


func (client *RedisClient) Expire(redisInstance string,dbId int,key string,seconds int){
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
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
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	if _,hmset_err := conn.Do("HMSET",redis.Args{}.Add(key).AddFlat(dict)...); hmset_err != nil{
		panic(hmset_err)
	}
}

func (client *RedisClient) Lset(redisInstance string,dbId int, key string,list []string){
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	if _,lset_err := conn.Do("LPUSH",redis.Args{}.Add(key).AddFlat(list)...); lset_err != nil{
		panic(lset_err)
	}
}

func (client *RedisClient) LsetByPipeline(redisInstance string,dbId int,resultMap map[string]([]string),expireTime int){
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	//r := NewRunner(conn)
	//for key,value := range resultMap {
	//	pushArgs := redis.Args{}.Add(key).AddFlat(value)
	//	r.send <- command{name: "LPUSH", args:pushArgs,result:make(chan Result,100)}
	//	if expireTime > 0 {
	//		expireArgs := redis.Args{}.Add(key).AddFlat(expireTime)
	//		r.send <- command{name: "EXPIRE", args: expireArgs,result:make(chan Result,100)}
	//	}
	//}
	//close(r.stop)
	//<-r.done
	conn.Send("MULTI")
	for key,value := range resultMap{
		fmt.Println(key,value)
		pushArgs := redis.Args{}.Add(key).AddFlat(value)
		conn.Send("LPUSH",pushArgs)
		if expireTime > 0{
			expireArgs := redis.Args{}.Add(key).AddFlat(expireTime)
			conn.Send("EXPIRE",expireArgs)
		}
	}
	if _,err := conn.Do("EXEC"); err != nil{
		panic(err)
	}

}

func (client *RedisClient) HmsetByPipeline(redisInstance string,dbId int,resultMap map[string](map[string]string),expireTime int){
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	r := NewRunner(conn)
	for key,value := range resultMap {
		//args := []interface{}{key}
		//for k,v := range value {
		//	args = append(args,k,v)
		//}
		args := redis.Args{}.Add(key).AddFlat(value)
		r.send <- command{name: "HMSET", args:args,result:make(chan Result,1)}
		if expireTime > 0 {
			expireArgs := redis.Args{}.Add(key).AddFlat(expireTime)
			r.send <- command{name: "EXPIRE", args: expireArgs,result:make(chan Result,1)}
		}
	}
	close(r.stop)
	<-r.done
}

func (client *RedisClient) LGetAllTargetAdWithPipeLine(dbId int,keys set.Set)  set.Set {
	conn := client.GetConnection(REDIS_DM)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	r := NewRunner(conn)
	for _,key := range keys.Elements(){
		args := redis.Args{}.Add(key).AddFlat(0).AddFlat(-1)
		r.send <- command{name:"LRANGE",args:args,result:make(chan Result,1)}
	}
	close(r.stop)
	<-r.done
	result := set.NewSimpleSet()
	for _,v := range r.last{
		if value,err := redis.String(v.value,v.err); err != nil{
			continue
		}else {
			result.Add(value)
		}
	}
	return result
}

func (client *RedisClient) Get(redisInstance string,dbId int,key string) string{
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
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
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
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

func (client *RedisClient) HGetAllAdWithPipeline(dbId int,keys []string) [](map[string]string){
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}

	r := NewRunner(conn)
	for _,adId := range keys{
		key := SARA_KEY_AD_BASEDATA + string(adId)
		args := redis.Args{}.Add(key)
        r.send <- command{name:"HGETALL",args:args,result:make(chan Result,1)}
	}
	close(r.stop)
	<-r.done
	result := make([](map[string]string),0)
	for i := 0; i < len(keys) ; i++  {
		key := SARA_KEY_AD_BASEDATA + keys[i]
		if value,err := redis.StringMap(r.last[i].value,r.last[i].err); err != nil{
			continue
		}else {
			value["advertisement_id"] = key
			result = append(result,value)
		}
	}
	return result
}

func (client *RedisClient) HGetAllAdExInfoWhithPipeline(dbId int,keys []string) map[string](map[string]string){
	conn := client.GetConnection(REDIS_DM)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	r := NewRunner(conn)
	for _,adId := range keys{
		key := SARA_KEY_AD_EX_INFO + string(adId)
		r.send <- command{name:"HGETALL",args:redis.Args{}.Add(key),result:make(chan Result,1)}
	}
	close(r.stop)
	<-r.done
	result := make(map[string](map[string]string))
	for i := 0; i < len(keys) ; i++  {
		key := SARA_KEY_AD_EX_INFO + keys[i]
		if value,err := redis.StringMap(r.last[i].value,r.last[i].err); err != nil{
			continue
		}else {
			result[key] = value
		}
	}
	return result
}

func (client *RedisClient) HGetAllAdPlanWhithPipeline(dbId int,keys map[string]string) map[string](map[string]string){
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	r := NewRunner(conn)
	temp := []string{}
	for key,value := range keys{
        temp = append(temp,key,value)
	}
	for i := 1; i < len(temp); i += 2{
		key := SARA_KEY_ADPLAN_COST_DATA + temp[i]
		r.send <- command{name:"HGETALL",args:redis.Args{}.Add(key),result:make(chan Result,1)}
	}
	close(r.stop)
	<-r.done
	result := make(map[string](map[string]string))
	for i := 0; i < len(temp); i += 2  {
		key := SARA_KEY_ADPLAN_COST_DATA + string(temp[i+1])
		if value,err := redis.StringMap(r.last[i/2].value,r.last[i/2].err); err != nil{
			continue
		}else {
			result[key] = value
		}
	}
	return result
}

func (client *RedisClient) HGetAllAdvertiserInfoWhithPipeline(dbId int,keys map[string]string) map[string](map[string]string) {
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	r := NewRunner(conn)
	temp := []string{}
	for key,value := range keys{
		temp = append(temp,key,value)
	}
	for i := 1; i < len(temp); i += 2{
		key := SARA_KEY_ADVERTISER_COST_DATA + temp[i]
		r.send <- command{name:"HGETALL",args:redis.Args{}.Add(key),result:make(chan Result,1)}
	}
	close(r.stop)
	<-r.done
	result := make(map[string](map[string]string))
	for i := 0; i < len(temp); i += 2  {
		key := SARA_KEY_ADVERTISER_COST_DATA + temp[i+1]
		if value,err := redis.StringMap((r.last[i/2]).value,(r.last[i/2]).err); err != nil{
			continue
		}else {
			result[key] = value
		}
	}
	return result
}

func (client *RedisClient) HGetAllAdPlanCostWhithPipeline(dbId int,keys map[string]string) map[string](map[string]string){
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	r := NewRunner(conn)
	temp := []string{}
	for key,value := range keys{
		temp = append(temp,key,value)
	}
	for i := 1; i < len(temp); i += 2{
		key := AD_BDP_SENSEAR_ADPLAN_COST_DATA + temp[i]
		r.send <- command{name:"HGETALL",args:redis.Args{}.Add(key),result:make(chan Result,1)}
	}
	close(r.stop)
	<-r.done
	result := make(map[string](map[string]string))
	for i := 0; i < len(temp); i += 2 {
		key := AD_BDP_SENSEAR_ADPLAN_COST_DATA + temp[i+1]
		if value, err := redis.StringMap(r.last[i/2].value, r.last[i/2].err); err != nil {
			continue
		} else {
			result[key] = value
		}
	}
	return result
}

func (client *RedisClient) HGetAllAdvertiserCostInfoWhithPipeline(dbId int,keys map[string]string) map[string](map[string]string)  {
	conn := client.GetConnection(REDIS_BDP_REALTIME)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	r := NewRunner(conn)
	temp := []string{}
	for key,value := range keys{
		temp = append(temp,key,value)
	}
	for i := 1; i < len(temp); i += 2{
		key := AD_BDP_SENSEAR_ADVERTISER_COST_DATA + temp[i]
		r.send <- command{name:"HGETALL",args:redis.Args{}.Add(key),result:make(chan Result,1)}
	}
	close(r.stop)
	<-r.done
	result := make(map[string](map[string]string))
	for i := 0; i < len(temp); i += 2  {
		key := AD_BDP_SENSEAR_ADVERTISER_COST_DATA + string(temp[i+1])
		if value,err := redis.StringMap(r.last[i/2].value,r.last[i/2].err); err != nil{
			continue
		}else {
			result[key] = value
		}
	}
	return result
}

func (client *RedisClient) HGetAllBroadcasterTagsWhithPipeline(dbId int,appId string) map[string]string{
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	r := NewRunner(conn)
	key := SARA_KEY_USER_TAGS + appId
	r.send <- command{name:"HGETALL",args:redis.Args{}.Add(key),result:make(chan Result,1)}
	close(r.stop)
	<-r.done
	if value,err :=  redis.StringMap(r.last[0].value,r.last[0].err);err != nil{
		log.Fatal(err)
		return nil
	}else {
		return value
	}
}

func (client *RedisClient) HGetAllUserCoverageWhithPipeline(dbId int,appId string,keys []string)[]string{
	conn := client.GetConnection(REDIS_DM)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	r := NewRunner(conn)
	key := FANS_COUNT + appId
	r.send <- command{name:"HGETALL",args:redis.Args{}.Add(key),result:make(chan Result,1)}
	close(r.stop)
	<-r.done
	if value,err :=  redis.Strings(r.last[0].value,r.last[0].err);err != nil{
		log.Fatal(err)
		return nil
	}else {
		return value
	}
}

func (client *RedisClient) LRange(redisInstance string,dbId int,key string,start int,end int) []string{
	conn := client.GetConnection(redisInstance)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}
	if value, err := redis.Strings(conn.Do("LRANGE",key,start,end)); err != nil{
		panic(err)
		return nil
	}else {
		return value
	}
}

func (client *RedisClient) HGetAllApps(dbId int) []string{
	conn := client.GetConnection(REDIS_SENSEAR)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	if _,select_err := conn.Do("SELECT",dbId);select_err != nil {
		panic(select_err)
	}

	if value,err := redis.Strings(conn.Do("LRANGE","SARA_KEY_APP_LIST",0,-1)); err != nil{
		return nil
	}else {
		return value
	}
}

