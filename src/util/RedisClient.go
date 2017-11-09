package util

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"fmt"
	"set"
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
	_,setx_err := conn.Do("SETEX",key,seconds,value)
	if setx_err != nil {
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
	_,expire_err := conn.Do("EXPIRE",key,seconds)
	if expire_err != nil {
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
	_,hmset_err := conn.Do("HMSET",dict)
	if hmset_err != nil {
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
		r.send <- command{name: "LPUSH", args:[]interface{}{key,value},result:make(chan Result,1)}
		if expireTime > 0 {
			r.send <- command{name: "EXPIRE", args: []interface{}{key,expireTime},result:make(chan Result,1)}
		}
	}
	close(r.stop)
	<-r.done
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
		args := []interface{}{key}
		for k,v := range value {
			args = append(args,k,v)
		}
		r.send <- command{name: "HMSET", args:args,result:make(chan Result,1)}
		if expireTime > 0 {
			r.send <- command{name: "EXPIRE", args: []interface{}{key,expireTime},result:make(chan Result,1)}
		}
	}
	close(r.stop)
	<-r.done
}

func (client *RedisClient) LGetAllTargetAdWithPipeLine(dbId int,keys set.Set) (result set.Set) {
	conn := client.GetConnection(REDIS_DM)
	defer client.ReturnConn(conn)
	defer func() {
		if err := recover(); err != nil {
			fmt.Errorf("出错了",err)
		}
	}()
	r := NewRunner(conn)
	for _,key := range keys.Elements(){
		r.send <- command{name:"LRANGE",args:[]interface{}{key,0,-1},result:make(chan Result,1)}
	}
	close(r.stop)
	<-r.done
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

func (client *RedisClient) HGetAllAdWithPipeline(dbId int,keys []string) map[string](map[string]string){
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
        r.send <- command{name:"HGETALL",args:[]interface{}{key},result:make(chan Result,1)}
	}
	close(r.stop)
	<-r.done
	result := make(map[string](map[string]string))
	for i := 0; i < len(keys) ; i++  {
		key := SARA_KEY_AD_BASEDATA + keys[i]
		if value,err := redis.StringMap(r.last[i].value,r.last[i].err); err != nil{
				continue
		}else {
				result[key] = value
				(result[key])["advertisement_id"] = key
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
		r.send <- command{name:"HGETALL",args:[]interface{}{key},result:make(chan Result,1)}
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
		r.send <- command{name:"HGETALL",args:[]interface{}{key},result:make(chan Result,1)}
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
		r.send <- command{name:"HGETALL",args:[]interface{}{key},result:make(chan Result,1)}
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
		r.send <- command{name:"HGETALL",args:[]interface{}{key},result:make(chan Result,1)}
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
		r.send <- command{name:"HGETALL",args:[]interface{}{key},result:make(chan Result,1)}
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
	r.send <- command{name:"HGETALL",args:[]interface{}{key},result:make(chan Result,1)}
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
	r.send <- command{name:"HGETALL",args:[]interface{}{key},result:make(chan Result,1)}
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
	if value, err := redis.Strings(conn.Do("LRANGE",key,start,end)); err != nil{
		log.Fatal(err)
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

