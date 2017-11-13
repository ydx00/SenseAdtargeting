package util

import (
	"github.com/tidwall/buntdb"
	"github.com/tidwall/cast"
	"log"
    "sync"
	"time"
	"bytes"
	"encoding/gob"
	"strconv"
)
const(
	DBPATH = "BuntDB.dat"
	CPM_ADINFO_DB = "CPM_ADINFO:"
	CPM_ADSTAT_DB = "CPM_ADSTAT:"
	CPT_ADINFO_DB = "CPT_ADINFO:"
	CPT_ADSTAT_DB = "CPT_ADSTAT:"
	PRELOAD_ADINFO_DB = "PRELOAD_ADINFO:"
	ADMODE_NUM_DB = "ADMODE_NUM:"
)

var AD_STATIC_INFO_TASK_FRE = NewConfigHelper().ConfigMap["AD_STATIC_INFO_TASK_FRE"]
var AD_REALTIME_INFO_TASK_PRE = NewConfigHelper().ConfigMap["AD_REALTIME_INFO_TASK_PRE"]
var PRE_LOAD_TASK_PRE = NewConfigHelper().ConfigMap["PRE_LOAD_TASK_PRE"]


var instance *BuntDBClient = nil
var once sync.Once

type BuntDBClient struct{
	db *buntdb.DB
	path string
}

//创建数据库
func newBuntDBClient() *BuntDBClient{
	db,err := buntdb.Open(DBPATH)
	if err != nil {
		log.Fatal("创建数据库失败")
		return nil
	}
	var config buntdb.Config
	if err := db.ReadConfig(&config); err != nil {
		log.Fatal("数据库读取配置失败")
		db.Close()
		return nil
	}
	config.SyncPolicy = buntdb.EverySecond
	if err := db.SetConfig(config); err != nil {
		log.Fatal("数据库设置配置失败")
		db.Close()
		return nil
	}
	store := &BuntDBClient{
		db:   db,
		path: DBPATH,
	}
	return store
}

/**
   获取BuntDBClient实例
 */
func GetBuntDBInstance() *BuntDBClient{
	once.Do(func() {
		instance = newBuntDBClient()
	})
	return instance
}

/**
   写入切片
 */
func (client *BuntDBClient) WriteArr(key string,value []string,bucket string) error{
	data,err := encodeArrToByte(value)
	if err != nil{
		log.Fatal("编码失败")
		return err
	}
	return client.set(key,data,bucket)
}

/**
   读取切片
 */
func (client *BuntDBClient) ReadArr(key string,bucket string)([]string,error){
	value,err := client.get(key,bucket)
	if err != nil {
		return nil,err
	}
	return byteDecodeToArr(value)
}

/**
   写入字典
 */
func (client *BuntDBClient) WriteMap(key string,value map[string]string,bucket string) error{
	data,err := encodeMapToByte(value)
	if err != nil{
		log.Fatal("编码失败")
		return err
	}
	return client.set(key,data,bucket)
}

/**
   读取字典
 */
func (client *BuntDBClient) ReadMap(key string,bucket string) (map[string]string,error){
	value,err := client.get(key,bucket)
	if err != nil {
		return nil,err
	}
	return byteDecodeToMap(value)
}

// 存入整数
func (client *BuntDBClient) WriteInt(key string, val int,bucket string) error {
	return client.set(key, []byte(strconv.Itoa(val)),bucket)
}

// 得到整数
func (client *BuntDBClient) ReadInt(key string,bucket string) (int, error) {
	val, err := client.get(key,bucket)
	if err != nil {
		return -1, err
	}
	return strconv.Atoi(cast.ToString(val))
}

/**
   将key,value存入数据库
 */
func (b *BuntDBClient) set(key string, value []byte,bucket string) error {
	opts := &buntdb.SetOptions{Expires:true}
	switch bucket {
	case CPM_ADINFO_DB:
        opts.TTL = StringToDuration(AD_STATIC_INFO_TASK_FRE) * time.Minute
	case CPM_ADSTAT_DB:
		opts.TTL = StringToDuration(AD_REALTIME_INFO_TASK_PRE) * time.Minute
	case CPT_ADINFO_DB:
		opts.TTL = StringToDuration(AD_STATIC_INFO_TASK_FRE) * time.Minute
	case CPT_ADSTAT_DB:
		opts.TTL = StringToDuration(AD_REALTIME_INFO_TASK_PRE) * time.Minute
	case PRELOAD_ADINFO_DB:
		opts.TTL = StringToDuration(PRE_LOAD_TASK_PRE) * time.Minute
	case ADMODE_NUM_DB:
		opts.TTL = StringToDuration(AD_STATIC_INFO_TASK_FRE) * time.Minute
	}
	return b.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(bucket+key, cast.ToString(value), opts)
		return err
	})
}

/**
   将根据Key得到value
 */
func (client *BuntDBClient) get(key string,bucket string) ([]byte, error) {
	var val []byte
	err := client.db.View(func(tx *buntdb.Tx) error {
		sval, err := tx.Get(bucket + key)
		if err != nil {
			return err
		}
		val = []byte(sval)
		return nil
	})
	if err != nil {
		if err == buntdb.ErrNotFound {
			return nil, nil
		}
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}


/**
   将[]string编码为[]byte
*/
func encodeArrToByte(value []string) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

/**
   将[]byte解码为[]string
*/
func byteDecodeToArr(data []byte) ([]string, error) {
	var p *[]string
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&p)
	if err != nil {
		return nil, err
	}
	return *p, nil
}

/**
   将map[string]string编码为[]byte
*/
func encodeMapToByte(value map[string]string) ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

/**
   将[]byte解码为map[string]string
*/
func byteDecodeToMap(data []byte) (map[string]string, error) {
	var p *map[string]string
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&p)
	if err != nil {
		return nil, err
	}
	return *p, nil
}

