package util

import (
	"github.com/boltdb/bolt"
	"time"
	"log"
	"fmt"
	"sync"
)

const (
	CPM_ADINFO_DB = "CPM_ADINFO"
	CPM_ADSTAT_DB = "CPM_ADSTAT"
	CPT_ADINFO_DB = "CPT_ADINFO"
	CPT_ADSTAT_DB = "CPT_ADSTAT"
	PRELOAD_ADINFO_DB = "PRELOAD_ADINFO"
	ADMODE_NUM_DB = "ADMODE_NUM"
)
var instance *BoltDBClient
var once sync.Once

type BoltDBClient struct {
	boltdb *bolt.DB
	CPM_AdInfo *bolt.Bucket
	CPM_AdStat *bolt.Bucket
	CPT_AdInfo *bolt.Bucket
    CPT_AdStat *bolt.Bucket
	Preload_adInfo *bolt.Bucket
    AdMode_Num *bolt.Bucket
	choosedb int
}

func newBoltDBClient() *BoltDBClient{
	dbConfig := &bolt.Options{Timeout:1 * time.Second}
	var (
		CPM_AdInfo *bolt.Bucket
		CPM_AdStat *bolt.Bucket
		CPT_AdInfo *bolt.Bucket
		CPT_AdStat *bolt.Bucket
		Preload_adInfo *bolt.Bucket
		AdMode_Num *bolt.Bucket
	)
	db,err := bolt.Open("BoltDB/BoltDBData.dat",0600,dbConfig)
	if err != nil{
		return nil
	}
	err = db.Update(func(tx *bolt.Tx) error {
		var create_err error
		CPM_AdInfo,create_err = tx.CreateBucketIfNotExists([]byte(CPM_ADINFO_DB))
		if create_err != nil{
			log.Fatal("create CPM_ADINFO_DB failed" )
			return create_err
		}
		CPM_AdStat,create_err = tx.CreateBucketIfNotExists([]byte(CPM_ADSTAT_DB))
		if create_err != nil{
			log.Fatal("create CPM_ADSTAT_DB failed" )
			return create_err
		}
		CPT_AdInfo,create_err = tx.CreateBucketIfNotExists([]byte(CPT_ADINFO_DB))
		if create_err != nil{
			log.Fatal("create CPT_ADINFO_DB failed" )
			return create_err
		}
		CPT_AdStat,create_err = tx.CreateBucketIfNotExists([]byte(CPT_ADSTAT_DB))
		if create_err != nil{
			log.Fatal("create CPT_ADSTAT_DB failed" )
			return create_err
		}
		Preload_adInfo,create_err = tx.CreateBucketIfNotExists([]byte(PRELOAD_ADINFO_DB))
		if create_err != nil{
			log.Fatal("create PRELOAD_ADINFO_DB failed" )
			return create_err
		}
		AdMode_Num,create_err = tx.CreateBucketIfNotExists([]byte(ADMODE_NUM_DB))
		if create_err != nil{
			log.Fatal("create ADMODE_NUM_DB failed" )
			return create_err
		}
		return nil
	})
	if err != nil {
		log.Fatal("create BoltDBClient failed" )
		return nil
	}
	fmt.Println("BoltDBClient Create Done")
	return &BoltDBClient{
		boltdb:db,
		CPM_AdInfo:CPM_AdInfo,
		CPM_AdStat:CPM_AdStat,
		CPT_AdInfo:CPT_AdInfo,
		CPT_AdStat:CPM_AdStat,
		Preload_adInfo:Preload_adInfo,
		AdMode_Num:AdMode_Num,
	}
}

func GetInstance() *BoltDBClient{
	once.Do(func() {
		instance = newBoltDBClient()
	})
	return instance
}

