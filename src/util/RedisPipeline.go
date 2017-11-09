package util

import (
	"github.com/garyburd/redigo/redis"
	"log"
	"fmt"
)

//redis命令
type command struct {
	name string
	args []interface{}
	result chan Result
}

//redis结果
type Result struct {
	err error
	value interface{}
}

//pipeline运行者
type runner struct {
	conn redis.Conn
	send chan command
	recv chan chan Result
	stop chan struct{}
	done chan struct{}
	last []Result
}

//发送命令
func (r *runner) sender(){
	var flush int
	for {
		select {
		case <-r.stop:
			if err := r.conn.Flush(); err != nil {
				log.Fatal(err)
			}
			close(r.recv)
			fmt.Println("FLUSH",flush)
			return
		case cmd := <-r.send:
			if err := r.conn.Send(cmd.name,cmd.args...); err != nil {
				log.Fatal(err)
			}
			if len(r.send) == 0 || len(r.recv) == cap(r.recv) {
				flush++
				if err := r.conn.Flush(); err != nil {
					log.Fatal(err)
				}
			}
			r.recv <- cmd.result
		}
	}
}

//结果接受者
func (r *runner) receiver() {
	for ch := range r.recv  {
		var result Result
		result.value,result.err = r.conn.Receive()
		ch <- result
		r.last = append(r.last,result)
		if result.err != nil && r.conn.Err() != nil {
			log.Fatal(r.conn.Err())
		}
	}
	close(r.done)
}

func NewRunner(conn redis.Conn) *runner {
	r := &runner{conn:conn,send:make(chan command,100),recv:make(chan chan Result,100),stop:make(chan struct{}),done:make(chan struct{})}
	go r.sender()
	go r.receiver()
	return r
}
