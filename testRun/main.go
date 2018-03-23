package main

import (
	"github.com/johnhjwsosd/callRabbitMQ"
	"fmt"
	"time"
	"strconv"
	"errors"
)

const (
	mqConnStr  = `amqp://ll:123qwe@118.31.32.168:5672/`
	exchange ="test.topic.12"
	queue = "test.topic.queue.12"
	queueKey = "t1"
	kind="topic"
)

func main(){
	p:= callRabbitMQ.NewProducer(mqConnStr,exchange,queue,queueKey,kind)
	p.SetReconnectionInfo(10,time.Second*10)
	go push(p)
	fmt.Println("=================+++")

	c := callRabbitMQ.NewConsumer(mqConnStr,exchange,queue,queueKey,kind,false,20)
	c.SetReconnectionInfo(100) //心跳时间5秒
	c.SetRetryInfo(5,time.Second*1,false)
	c.RegisterHandleFunc(test1)
	c.Run()
}

func test(content []byte)error{
	fmt.Println(" Recevice :",string(content))
	return nil
}


func test1(content []byte)error{
	fmt.Println(" Recevice :",string(content))
	return errors.New("test err")
}

func push(p *callRabbitMQ.Producer){
	for i:=0;i<10;i++ {
		time.Sleep(time.Millisecond * 100)
		err := p.Push([]byte("test   "+strconv.Itoa(i)))
		if err != nil {
			fmt.Println("producer :", err)
			return
		}
	}
	p.CloseConnection()
}