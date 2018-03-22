package main

import (
	"github.com/johnhjwsosd/callRabbitMQ/producerMq"
	"github.com/johnhjwsosd/callRabbitMQ/consumerMq"

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

	p:= producerMq.NewProducer(mqConnStr,exchange,queue,queueKey,kind,&producerMq.ReconnectionInfo{5,time.Second*5})
	go push(p)
	fmt.Println("=================+++")

	c := consumerMq.NewConsumer(mqConnStr,exchange,queue,queueKey,kind,true,20)
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

func push(p *producerMq.Producer){
	for i:=0;;i++ {
		time.Sleep(time.Millisecond * 1000)
		err := p.Push([]byte("test   "+strconv.Itoa(i)))
		if err != nil {
			fmt.Println("producer :", err)
			return
		}
	}
	p.CloseConnection()
}