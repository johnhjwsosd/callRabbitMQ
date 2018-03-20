package main

import (
	"github.com/johnhjwsosd/callRabbitMQ/producerMq"
	"fmt"
	"github.com/johnhjwsosd/callRabbitMQ/consumerMq"
	"time"
)

const (
	mqConnStr  = `amqp://xxx:xxx@www.xxxxx.com:5672/`
	exchange ="test.topic.1"
	queue = "test.topic.queue.1"
	queueKey = "t1"
	kind="topic"
)

func main(){

	p:= producerMq.NewProducer(mqConnStr,exchange,queue,queueKey,kind,true)
	go func(){ for {
		p.Push([]byte("test"))
		time.Sleep(time.Millisecond*5)
		}
	}()
	fmt.Println("=================+++")

	c := consumerMq.NewConsumer(mqConnStr,exchange,queue,queueKey,kind,true,2)
	c.RegisterHandleFunc(test)
	c.Pull()
	//go base.Push()
	//time.Sleep(time.Second*1)
	//consume()
}

func test(content []byte)error{
	fmt.Println(" Recevice :",string(content))
	return nil
}
