package main

import (
	"github.com/johnhjwsosd/callRabbitMQ/serverMq"
	"fmt"
	"github.com/johnhjwsosd/callRabbitMQ/clientMq"
	"time"
)

const (
	mqConnStr  = `amqp://xxxxx:xxxxx@www.xxxx.com:5672/`
	exchange ="test.topic.1"
	queue = "test.topic.queue.1"
	queueKey = "t1"
	kind="topic"
)

func main(){

	p:= serverMq.NewProducer(mqConnStr,exchange,queue,queueKey,kind,true)
	go func(){ for {
		p.Push([]byte("test"))
		time.Sleep(time.Millisecond*5)
		}
	}()
	fmt.Println("=================+++")

	c := clientMq.NewComsumer(mqConnStr,exchange,queue,queueKey,kind,true,2)
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
