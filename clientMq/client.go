package clientMq

import (
	"github.com/streadway/amqp"
	"fmt"
)

type comsumer struct{
	mqConnStr string
	exchangeName string
	queueName string
	kind string
	routeKey string
	autoAck bool
	handlePool int
	handleFunc func([]byte)error
	chanClients []*chanClient
}
type chanClient struct{
	chanDelivery <-chan amqp.Delivery
	chanel *amqp.Channel
}

func (c *comsumer) Pull(){
	var err error
	isClosed := make(chan int)
	for i:=0;i<c.handlePool;i++ {
		if len(c.chanClients) == i {
			chanel, err := c.newClientConn()
			if err != nil {
				fmt.Println("occur connection fatal :", err)
				return
			}
			c.chanClients = append(c.chanClients,&chanClient{make(chan amqp.Delivery),chanel})
		}
		c.chanClients[i].chanDelivery,err = c.chanClients[i].chanel.Consume(c.queueName,"test",c.autoAck,false,false,false,nil)
		if err!=nil{
			fmt.Println("client", i," get err : ",err)
		}
		go func(msg <-chan amqp.Delivery){
			for body :=range msg{
				c.handleFunc(body.Body)
			}
		}(c.chanClients[i].chanDelivery)
	}
	<-isClosed
}

func NewComsumer(connStr,exchange,queue,routeKey,kind string,autoAck bool,handlePool int)*comsumer{
	return &comsumer{
		mqConnStr:connStr,
		exchangeName:exchange,
		queueName:queue,
		kind:kind,
		routeKey:routeKey,
		autoAck:autoAck,
		handlePool:handlePool,
		chanClients:make([]*chanClient,0,1024),
	}
}

func (c *comsumer) RegisterHandleFunc(this func([]byte) error){
	c.handleFunc=this
}


func (c *comsumer) newClientConn() (*amqp.Channel,error){
	conn,err := amqp.Dial(c.mqConnStr)
	if err!=nil{
		return nil,err
	}
	mychan,err:= conn.Channel()
	if err!=nil{
		return nil,err
	}
	err = mychan.ExchangeDeclare(c.exchangeName,c.kind,true,false,false,false,nil)
	if err !=nil {
		return nil,err
	}
	_,err =mychan.QueueDeclare(c.queueName,true,false,false,false,nil)
	if err !=nil {
		return nil,err
	}
	err = mychan.QueueBind(c.queueName,c.routeKey,c.exchangeName,false,nil)
	if err !=nil {
		return nil,err
	}
	return mychan,nil
}
