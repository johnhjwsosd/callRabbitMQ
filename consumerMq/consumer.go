package consumerMq

import (
	"github.com/streadway/amqp"
	"fmt"
	"time"
)

type consumer struct{
	mqConnStr string
	exchangeName string
	queueName string
	kind string
	routeKey string
	autoAck bool
	handlePool int
	handleFunc func([]byte)error
	connClient *amqp.Connection
	chanClients []*chanClient
	isClosed chan int

	handlerRetry *HandleRetryInfo
}
type chanClient struct{
	chanDelivery <-chan amqp.Delivery
	chanel *amqp.Channel
}

type HandleRetryInfo struct{
	handleCount int
	handleTime time.Duration
}

func (c *consumer) Pull(){
	var err error
	for i:=0;i<c.handlePool;i++ {
		if len(c.chanClients) == i {
			chanel, err := c.newChanel()
			if err != nil {
				fmt.Println("occur connection fatal :", err)
				return
			}
			c.chanClients = append(c.chanClients,&chanClient{make(chan amqp.Delivery),chanel})
		}
		c.chanClients[i].chanDelivery,err = c.chanClients[i].chanel.Consume(c.queueName,"test",c.autoAck,false,false,false,nil)
		if err!=nil{
			fmt.Println("consumer", i," consume err : ",err)
		}
		go func(msg <-chan amqp.Delivery){
			for body :=range msg{
				err = c.handleFunc(body.Body)
			}
		}(c.chanClients[i].chanDelivery)
	}
	<-c.isClosed
}

// connStr 连接字符串
// exchange exchange 名字
// queue queue 名字
// routeKey exchange 与queue 绑定key
// kind exchange的Type direct fanout headers topic
// autoAck 自动回应确认
// handlePool 消费者处理个数
func NewConsumer(connStr,exchange,queue,routeKey,kind string,autoAck bool,handlePool int)*consumer{
	return &consumer{
		mqConnStr:connStr,
		exchangeName:exchange,
		queueName:queue,
		kind:kind,
		routeKey:routeKey,
		autoAck:autoAck,
		handlePool:handlePool,
		chanClients:make([]*chanClient,0,1024),
		isClosed:make(chan int),
	}
}

func (c *consumer) RegisterHandleFunc(this func([]byte) error){
	c.handleFunc=this
}



func (c *consumer) newConn()error{
	conn,err := amqp.Dial(c.mqConnStr)
	if err!=nil{
		return err
	}
	c.connClient= conn
	return nil
}


func (c *consumer) newChanel() (*amqp.Channel,error){
	if c.connClient == nil{
		err:= c.newConn()
		if err !=nil{
			return nil,err
		}
	}
	myChan,err:= c.connClient.Channel()
	if err!=nil{
		return nil,err
	}
	err = myChan.ExchangeDeclare(c.exchangeName,c.kind,true,false,false,false,nil)
	if err !=nil {
		return nil,err
	}
	_,err =myChan.QueueDeclare(c.queueName,true,false,false,false,nil)
	if err !=nil {
		return nil,err
	}
	myChan.
	err = myChan.QueueBind(c.queueName,c.routeKey,c.exchangeName,false,nil)
	if err !=nil {
		return nil,err
	}
	err = myChan.Qos(1,0,false)
	if err!=nil{
		return nil,err
	}
	return myChan,nil
}

func (c *consumer) CloseConnection()error{
	err:= c.connClient.Close()
	c.isClosed<-1
	return err
}
