package callRabbitMQ

import (
	"github.com/streadway/amqp"
	"fmt"
	"time"
	"errors"
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

	handlerFuncRetry *handleErrRetryInfo
	reConnInfo *reconnectionInfo
}
type chanClient struct{
	chanDelivery <-chan amqp.Delivery
	chanel *amqp.Channel
}

func (c *consumer) pull(){
	var err error
	for i:=0;i<c.handlePool;i++ {
		if len(c.chanClients) == i {
			chanel, err := c.newChanel()
			if err != nil {
				fmt.Println("occur fatal connection  :", err)
				return
			}
			c.chanClients = append(c.chanClients,&chanClient{make(chan amqp.Delivery),chanel})
		}
		c.chanClients[i].chanDelivery,err = c.chanClients[i].chanel.Consume(c.queueName,"ll",c.autoAck,false,false,false,nil)
		if err!=nil{
			fmt.Println("consumer", i," consume err : ",err)
		}
		go func(msg <-chan amqp.Delivery){
			for body :=range msg{
				c.handleFuncACK(body)
			}
		}(c.chanClients[i].chanDelivery)
	}
	<-c.isClosed
}

// NewConsumer 返回一个消费者对象，一个消费者共用一个链接
// connStr 连接字符串
// exchange exchange 名字
// queue queue 名字
// routeKey exchange 与queue 绑定key
// kind exchange的Type direct fanout headers topic
// autoAck 自动回应确认,如果为true 不会对处理函数返回错误而处理queue消息
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

// RegisterHandleFunc 注册处理方法
// this 处理方法
func (c *consumer) RegisterHandleFunc(this func([]byte) error){
	c.handleFunc=this
}

// SetReconnectionInfo 设置重连信息
// reConnCounts 重连次数
func (c *consumer) SetReconnectionInfo(reConnCounts int){
	c.reConnInfo = &reconnectionInfo{ReconnectionCounts:reConnCounts}
}

// SetRetryInfo 设置处理函数返回ERROR重试信息
// retryCounts 重试次数
// retryTime 每次重试时间间隔
// isRequeue 是否放回队列
func (c *consumer) SetRetryInfo(retryCounts int,retryTime time.Duration, isRequeue bool){
	c.handlerFuncRetry = &handleErrRetryInfo{retryCounts,retryTime,isRequeue}
}


func (c *consumer) newConn()error{
	conn,err := amqp.Dial(c.mqConnStr)
	if err!=nil{
		fmt.Println("consumer connection fatal :",err)
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

func (c *consumer) handleFuncACK(body amqp.Delivery){
	if c.autoAck{
		c.handleFunc(body.Body)
	}else{
		if c.handlerFuncRetry !=nil {
			i := 0
			for {
				if i > c.handlerFuncRetry.handleCount {
					i = 0
					body.Nack(false,c.handlerFuncRetry.isrequeue)
					break
				}
				err := c.handleFunc(body.Body)
				i++
				if err == nil {
					i = 0
					body.Ack(false)
					break
				}
				time.Sleep(c.handlerFuncRetry.handleTime)
			}
		}else{
			c.handleFunc(body.Body)
			body.Nack(false, false)
		}
	}
}

func (c *consumer) Run()error{
	ch :=make(chan int)
	err:= c.newConn()
	if err !=nil{
		fmt.Println("consumer connection fatal :",err)
		return err
	}
	go c.heartBeat(ch)

	c.pull()

	<- ch
	return errors.New("consumer connection error, closed")
}

func (c *consumer) heartBeat(closeCh chan int){
	i:=1
	for{
		time.Sleep(time.Second * 5)
		ch,err:= c.connClient.Channel()
		if err == amqp.ErrClosed {
			fmt.Println("Consumber Connection Occur Error ,Try Reconnection")
			if c.reConnInfo !=nil {
				if i > c.reConnInfo.ReconnectionCounts{
					fmt.Println("Consumber ReConnection  Fail ,Closed")
					closeCh <- 1
					return
				}
				err := c.newConn()
				if err != nil {
					fmt.Println("Consumer  Reconnection times :", i)
					i++
					continue
				}
				i = 1
				fmt.Println("Consumer Reconnection Success")
				c.chanClients = make([]*chanClient, 0, 1024)
				go func() { c.isClosed <- 1 }()
				go c.pull()
				continue
			}
			closeCh <- 1
			return
		}
		ch.Close()
	}
}
