package producerMq

import (
	"fmt"
	"github.com/streadway/amqp"
)

type producer struct{
	mqConnStr string
	exchangeName string
	queueName string
	kind string
	routeKey string
	autoAck bool
	connClient *amqp.Connection
	*amqp.Channel

}


// connStr 连接字符串
// exchange exchange 名字
// queue queue 名字
// routeKey exchange 与queue 绑定key
// kind exchange的Type direct fanout headers topic
// autoAck 回应确认，暂未用
func NewProducer(connStr,exchange,queue,routeKey,kind string,autoAck bool) *producer{
	return &producer{
		mqConnStr:connStr,
		exchangeName:exchange,
		queueName:queue,
		kind:kind,
		routeKey:routeKey,
		autoAck:autoAck,
	}
}

//todo:处理data
func (s *producer) Push(body []byte,data ...interface{}){
	if s.Channel==nil {
		chanel, err := s.newSerConn()
		if err !=nil{
			fmt.Println("occur connection fatal :",err)
			return
		}
		s.Channel = chanel
	}
	s.Publish(s.exchangeName,s.routeKey,false,false,amqp.Publishing{
		Body:body,
	})
}


func (s *producer) newConn()error{
	conn,err := amqp.Dial(s.mqConnStr)
	if err!=nil{
		return err
	}
	s.connClient= conn
	return nil
}

func (s *producer) newSerConn() (*amqp.Channel,error){
	if s.connClient == nil{
		err:= s.newConn()
		if err !=nil{
			return nil,err
		}
	}
	myChan,err:= s.connClient.Channel()
	if err!=nil{
		return nil,err
	}
	err = myChan.ExchangeDeclare(s.exchangeName,s.kind,true,false,false,false,nil)
	if err !=nil {
		return nil,err
	}
	_,err =myChan.QueueDeclare(s.queueName,true,false,false,false,nil)
	if err !=nil {
		return nil,err
	}
	err = myChan.QueueBind(s.queueName,s.routeKey,s.exchangeName,false,nil)
	if err !=nil {
		return nil,err
	}
	return myChan,nil
}

