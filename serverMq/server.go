package serverMq

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


func (s *producer) Push(body []byte){
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
	mychan,err:= s.connClient.Channel()
	if err!=nil{
		return nil,err
	}
	err = mychan.ExchangeDeclare(s.exchangeName,s.kind,true,false,false,false,nil)
	if err !=nil {
		return nil,err
	}
	_,err =mychan.QueueDeclare(s.queueName,true,false,false,false,nil)
	if err !=nil {
		return nil,err
	}
	err = mychan.QueueBind(s.queueName,s.routeKey,s.exchangeName,false,nil)
	if err !=nil {
		return nil,err
	}
	return mychan,nil
}

