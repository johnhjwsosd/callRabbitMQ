package producerMq

import (
	"github.com/streadway/amqp"
	"time"
	"fmt"
	"errors"
)

type Producer struct{
	mqConnStr string
	exchangeName string
	queueName string
	kind string
	routeKey string
	connClient *amqp.Connection
	*amqp.Channel

	reConnInfo *ReconnectionInfo
}

type ReconnectionInfo struct{
	ReconnectionCounts int
	ReconnectionTime time.Duration
}

func NewProducer(connStr,exchange,queue,routeKey,kind string,reConnInfo *ReconnectionInfo) *Producer{
	return &Producer{
		mqConnStr:    connStr,
		exchangeName: exchange,
		queueName:    queue,
		kind:         kind,
		routeKey:     routeKey,
		reConnInfo:reConnInfo,
	}
}


func (s *Producer) Push(body []byte,data ...interface{})error{
	if s.Channel==nil {
		chanel, err := s.newProChannel()
		if err !=nil{
			return err
		}
		s.Channel = chanel
	}
	err:= s.Publish(s.exchangeName,s.routeKey,false,false,amqp.Publishing{
		Body:body,
	})
	if err!=nil{
		if err == amqp.ErrClosed{
			if s.reConnInfo == nil{
				return err
			}else {
				fmt.Println("Producer connetion occur fatal,try reconnection")
				i :=1
				err = s.reconnection(i)
				if err!=nil{
					return err
				}else {
					s.Push(body, data) //重发链接中断的包
				}
			}
		}
		return err
	}
	return nil
}


func (s *Producer) reconnection(i int)error{
	fmt.Println("Producer reconnection  times :",i)
	err := s.newConn()
	if err !=nil{
		if i>= s.reConnInfo.ReconnectionCounts{
			fmt.Println("Producer reconnection fatal")
			return errors.New("Producer reconnection fatal")
		}
		i++
		time.Sleep(s.reConnInfo.ReconnectionTime)
		return s.reconnection(i)
	}
	fmt.Println("Producer Reconnection Success")
	s.Channel=nil
	return nil
}

//todo:传入参数
func getPushDate(data ...interface{}){
	for _,v :=range data{
		switch v.(type){
		case bool:

		case byte:
		}
	}
}


func (s *Producer) newConn()error{
	conn,err := amqp.Dial(s.mqConnStr)
	if err!=nil{
		return err
	}
	s.connClient= conn
	return nil
}

func (s *Producer) newProChannel() (*amqp.Channel,error){
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

func (s *Producer) CloseConnection()error{
	err:= s.connClient.Close()
	if err == amqp.ErrClosed{
		return nil
	}
	return err
}