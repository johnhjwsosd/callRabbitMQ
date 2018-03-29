package callRabbitMQ

import (
	"github.com/streadway/amqp"
	"time"
	"fmt"
	"errors"
)

type Producer struct{
	mqConnStr string
	exchangeName string
	kind string
	routeKey string
	connClient *amqp.Connection
	*amqp.Channel

	reConnInfo *reconnectionInfo
}


// NewProducer 创建一个生产者对象
// connStr 连接字符串
// exchange exchange 名字
// routeKey exchange 与queue 绑定key
// kind exchange的Type direct fanout headers topic
func NewProducer(connStr,exchange,routeKey,kind string) *Producer{
	return &Producer{
		mqConnStr:    connStr,
		exchangeName: exchange,
		kind:         kind,
		routeKey:     routeKey,
	}
}

// SetReconnectionInfo 设置重连信息
// reConnCounts 重连次数
// reConnTime 重连间隔时间
func (s *Producer) SetReconnectionInfo(reConnCounts int,reConnTime time.Duration){
	s.reConnInfo = &reconnectionInfo{reConnCounts,reConnTime}
}


func (s *Producer) Push(publishing amqp.Publishing)error{
	if s.Channel==nil {
		chanel, err := s.newProChannel()
		if err !=nil{
			return err
		}
		s.Channel = chanel
	}
	err:= s.Publish(s.exchangeName,s.routeKey,false,false,publishing)
	if err!=nil{
		if err == amqp.ErrClosed{
			if s.reConnInfo == nil{
				return err
			}else {
				fmt.Println("Producer connetion occur error,try reconnection")
				i :=1
				err = s.reconnection(i)
				if err!=nil{
					return err
				}else {
					s.Push(publishing) //重发链接中断的包
				}
			}
		}
		return err
	}
	return nil
}


func (s *Producer) reconnection(i int)error{
	fmt.Println("Producer  reconnection  times :",i)
	err := s.newConn()
	if err !=nil{
		if i>= s.reConnInfo.ReconnectionCounts{
			return errors.New("Producer reconnection fatal ,Closed")
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
		fmt.Println("producer connection fatal :",err)
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

	return myChan,nil


}

func (s *Producer) CloseConnection()error{
	err:= s.connClient.Close()
	if err == amqp.ErrClosed{
		return nil
	}
	return err
}