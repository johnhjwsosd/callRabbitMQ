package main

import (
	"github.com/johnhjwsosd/callRabbitMQ"
	"fmt"
	"time"
	"strconv"
	"errors"
	"github.com/streadway/amqp"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"os"
)

type Conf struct {
	Consumer *consumer `yaml:"consumer"`
	Producer *producer `yaml:"producer"`
}
type consumer struct{
	MqConnStr string `yaml:"mqConnStr"`
	Exchange string	`yaml:"exchange"`
	Queue string	`yaml:"queue"`
	RouterKey string	`yaml:"routerKey"`
	Kind string	`yaml:"kind"`
	AutoAck bool `yaml:"autoAck"`
	HandlePool int `yaml:"handlePool"`

	ReconnectionInfo *reconnectionInfo `yaml:"reconnectionInfo"`
	HandleErrRetryInfo *handleErrRetryInfo `yaml:"handleErrRetryInfo"`
}

type producer struct{
	MqConnStr string `yaml:"mqConnStr"`
	Exchange string	`yaml:"exchange"`
	RouterKey string	`yaml:"routerKey"`
	Kind string	`yaml:"kind"`

	ReconnectionInfo *reconnectionInfo `yaml:"reconnectionInfo"`
}

type reconnectionInfo struct{
	ReconnectionCounts int `yaml:"reconnectionCounts"`
	ReconnectionTime time.Duration `yaml:"reconnectionTime"`
}

type handleErrRetryInfo struct{
	HandleCount int	`yaml:"handleCount"`
	HandleTime time.Duration	`yaml:"handleTime"`
	IsRequeue bool	`yaml:"isRequeue"`
}



var (
	config *Conf
)

func init(){
	err:= readConf()
	if err !=nil{
		fmt.Println("read config err :",err)
		os.Exit(0)
	}
}

func readConf() error{
	ymlTest, err := ioutil.ReadFile("config.yml")
	if err!=nil{
		fmt.Println("read : ",err)
		return err
	}
	err= yaml.Unmarshal(ymlTest,&config)
	if err !=nil{
		fmt.Println(err)
		return err
	}
	return nil
}

func main(){
	if config == nil{
		fmt.Println("config read err ,shutdown")
		return
	}

	p:= callRabbitMQ.NewProducer(config.Producer.MqConnStr,config.Producer.Exchange,config.Producer.RouterKey,config.Producer.Kind)
	p.SetReconnectionInfo(10,time.Second*5)
	//go push(p)
	fmt.Println("=================+++")
	c := callRabbitMQ.NewConsumer(config.Consumer.MqConnStr,config.Consumer.Exchange,config.Consumer.Queue,config.Consumer.RouterKey,config.Consumer.Kind,config.Consumer.AutoAck,config.Consumer.HandlePool)
	c.SetReconnectionInfo(100) //心跳时间5秒
	c.SetRetryInfo(5,time.Second*1,false)
	c.RegisterHandleFunc(test1)
	c.Run()
}

func test(content amqp.Delivery)error{
	fmt.Println(" Recevice :",string(content.Body)," routerKey : ",content.RoutingKey)
	return nil
}


func test1(content amqp.Delivery)error{
	fmt.Println(" Recevice :",string(content.Body)," routerKey : ",content.RoutingKey,"  Header :",content.Headers)
	return errors.New("test err")
}

func push(p *callRabbitMQ.Producer){
	for i:=0;;i++ {
		time.Sleep(time.Millisecond * 1)
		err := p.Push(amqp.Publishing{
			Body:[]byte("test    "+strconv.Itoa(i)),
		})
		if err != nil {
			fmt.Println("producer :", err)
			return
		}
	}
	p.CloseConnection()
}