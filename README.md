# 调用rabbitMQ

方便调用MQ封装的一些方法。加入了连接断开重连和消费者ACK机制。



## Producer
```
	p:= callRabbitMQ.NewProducer(config.Producer.MqConnStr,config.Producer.Exchange,config.Producer.RouterKey,config.Producer.Kind)
	p.SetReconnectionInfo(10,time.Second*5)
    p.Push([]byte("test"))
```


## Consumer
```
	c := callRabbitMQ.NewConsumer(mqConnStr,exchange,queue,queueKey,kind,true,20)
	c.RegisterHandleFunc(test1)
    c.SetReconnectionInfo(100) //心跳时间5秒
    c.SetRetryInfo(5,time.Second*1,false)
	c.Run()
    // ---------------------------
    func test1(content amqp.Delivery)error{
        fmt.Println(" Recevice :",string(content.Body)," routerKey : ",content.RoutingKey)
        return errors.New("test err")
    }
```