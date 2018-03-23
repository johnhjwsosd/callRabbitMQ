# 调用rabbitMQ

方便调用MQ封装的一些方法。加入了连接断开重连和消费者ACK机制。



## Producer
```
    p:= callRabbitMQ.NewProducer(mqConnStr,exchange,queue,queueKey,kind)
    p.SetReconnectionInfo(10,time.Second*10)
    p.Push([]byte("test"))
```

参数

## Consumer
```
	c := callRabbitMQ.NewConsumer(mqConnStr,exchange,queue,queueKey,kind,true,20)
	c.RegisterHandleFunc(test1)
    c.SetReconnectionInfo(100) //心跳时间5秒
    c.SetRetryInfo(5,time.Second*1,false)
	c.Run()
    // ---------------------------
	func test(content []byte)error{
    	fmt.Println(" Recevice :",string(content))
    	return nil
    }
```