# 调用rabbitMQ
方便调用MQ封装。

## Server
```
	p:= serverMq.NewProducer(mqConnStr,exchange,queue,queueKey,kind,true)
	p.Push([]byte("test"))

```
## Client
```
	c := clientMq.NewComsumer(mqConnStr,exchange,queue,queueKey,kind,true,2)
	c.RegisterHandleFunc(test)
	go c.Pull()   //阻塞方法
    // ---------------------------
	func test(content []byte)error{
    	fmt.Println(" Recevice :",string(content))
    	return nil
    }
```