package mqtt

import (
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"

	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Opts struct {
	Broker               string
	Topic                string
	ClientId             string
	Username             string
	Password             string
	CleanSession         bool
	WriteTimeout         time.Duration
	ConnectTimeout       time.Duration
	MaxReconnectInterval time.Duration
	AutoReconnect        bool
	ConnectRetry         bool
	ConnectRetryInterval time.Duration
	PingTimeout          time.Duration
	KeepAlive            time.Duration
	ProtocolVersion      uint
	Qos                  uint
}

func NewOpts(broker string, topic string,
	clientId string, username string, password string,
	cleanSession bool,
	writeTimeout time.Duration, connectTimeout time.Duration,
	maxReconnectInterval time.Duration, autoReconnect bool,
	connectRetry bool,
	connectRetryInterval time.Duration, pingTimeout time.Duration,
	keepalive time.Duration,
	protocolVersion uint, qos uint) Opts {
	return Opts{
		Broker:               broker,
		Topic:                topic,
		ClientId:             clientId,
		Username:             username,
		Password:             password,
		CleanSession:         cleanSession,
		WriteTimeout:         writeTimeout,
		ConnectTimeout:       connectTimeout,
		MaxReconnectInterval: maxReconnectInterval,
		AutoReconnect:        autoReconnect,
		ConnectRetry:         connectRetry,
		ConnectRetryInterval: connectRetryInterval,
		PingTimeout:          pingTimeout,
		KeepAlive:            keepalive,
		ProtocolVersion:      protocolVersion,
		Qos:                  qos,
	}
}

func (opts *Opts) ToString() {
	fmt.Printf("MQTT  Info:\n")
	fmt.Printf("\tbroker:    %s\n", opts.Broker)
	fmt.Printf("\tclientid:  %s\n", opts.ClientId)
	fmt.Printf("\tusername:  %s\n", opts.Username)
	fmt.Printf("\tpassword:  %s\n", opts.Password)
	fmt.Printf("\ttopic:     %s\n", opts.Topic)
	fmt.Printf("\tqos:       %d\n", opts.Qos)
	fmt.Printf("\tcleansess: %v\n", opts.CleanSession)
	//fmt.Printf("\tstore:     %s\n", *store)
}

/*
创建MQTT的 clientOptions
*/
func (opts *Opts) MakeMqttOpts() *MQTT.ClientOptions {
	mopts := MQTT.NewClientOptions()
	mopts.AddBroker(opts.Broker)
	mopts.SetClientID(opts.ClientId)
	mopts.SetUsername(opts.Username)
	mopts.SetPassword(opts.Password)

	mopts.SetCleanSession(opts.CleanSession)                 // ture：会无法获得历史消息，false：能获得历史消息
	mopts.SetWriteTimeout(opts.WriteTimeout)                 // 发布消息超时 发布消息阻塞时间, 0: 永远不超时
	mopts.SetConnectTimeout(opts.ConnectTimeout)             // 连接超时时间 在尝试打开与MQTT服务器的连接之前客户端超时和错误尝试之前等待的时间。持续时间为0永远不会超时。默认30秒。当前仅可用于TCP / TLS连接。
	mopts.SetMaxReconnectInterval(opts.MaxReconnectInterval) // 重连时间间隔 //连接断开后两次尝试重新连接之间等待的最长时间
	mopts.SetAutoReconnect(opts.AutoReconnect)               // 自动重连
	//配置在失败的情况下connect函数是否将自动重试连接（当为true时，Connect函数返回的令牌在连接建立或被取消之前不会完成）
	// 如果ConnectRetry为true，则应在OnConnect处理程序中请求订阅
	//将其设置为TRUE允许在建立连接之前发布消息
	mopts.SetConnectRetry(opts.ConnectRetry)                 // 连接重试
	mopts.SetConnectRetryInterval(opts.ConnectRetryInterval) // 如果ConnectRetry为TRUE，则在最初连接时两次连接尝试之间要等待的时间
	mopts.SetPingTimeout(opts.PingTimeout)                   // 向代理发送PING请求之后客户端确定连接丢失之前等待的时间（以秒为单位）。默认值为10秒。
	mopts.SetKeepAlive(opts.KeepAlive)                       // 向代理发送PING请求之前客户端应等待的时间（以秒为单位）
	mopts.SetProtocolVersion(opts.ProtocolVersion)
	//mopts.SetStore(MQTT.NewFileStore(":memory"))
	return mopts
}

func (opts *Opts) Pub(msg string) {
	mopts := MQTT.NewClientOptions()
	mopts.AddBroker(opts.Broker)
	mopts.SetClientID(opts.ClientId)
	mopts.SetUsername(opts.Username)
	mopts.SetPassword(opts.Password)

	mopts.SetCleanSession(opts.CleanSession)
	mopts.SetWriteTimeout(opts.WriteTimeout)                 // 发布消息超时
	mopts.SetConnectTimeout(opts.ConnectTimeout)             // 连接超时时间
	mopts.SetMaxReconnectInterval(opts.MaxReconnectInterval) // 重连时间间隔
	mopts.SetAutoReconnect(opts.AutoReconnect)               // 自动重连
	mopts.SetConnectRetry(opts.ConnectRetry)                 // d
	mopts.SetConnectRetryInterval(opts.ConnectRetryInterval) // 初次连接的时的尝试间隔，前提是设置了连接重试
	mopts.SetPingTimeout(opts.PingTimeout)
	mopts.SetKeepAlive(opts.KeepAlive)
	mopts.SetProtocolVersion(opts.ProtocolVersion)
	opts.ToString()

	client := MQTT.NewClient(mopts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	log.Println("== Publisher Started ==")
	incr := 0
	for {
		payload := fmt.Sprintf("%s=%d", msg, incr)
		token := client.Publish(opts.Topic, byte(opts.Qos), false, payload)
		log.Printf("publish message on topic: \"%s\"  Message: \"%s\"\n", opts.Topic, payload)
		token.Wait()
		time.Sleep(2 * time.Second)
		incr++
	}
	client.Disconnect(250)
	log.Println("Publisher Disconnected")

}

func onMessageReceived(client MQTT.Client, message MQTT.Message) {
	log.Printf("Received message on topic: \"%s\"  Message: \"%s\"\n", message.Topic(), message.Payload())
}
func (opts *Opts) Sub() {
	opts.MakeMqttOpts()

	mopts := MQTT.NewClientOptions()
	mopts.AddBroker(opts.Broker)
	mopts.SetClientID(opts.ClientId)
	mopts.SetUsername(opts.Username)
	mopts.SetPassword(opts.Password)

	mopts.SetCleanSession(opts.CleanSession)                 // ture：会无法获得历史消息，false：能获得历史消息
	mopts.SetWriteTimeout(opts.WriteTimeout)                 // 发布消息超时 发布消息阻塞时间, 0: 永远不超时
	mopts.SetConnectTimeout(opts.ConnectTimeout)             // 连接超时时间 在尝试打开与MQTT服务器的连接之前客户端超时和错误尝试之前等待的时间。持续时间为0永远不会超时。默认30秒。当前仅可用于TCP / TLS连接。
	mopts.SetMaxReconnectInterval(opts.MaxReconnectInterval) // 重连时间间隔 //连接断开后两次尝试重新连接之间等待的最长时间
	mopts.SetAutoReconnect(opts.AutoReconnect)               // 自动重连
	//配置在失败的情况下connect函数是否将自动重试连接（当为true时，Connect函数返回的令牌在连接建立或被取消之前不会完成）
	// 如果ConnectRetry为true，则应在OnConnect处理程序中请求订阅
	//将其设置为TRUE允许在建立连接之前发布消息
	mopts.SetConnectRetry(opts.ConnectRetry)                 // 连接重试
	mopts.SetConnectRetryInterval(opts.ConnectRetryInterval) // 如果ConnectRetry为TRUE，则在最初连接时两次连接尝试之间要等待的时间
	mopts.SetPingTimeout(opts.PingTimeout)                   // 向代理发送PING请求之后客户端确定连接丢失之前等待的时间（以秒为单位）。默认值为10秒。
	mopts.SetKeepAlive(opts.KeepAlive)                       // 向代理发送PING请求之前客户端应等待的时间（以秒为单位）
	mopts.SetProtocolVersion(opts.ProtocolVersion)
	//mopts.SetStore(MQTT.NewFileStore(":memory"))
	opts.ToString()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	//mopts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
	//	log.Println("== get me == ", time.Now())
	//	//choke <- [2]string{msg.Topic(), string(msg.Payload())}
	//})
	mopts.SetDefaultPublishHandler(onMessageReceived)
	// 在客户端连接到broker后被调用，在初次连接和自动重新连接后都会被调用
	mopts.SetOnConnectHandler(func(client MQTT.Client) {
		log.Println("== connect to broker == ")
		if token := client.Subscribe(opts.Topic, byte(opts.Qos), onMessageReceived); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	})
	//连接丢失处理回调
	mopts.SetConnectionLostHandler(func(client MQTT.Client, err error) {
		log.Println("== lost connection == ")
	})
	//重连处理回调 (初始连接丢失后，在重新连接之前调用)
	mopts.SetReconnectingHandler(func(client MQTT.Client, opt *MQTT.ClientOptions) {
		log.Println("== reconnecting == ")
	})

	client := MQTT.NewClient(mopts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := client.Subscribe(opts.Topic, byte(opts.Qos), onMessageReceived); token.Wait() && token.Error() != nil {
		log.Println(token.Error())
		os.Exit(1)
	}

	<-c
}
