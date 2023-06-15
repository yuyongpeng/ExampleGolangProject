package main

import (
	"ExampleGolangPorject/mqtt"
	"time"
)

func main() {
	ops := new(mqtt.Opts)
	ops.Broker = "tcp://mqtt.smartholder.jp:1883"
	ops.Topic = "test_yyp"
	ops.ClientId = "test_pub"
	ops.Username = "nft_mqtt_prod"
	ops.Password = "inmyshowD3"
	ops.CleanSession = false // 读取历史数据，必须设置为false
	ops.WriteTimeout = 10 * time.Second
	ops.ConnectTimeout = 5 * time.Second
	ops.MaxReconnectInterval = 3 * time.Second
	ops.AutoReconnect = true
	ops.ConnectRetry = true
	ops.ConnectRetryInterval = 2 * time.Second
	ops.PingTimeout = 5 * time.Second
	ops.KeepAlive = 6 * time.Second
	ops.ProtocolVersion = 4
	ops.Qos = 2

	ops.Pub("test msg")
}
