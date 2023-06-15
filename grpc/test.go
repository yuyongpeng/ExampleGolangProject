package grpc

import (
	x "ExampleGolangPorject/mqtt"
	"fmt"
)

type Ts struct {
	A int
}

func Trd() {
	o := new(x.Opts)
	o.ToString()
	fmt.Println("abc")
}
