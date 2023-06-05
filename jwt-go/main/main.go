package main

import "fmt"

func main(){
	fmt.Println("1111111111")
	// HMAC-SHA256 使用私钥对数据签名
	ExampleNewWithClaims_standardClaims()
	ExampleNewWithClaims_customClaimsType()
	// 反解 HMAC-SHA256的数据
	ExampleParseWithClaims_customClaimsType()
	ExampleParse_errorChecking()

	fmt.Println("2222222222")
	// hmac_example
	ExampleNew_hmac()
	ExampleParse_hmac()
}