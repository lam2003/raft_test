package main

import (
	"RaftTest/service"
)

func main() {
	var ss service.Server
	ss.Serve()
}
