package main

import (
	"RaftTest/service"
	"fmt"
	"os"
	"os/signal"
)

func main() {

	server1 := service.NewServer(10001, []int{20001, 30001})
	server2 := service.NewServer(20001, []int{10001, 30001})
	server3 := service.NewServer(30001, []int{20001, 10001})

	servers := []*service.Server{
		server1,
		server2,
		server3,
	}

	for _, server := range servers {
		err := server.Init()
		if err != nil {
			return
		}
	}

	for _, server := range servers {
		server.ConnectToPeers()
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	sig := <-ch
	fmt.Printf("killed by %v", sig)

	for _, server := range servers {
		server.Close()
	}

}
