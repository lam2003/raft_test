package main

import (
	"RaftTest/core"
	"RaftTest/service"
	"fmt"
	"os"
	"os/signal"
)

func main() {

	server1 := service.NewServer(10000, []int{10001, 10002})
	server2 := service.NewServer(10001, []int{10000, 10002})
	server3 := service.NewServer(10002, []int{10000, 10001})

	servers := []*service.Server{
		server1,
		server2,
		server3,
	}

	raftArr := []*core.Raft{}
	for idx, server := range servers {
		raft := core.NewRaft(idx, 3, server)
		err := server.Init(raft)
		if err != nil {
			return
		}
		raftArr = append(raftArr, raft)
	}

	for _, server := range servers {
		server.ConnectToPeers()
	}

	for _, raft := range raftArr {
		go raft.Process()
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, os.Kill)

	sig := <-ch
	fmt.Printf("killed by %v", sig)

	for _, server := range servers {
		server.Close()
	}

}
