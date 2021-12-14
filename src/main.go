package main

import (
	"RaftTest/core"
	"RaftTest/service"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"
)

var id int
var clusterNum int

func main() {

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	flag.IntVar(&id, "id", 0, "id")
	flag.IntVar(&clusterNum, "cluster_num", 0, "cluster_num")
	flag.Parse()

	rand.Seed(time.Now().Unix())
	serverId := 10000 + id
	peerIds := []int{}
	for i := 0; i < clusterNum; i++ {
		if i == id {
			continue
		}
		peerIds = append(peerIds, i+10000)
	}

	server := service.NewServer(serverId, peerIds)
	raft := core.NewRaft(id, clusterNum, server)

	err := server.Init(raft)
	if err != nil {
		log.Println(err)
	}

	server.ConnectToPeers()
	go raft.Process()

	idx := 0
	for {
		time.Sleep(time.Second)
		raft.Put(fmt.Sprintf("hello_%d", idx))
		idx++
	}
}
