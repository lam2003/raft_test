package service

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	serverId    int
	peerIds     []int
	peerClients map[int]*rpc.Client
	acceptConns []net.Conn
	rpcServer   *rpc.Server
	closeChan   chan struct{}
	wg          sync.WaitGroup
	listener    net.Listener
	mu          sync.Mutex
}

func NewServer(serverId int, peerIds []int) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.acceptConns = []net.Conn{}
	s.closeChan = make(chan struct{}, 1)
	return s
}

func (s *Server) Init() error {
	var err error
	s.listener, err = net.Listen("tcp", "localhost:"+fmt.Sprintf("%d", s.serverId))
	if err != nil {
		log.Printf("[%d] listen failed. %s\n", s.serverId, err.Error())
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.rpcServer = rpc.NewServer()
		s.rpcServer.Register(NewRaftCore())
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.closeChan:
					return
				default:
					log.Printf("[%d] accept failed. %s\n", s.serverId, err.Error())
					continue
				}
			}

			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				addAcceptConns := func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					s.acceptConns = append(s.acceptConns, conn)
				}

				addAcceptConns()
				s.rpcServer.ServeConn(conn)
			}()
		}
	}()

	return nil
}

func (s *Server) ConnectToPeers() {
	connectPeer := func(peerId int) error {
		client, err := rpc.Dial("tcp", "localhost:"+fmt.Sprintf("%d", peerId))
		if err != nil {
			log.Printf("[%d] connect to localhost:%d failed. %s\n", s.serverId, peerId, err.Error())
			return err
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		_, ok := s.peerClients[peerId]
		if ok {
			client.Close()
			log.Printf("[%d] peer[%d] already existed\n", s.serverId, peerId)
			return nil
		}
		s.peerClients[peerId] = client
		return nil
	}

	for _, peerId := range s.peerIds {
		for {
			select {
			case <-s.closeChan:
				return
			default:
				err := connectPeer(peerId)
				if err != nil {
					continue
				}
				log.Printf("[%d] connect to localhost:%d succeed\n", s.serverId, peerId)
			}
			break
		}
	}
}

func (s *Server) Close() {
	cleanAcceptConns := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, conn := range s.acceptConns {
			conn.Close()
		}
		s.acceptConns = []net.Conn{}
	}

	cleanPeerClients := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, cli := range s.peerClients {
			cli.Close()
		}
	}

	close(s.closeChan)
	s.listener.Close()
	cleanAcceptConns()
	cleanPeerClients()
	s.wg.Wait()

}

func (s *Server) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	// If this is called after shutdown (where client.Close is called), it will
	// return an error.
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}
}

// RPCProxy is a trivial pass-thru proxy type for ConsensusModule's RPC methods.
// It's useful for:
// - Simulating a small delay in RPC transmission.
// - Avoiding running into https://github.com/golang/go/issues/19957
// - Simulating possible unreliable connections by delaying some messages
//   significantly and dropping others when RAFT_UNRELIABLE_RPC is set.
// type RPCProxy struct {
// 	cm *ConsensusModule
// }

// func (rpp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
// 	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
// 		dice := rand.Intn(10)
// 		if dice == 9 {
// 			rpp.cm.dlog("drop RequestVote")
// 			return fmt.Errorf("RPC failed")
// 		} else if dice == 8 {
// 			rpp.cm.dlog("delay RequestVote")
// 			time.Sleep(75 * time.Millisecond)
// 		}
// 	} else {
// 		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
// 	}
// 	return rpp.cm.RequestVote(args, reply)
// }

// func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
// 	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
// 		dice := rand.Intn(10)
// 		if dice == 9 {
// 			rpp.cm.dlog("drop AppendEntries")
// 			return fmt.Errorf("RPC failed")
// 		} else if dice == 8 {
// 			rpp.cm.dlog("delay AppendEntries")
// 			time.Sleep(75 * time.Millisecond)
// 		}
// 	} else {
// 		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
// 	}
// 	return rpp.cm.AppendEntries(args, reply)
// }

type RaftCore struct {
}

type Request struct {
}

type Response struct {
}

func NewRaftCore() *RaftCore {
	return &RaftCore{}
}

func (c *RaftCore) AppendEntries(req Request, resp *Response) error {
	resp = &Response{}
	return nil
}

func (c *RaftCore) RequestVote(req Request, resp *Response) error {
	resp = &Response{}
	return nil
}
