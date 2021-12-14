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

func (s *Server) Init(obj interface{}) error {
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
		s.rpcServer.Register(obj)
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
	peer := s.peerClients[id]
	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	} else {
		err := peer.Call(serviceMethod, args, reply)
		if err != nil {
			peer.Close()

			client, err := rpc.Dial("tcp", "localhost:"+fmt.Sprintf("%d", id))
			if err != nil {
				// log.Printf("[%d] connect to localhost:%d failed. %s\n", s.serverId, id, err.Error())
				return err
			}

			f := func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.peerClients[id] = client
			}
			f()
			err = client.Call(serviceMethod, args, reply)

		}
		return err
	}
}
