package core

import (
	"RaftTest/service"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

type Raft struct {
	currentTerm int
	votedFor    int
	logs        []*Log

	commitIndex int
	lastApplied int

	nextIndexs  []int
	matchIndexs []int

	serverId   int
	clusterNum int

	role Role

	taskChan      chan func() bool
	server        *service.Server
	electionTimer *time.Timer
}

func electionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond

}

func NewRaft(serverId int, clusterNum int, server *service.Server) *Raft {
	r := new(Raft)

	r.currentTerm = 0
	r.votedFor = -1
	r.logs = []*Log{}

	r.commitIndex = -1
	r.lastApplied = -1

	r.nextIndexs = make([]int, clusterNum)
	r.matchIndexs = make([]int, clusterNum)

	r.serverId = serverId
	r.clusterNum = clusterNum

	r.role = Follower

	r.taskChan = make(chan func() bool, r.clusterNum)
	r.server = server
	return r
}

func (r *Raft) Process() {
	r.becomeFollower(r.currentTerm, -1)

	for {
		switch r.role {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			return
		}
	}
}

func (r *Raft) becomeCandidate() {
	r.role = Candidate
	r.votedFor = r.serverId
	r.currentTerm++
	r.electionTimer = time.NewTimer(electionTimeout())

	log.Printf("[%d] become candidate, term[%d]\n", r.serverId, r.currentTerm)
}

func (r *Raft) becomeFollower(term int, voteFor int) {
	r.role = Follower
	r.votedFor = voteFor
	r.currentTerm = term
	r.electionTimer = time.NewTimer(electionTimeout())

	log.Printf("[%d] become follower, term[%d]\n", r.serverId, r.currentTerm)
}

func (r *Raft) becomeLeader() {
	r.role = Leader
	r.votedFor = -1
	log.Printf("[%d] become leader, term[%d]\n", r.serverId, r.currentTerm)
}

func (r *Raft) lastLogIndexAndTerm() (int, int) {
	n := len(r.logs)
	if n <= 0 {
		return -1, -1
	}
	log := r.logs[n-1]
	return log.index, log.term
}

func (r *Raft) startElection() {
	votesReceived := int32(1)
	lastLogIndex, lastLogTerm := r.lastLogIndexAndTerm()

	for i := 0; i < r.clusterNum; i++ {
		if i == r.serverId {
			continue
		}

		id := i
		rpc := func() {
			req := &VoteRequest{}
			req.CandidateId = r.serverId
			req.LastLogIndex = lastLogIndex
			req.LastLogTerm = lastLogTerm
			req.Term = r.currentTerm

			var resp VoteResponse
			err := r.server.Call(id+10000, "Raft.VoteRPC", req, &resp)
			if err != nil {
				log.Printf("[%d] call peer[%d]::VoteRPC failed. %s\n",
					r.serverId, id, err.Error())
				return
			}

			task := func() bool {

				if r.role != Candidate {
					return false
				}
				if resp.Term > req.Term {
					r.becomeFollower(resp.Term, id)
					return true
				}

				if resp.Granted {
					atomic.AddInt32(&votesReceived, 1)
					if int(atomic.LoadInt32(&votesReceived)) > ((r.clusterNum + 1) / 2) {
						r.becomeLeader()
						return true
					}
				}
				return false
			}
			r.taskChan <- task
		}
		go rpc()
	}
}

func (r *Raft) runFollower() {
	for {
		select {
		case <-r.electionTimer.C:
			r.becomeCandidate()
			return
		case f := <-r.taskChan:
			needToReturn := f()
			if needToReturn {
				return
			}
		}
	}
}

func (r *Raft) runCandidate() {
	r.startElection()
	for {
		select {
		case <-r.electionTimer.C:
		case f := <-r.taskChan:
			needToReturn := f()
			if needToReturn {
				return
			}
		}
	}
}

func (r *Raft) VoteRPC(req VoteRequest, resp *VoteResponse) error {
	joinChan := make(chan struct{}, 1)

	task := func() bool {
		defer func() {
			joinChan <- struct{}{}
		}()

		if req.Term > r.currentTerm {
			r.becomeFollower(req.Term, req.CandidateId)
			resp.Granted = true
			resp.Term = r.currentTerm
			return true
		}
		lastLogIndex, lastLogTerm := r.lastLogIndexAndTerm()

		if req.Term == r.currentTerm &&
			(r.votedFor == -1 || r.votedFor == req.CandidateId) &&
			((req.LastLogTerm > lastLogTerm) ||
				(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)) {
			r.becomeFollower(req.Term, req.CandidateId)
			resp.Granted = true
			resp.Term = r.currentTerm
			return true
		}
		return false
	}

	r.taskChan <- task
	<-joinChan

	log.Printf("[%d] recv vote from[%d]. req[%v], resp[%v]\n", r.serverId, req.CandidateId, req, resp)
	return nil
}
