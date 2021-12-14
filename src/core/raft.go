package core

import (
	"RaftTest/service"
	"fmt"
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

	taskChan       chan func() bool
	server         *service.Server
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

func electionTimeout() time.Duration {
	return time.Duration(500+rand.Intn(500)) * time.Millisecond
}

func heartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
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

func (r *Raft) resetElectionTimer() {
	if r.electionTimer == nil {
		r.electionTimer = time.NewTimer(electionTimeout())
	} else {
		r.electionTimer.Stop()
		r.electionTimer.Reset(electionTimeout())
	}
}

func (r *Raft) resetHeartbeatTimer() {
	if r.heartbeatTimer == nil {
		r.heartbeatTimer = time.NewTimer(time.Duration(heartbeatTimeout()))
	} else {
		r.heartbeatTimer.Stop()
		r.heartbeatTimer.Reset(heartbeatTimeout())
	}
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
			r.runLeader()
			return
		}
	}
}

func (r *Raft) becomeCandidate() {
	r.role = Candidate
	r.votedFor = r.serverId
	r.currentTerm++
	r.resetElectionTimer()
	log.Printf("[%d] become candidate, term[%d]\n", r.serverId, r.currentTerm)
}

func (r *Raft) becomeFollower(term int, voteFor int) {
	r.role = Follower
	r.votedFor = voteFor
	r.currentTerm = term
	r.resetElectionTimer()
	log.Printf("[%d] become follower, term[%d]\n", r.serverId, r.currentTerm)
}

func (r *Raft) becomeLeader() {
	r.role = Leader
	r.votedFor = -1
	for idx := range r.nextIndexs {
		r.nextIndexs[idx] = len(r.logs)
		r.matchIndexs[idx] = 0
	}
	r.resetHeartbeatTimer()
	log.Printf("[%d] become leader, term[%d]\n", r.serverId, r.currentTerm)
}

func (r *Raft) lastLogIndexAndTerm() (int, int) {
	n := len(r.logs)
	if n <= 0 {
		return -1, -1
	}
	log := r.logs[n-1]
	return log.Index, log.Term
}

func (r *Raft) startElection() {
	votesReceived := int32(1)
	lastLogIndex, lastLogTerm := r.lastLogIndexAndTerm()
	currentTerm := r.currentTerm

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
			req.Term = currentTerm

			resp := &VoteResponse{}
			err := r.server.Call(id+10000, "Raft.VoteRPC", req, &resp)
			if err != nil {
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
					if int(atomic.LoadInt32(&votesReceived)) >= ((r.clusterNum + 1) / 2) {
						r.becomeLeader()
						return true
					}
				}
				return false
			}
			go func() { r.taskChan <- task }()
		}
		go rpc()
	}
}

func (r *Raft) Put(str string) {
	data := str
	task := func() bool {
		if r.role != Leader {
			return false
		}
		n := len(r.logs)
		log := &Log{
			Index: n,
			Term:  r.currentTerm,
			Data:  data,
		}
		r.logs = append(r.logs, log)
		return false
	}
	r.taskChan <- task
}

func (r *Raft) sendAppendEntries() {
	currentTerm := r.currentTerm
	for i := 0; i < r.clusterNum; i++ {
		if i == r.serverId {
			continue
		}

		id := i
		nextIndex := r.nextIndexs[id]
		prevLogIndex := nextIndex - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = r.logs[prevLogIndex].Term
		}

		n := len(r.logs)
		entries := make([]*Log, n-(prevLogIndex+1))
		idx := 0
		for j := prevLogIndex + 1; j < n; j++ {
			entries[idx] = r.logs[j]
			idx++
		}

		rpc := func() {
			req := &AppendEntriesRequest{}
			req.LeaderId = r.serverId
			req.LeaderCommitIndex = r.commitIndex
			req.PrevLogIndex = prevLogIndex
			req.PrevLogTerm = prevLogTerm
			req.Term = currentTerm
			req.Entries = entries

			resp := &AppendEntriesResponse{}
			err := r.server.Call(id+10000, "Raft.AppendEntriesRPC", req, resp)
			if err != nil {
				return
			}

			task := func() bool {
				if resp.Term > r.currentTerm {
					r.becomeFollower(resp.Term, id)
					return true
				}

				if r.role != Leader || resp.Term != r.currentTerm {
					return false
				}
				if !resp.Success {
					if resp.ConfilctTerm >= 0 {
						lastLogIndex, _ := r.lastLogIndexAndTerm()
						for i := lastLogIndex; i >= 0; i-- {
							if resp.ConfilctTerm == r.logs[i].Term {
								resp.ConfilctIndex = i + 1
								break
							}
						}
					}
					r.nextIndexs[id] = resp.ConfilctIndex
					return false
				} else {
					r.nextIndexs[id] = nextIndex + len(entries)
					r.matchIndexs[id] = r.nextIndexs[id] - 1
					return false
				}
			}
			go func() { r.taskChan <- task }()
		}
		go rpc()
	}
}

func (r *Raft) runLeader() {
	for {
		doSend := false
		select {
		case <-r.heartbeatTimer.C:
			r.resetHeartbeatTimer()
			doSend = true
		case f := <-r.taskChan:
			needToReturn := f()
			if needToReturn {
				return
			}
		}
		if doSend {
			r.sendAppendEntries()
		}
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

func (r *Raft) AppendEntriesRPC(req AppendEntriesRequest, resp *AppendEntriesResponse) error {
	joinChan := make(chan struct{}, 1)

	task := func() bool {
		defer func() {
			joinChan <- struct{}{}
		}()

		resp.Success = false
		resp.Term = r.currentTerm

		if req.Term > r.currentTerm {
			r.becomeFollower(req.Term, req.LeaderId)
			return true
		}

		lastLogIndex, _ := r.lastLogIndexAndTerm()
		if req.Term == r.currentTerm {
			if r.role != Follower {
				r.becomeFollower(req.Term, req.LeaderId)
				return true
			}

			if req.PrevLogIndex == -1 || (req.PrevLogIndex <= lastLogIndex && req.PrevLogTerm == r.logs[req.PrevLogIndex].Term) {
				newEntriesIndex := 0
				for i := req.PrevLogIndex + 1; i <= lastLogIndex; i++ {
					if r.logs[i].Term != req.Entries[i].Term {
						break
					}
					newEntriesIndex++
				}
				resp.Success = true
				resp.Term = r.currentTerm
				r.logs = append(r.logs[:req.PrevLogIndex+1+newEntriesIndex], req.Entries[newEntriesIndex:]...)

				str := fmt.Sprintf("[%d]", r.serverId)
				for _, log := range r.logs {
					str += log.Data + " "
				}
				log.Println(str)
			} else {
				confilctTerm := -1
				confilctIndex := -1

				if req.PrevLogIndex > lastLogIndex {
					confilctIndex = lastLogIndex + 1
				} else {
					confilctTerm = r.logs[req.PrevLogIndex].Term
					for i := req.PrevLogIndex - 1; i >= 0; i-- {
						if r.logs[i].Term != confilctTerm {
							confilctIndex = i + 1
							break
						}
					}
				}
				resp.ConfilctIndex = confilctIndex
				resp.ConfilctTerm = confilctTerm
			}

			r.resetElectionTimer()
		}

		return false
	}

	r.taskChan <- task
	<-joinChan

	return nil
}

func (r *Raft) VoteRPC(req VoteRequest, resp *VoteResponse) error {
	joinChan := make(chan struct{}, 1)

	task := func() bool {
		defer func() {
			joinChan <- struct{}{}
		}()

		resp.Granted = false
		resp.Term = r.currentTerm

		needQuit := false
		if req.Term > r.currentTerm {
			r.becomeFollower(req.Term, -1)
			needQuit = true
		}
		lastLogIndex, lastLogTerm := r.lastLogIndexAndTerm()

		if req.Term == r.currentTerm &&
			(r.votedFor == -1 || r.votedFor == req.CandidateId) &&
			((req.LastLogTerm > lastLogTerm) ||
				(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)) {
			r.votedFor = req.CandidateId
			resp.Granted = true
			needQuit = true
		} else {
			resp.Granted = false
		}
		resp.Term = r.currentTerm
		return needQuit
	}

	r.taskChan <- task
	<-joinChan

	log.Printf("[%d] recv vote from[%d]. req[%v], resp[%v]\n", r.serverId, req.CandidateId, req, resp)
	return nil
}
