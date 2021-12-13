package core

type VoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type VoteResponse struct {
	Term    int
	Granted bool
}

type AppendEntriesRequest struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []*Log
	LeaderCommitIndex int
}

type AppendEntriesResponse struct {
	Term          int
	Success       bool
	ConfilctIndex int
	ConfilctTerm  int
}
