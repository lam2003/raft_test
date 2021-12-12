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
