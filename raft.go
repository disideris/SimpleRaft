package raft

import "sync"
import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"math/rand"
	"sync/atomic"
	"time"
)

type ApplyMessage struct {
	Index       int
	Command     interface{}
	UseSnapshot bool
	Snapshot    []byte
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu               sync.Mutex
	peers            []*labrpc.ClientEnd
	persister        *Persister
	me               int // index into peers[]
	applyCh          chan ApplyMessage
	currentTerm      int
	votedFor         int
	logs             []map[string]interface{}
	commitIndex      int
	lastApplied      int
	nextIndex        []int
	matchIndex       []int
	role             int //0-follower, 1-candidate, 2-leader
	electionTimeout  time.Duration
	heartBeatTimeout time.Duration
	heartBeatTicker  *time.Ticker
	lastHeart        time.Time
	running          bool
	counter          int64
	showLog          bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	if rf.showLog {
		fmt.Println(rf.me, "says: my current state, {", rf.currentTerm, ",", rf.votedFor, ",", rf.logs, "}")
	}
	return rf.currentTerm, rf.role == 2
}

func (rf *Raft) GetState2() (bool, []map[string]interface{}, int) {
	if rf.showLog {
		fmt.Println(rf.me, "says: current state, {", rf.currentTerm, ",", rf.votedFor, ",", rf.logs, "}")
	}
	return rf.role == 2, rf.logs, rf.commitIndex
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []map[string]interface{}
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Len     int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		if rf.showLog {
			fmt.Println(rf.me, "tells", args.CandidateId, ": you are to late, highest term is", rf.currentTerm)
		}
	} else {
		if args.Term > rf.currentTerm {
			rf.role = 0
			rf.votedFor = -1
			rf.currentTerm = args.Term
			if rf.showLog {
				fmt.Println(rf.me, "says: higher term detected, term=", args.Term)
			}
		}

		lastLogTerm := 0
		if len(rf.logs) > 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1]["term"].(int)
		}
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && len(rf.logs) <= args.LastLogIndex)) {
			reply.VoteGranted = true
			rf.role = 0
			rf.votedFor = args.CandidateId
			rf.lastHeart = time.Now()
			if rf.showLog {
				fmt.Println(rf.me, "tells", args.CandidateId, ": vote granted")
			}
		} else {
			reply.VoteGranted = false
			if rf.showLog {
				fmt.Println(rf.me, "tells", args.CandidateId, ": vote rejected")
			}
		}
	}
	rf.persist()
	reply.Term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.showLog {
		fmt.Println(rf.me, "tells", server, ": vote me, ", args)
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.role = 0
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
		}
	}
	return ok && reply.VoteGranted
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		if rf.showLog {
			fmt.Println(rf.me, "tells", args.LeaderId, ": you are too late")
		}
	} else {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.role = 0
		rf.votedFor = args.LeaderId

		if args.PrevLogIndex > 0 {
			if len(rf.logs) >= args.PrevLogIndex && rf.logs[args.PrevLogIndex-1]["term"] == args.PrevLogTerm {
				reply.Success = true
			} else {
				reply.Success = false
				reply.Len = len(rf.logs)
				rf.lastHeart = time.Now()
			}
		}
	}
	reply.Term = rf.currentTerm
	if reply.Success {
		rf.logs = rf.logs[0:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)
		for iter := rf.commitIndex; iter < args.LeaderCommit; iter++ {
			command := rf.logs[iter]["command"]
			if rf.showLog {
				fmt.Println(rf.me, "says: commit", command, "index=", iter+1)
			}
			rf.applyCh <- ApplyMessage{Index: iter + 1, Command: command, UseSnapshot: false}
		}
		rf.commitIndex = args.LeaderCommit
		rf.lastHeart = time.Now()
	}
	rf.persist()
	if rf.showLog {
		fmt.Println(rf.me, "tells", args.LeaderId, ": pong,", reply)
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if rf.showLog {
		fmt.Println(rf.me, "tells", server, ": ping,", args)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.role = 0
			rf.votedFor = -1
			rf.persist()
			rf.mu.Unlock()
		}
		if ok && rf.role == 2 && !reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex
			if reply.Len < args.PrevLogIndex {
				rf.nextIndex[server] = reply.Len + 1
			}
			rf.nextIndex[server] = 1
			if rf.showLog {
				fmt.Println(rf.me, "says: decrease nextindex of", server, "to", rf.nextIndex[server])
			}
		}
	}
	return ok && reply.Success
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.logs) + 1
	if rf.role == 2 {
		log := map[string]interface{}{"command": command, "term": rf.currentTerm}
		rf.logs = append(rf.logs, log)
		rf.persist()
		if rf.showLog {
			fmt.Println(rf.me, "says:", "new command", command, "in term", rf.currentTerm)
		}
	} else {
		fmt.Println(rf.me, "says: I am not a leader")
	}
	return index, rf.currentTerm, rf.role == 2
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	/* stop the world */
	rf.running = false
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMessage messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMessage) *Raft {

	fmt.Println(me, "says:", "hello world!")

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.running = true

	rf.showLog = true

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here.
	rf.electionTimeout = time.Duration(rand.Intn(100)+100) * time.Millisecond
	rf.heartBeatTimeout = time.Duration(rand.Intn(50)+50) * time.Millisecond
	rf.counter = 0
	rf.lastHeart = time.Now()
	rf.heartBeatTicker = time.NewTicker(rf.heartBeatTimeout)

	rf.role = 0 //start in follower state

	rf.commitIndex = 0
	rf.lastApplied = 0

	go func() {
		for {
			if !rf.running {
				break
			}
			/* wait timeout */
			time.Sleep(rf.electionTimeout - time.Since(rf.lastHeart))

			if rf.role != 2 && time.Since(rf.lastHeart) >= rf.electionTimeout {
				rf.mu.Lock()
				// re-generate time
				rf.electionTimeout = time.Duration(rand.Intn(100)+100) * time.Millisecond
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.role = 1
				rf.persist()
				rf.mu.Unlock()
				rf.doVote()
			}
			/* sleep at most electionTimeout duration */
			if time.Since(rf.lastHeart) >= rf.electionTimeout {
				rf.lastHeart = time.Now()
			}
		}
		fmt.Println(rf.me, "says: bye~")
	}()
	return rf
}

func (rf *Raft) doVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var agreed int64 = 1
	index := len(rf.logs)
	term := 0
	if index != 0 {
		term = rf.logs[index-1]["term"].(int)
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(peer int, currTerm int, index int, term int) {
				args := RequestVoteArgs{Term: currTerm, CandidateId: rf.me, LastLogIndex: index, LastLogTerm: term}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, args, &reply)
				rf.mu.Lock()
				if ok && args.Term == rf.currentTerm && rf.role == 1 {
					atomic.AddInt64(&agreed, 1)
					if int(agreed)*2 > len(rf.peers) {
						rf.role = 2

						rf.nextIndex = rf.nextIndex[0:0]
						rf.matchIndex = rf.matchIndex[0:0]
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex = append(rf.nextIndex, len(rf.logs)+1)
							rf.matchIndex = append(rf.matchIndex, 0)
						}
						/* persist state */
						rf.persist()
						go rf.doSubmit()
						if rf.showLog {
							fmt.Println(rf.me, "says:", "I am the leader in term", rf.currentTerm)
						}
					}
				}
				rf.mu.Unlock()
			}(i, rf.currentTerm, index, term)
		}
	}
}

func (rf *Raft) doSubmit() {
	/* ensure only one thread is running */
	if atomic.AddInt64(&rf.counter, 1) > 1 {
		atomic.AddInt64(&rf.counter, -1)
		return
	}
	for range rf.heartBeatTicker.C {
		if !rf.running {
			break
		}
		if rf.role != 2 {
			break
		}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(peer int) {
					rf.mu.Lock()
					index := rf.nextIndex[peer]
					term := 0
					/* TODO: limit entries max size */
					entries := make([]map[string]interface{}, 0)
					if len(rf.logs) >= index {
						entries = append(entries, rf.logs[index-1:]...)
					}
					if index > 1 {
						//fmt.Println(index, rf.logs)
						term = rf.logs[index-2]["term"].(int)
					}
					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: index - 1, PrevLogTerm: term, Entries: entries, LeaderCommit: rf.commitIndex}
					reply := AppendEntriesReply{}
					rf.nextIndex[peer] = args.PrevLogIndex + len(entries) + 1
					rf.mu.Unlock()

					ok := rf.sendAppendEntries(peer, args, &reply)

					rf.mu.Lock()
					if ok && args.Term == rf.currentTerm && rf.role == 2 {
						rf.matchIndex[peer] = args.PrevLogIndex + len(entries)
						for iter := rf.commitIndex; iter < len(rf.logs); iter++ {
							if rf.logs[iter]["term"].(int) < rf.currentTerm {
								continue
							}
							count := 1
							for j := 0; j < len(rf.peers); j++ {
								if j != rf.me {
									if rf.matchIndex[j] > iter {
										count++
									}
									//fmt.Println(j, rf.matchIndex[j], count)
								}
							}

							if count*2 > len(rf.peers) {
								for i := rf.commitIndex; i <= iter; i++ {
									rf.commitIndex = i + 1
									command := rf.logs[i]["command"]
									if rf.showLog {
										fmt.Println(rf.me, "says: ", command, "is committed, index=", i+1)
									}
									rf.applyCh <- ApplyMessage{Index: i + 1, Command: command, UseSnapshot: false}
								}
							} else {
								break
							}
						}
					}
					rf.mu.Unlock()
				}(i)
			}
		}
	}
	if rf.showLog {
		fmt.Println(rf.me, "says: stop heart beat")
	}
	atomic.AddInt64(&rf.counter, -1)
}
