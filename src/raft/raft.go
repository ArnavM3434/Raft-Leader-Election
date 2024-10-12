package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"raft/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // this peer's index into peers[]
	dead  int32               // set by Kill()

	// Your data here (2A, 2B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// You may also need to add other state, as per your implementation.
	log                          []LogEntry
	currentTerm                  int //initialized to 0
	votedFor                     int //-1 for nil
	commitIndex                  int //initialized to 0 -> index of highest log entry known to be committed
	lastApplied                  int //initialized to 0 -> index of highest log entry applied to state machine
	currentRole                  int //follower is 0, candidate is 1, leader is 2
	electionTimeout              float64
	timeAtWhichLastResetOccurred float64
	votesReceived                int //keeps track of how many votes one got, initialized to 0

	//volatile, reinitialized after election
	nextIndex  map[int]int //for each follower, index of next log entry to send to that server - initialized to leader last log index + 1
	matchIndex map[int]int //for each follower, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	commit_channel chan ApplyMsg
}

type LogEntry struct {
	LogCommand        interface{}
	EntryReceivedTerm int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.currentRole == 2 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isleader
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".
	rf.mu.Lock()
	//fmt.Printf("Append Entries: Follower Process %d %v \n", rf.me, args.Entries)
	//fmt.Printf("follower %d has term %d \n", rf.me, rf.currentTerm)
	//fmt.Printf("Append Entries RPC Message: Follower %d %v \n", rf.me, args.Entries)
	// leadermoreuptodate := true
	// var lasttermf int
	// if rf.lastApplied > 0 {
	// 	lasttermf = rf.log[rf.lastApplied-1].EntryReceivedTerm
	// } else {
	// 	lasttermf = 0 //if nothing in the log, always want leader to get voted for, if leader also has nothing in the log it will win this check
	// }
	// if lasttermf > args.LastLogTerm || (lasttermf == args.LastLogTerm && rf.lastApplied > args.PrevLogIndex) {
	// 	leadermoreuptodate = false
	// }
	//fmt.Printf("")
	//want to make sure log of follower is as up to date as leader, if not, leader should remain (but its term should be the same)
	//fmt.Printf("Leader Term: %d Follower %d's Term: %d\n", args.Term, rf.me, rf.currentTerm)

	// if the current term of the follower is greater than that of the leader, do not accept the request
	if rf.currentTerm > args.Term { //&& leadermoreuptodate == false {
		reply.Term = max(rf.currentTerm, args.Term)
		reply.Success = false
		//fmt.Printf("follower %d has term which is bigger \n", rf.me)
		rf.mu.Unlock()
		//reply.FollowerMoreUpToDate = true
		return

	}
	//reply.FollowerMoreUpToDate = false
	//if rf.currentTerm <= args.Term { //<= might be an issue here
	reply.Term = args.Term
	if len(args.Entries) == 0 && rf.commitIndex >= args.LeaderCommit { //means it is a heartbeat
		reply.Success = true
		rf.currentTerm = args.Term
		rf.currentRole = 0
		rf.timeAtWhichLastResetOccurred = float64(time.Now().UnixNano()) / float64(1e9)
		rf.mu.Unlock()
		return
	} else { //not a heartbeat, just handle this as a completely different case for simplicity

		// for temp := 0; temp < len(args.Entries); temp++ {
		//fmt.Printf("Append Entries RPC Message: Follower %d %v \n", rf.me, args.Entries)
		// }

		rf.currentTerm = args.Term
		rf.currentRole = 0

		//check to see if follower has a previous index - if so, check if mathcing. if it does not have a previous index, also decrement
		if args.PrevLogIndex > 0 {
			if rf.lastApplied < args.PrevLogIndex || (rf.log[args.PrevLogIndex-1].EntryReceivedTerm != args.PrevLogTerm) {
				reply.Success = false
				rf.timeAtWhichLastResetOccurred = float64(time.Now().UnixNano()) / float64(1e9)
				rf.mu.Unlock()
				//fmt.Println("NOT WORKING")
				return

			}
		}

		//at this point can now update the log
		//just make a new slice for the old matching entries and the new ones, and assign that to rf.log

		// for temp := 0; temp < len(args.Entries); temp++ {
		// 	fmt.Printf("Append Entries: Follower Role %d Follower %d %v \n", rf.currentRole, rf.me, args.Entries[temp])
		// }
		//fmt.Printf("Append Entries: Follower Process %d %v \n", rf.me, args.Entries)

		copyUntil := args.PrevLogIndex
		var newSlice []LogEntry
		for k := 0; k <= min(copyUntil-1, len(rf.log)-1); k++ {
			newSlice = append(newSlice, rf.log[k])
		}

		for k := 0; k < len(args.Entries); k++ {
			newSlice = append(newSlice, args.Entries[k])

		}
		rf.log = newSlice

		rf.lastApplied = len(rf.log)
		//fmt.Printf("Logs ")
		// for temp := 0; temp < len(rf.log); temp++ {
		//fmt.Printf("Log Entries: Follower %d %v \n", rf.me, rf.log)
		// }

		//now update the commits - commit everthing after current commitIndex and up to and including args.commitIndex, then set commitIndex to args.commitIndex
		//fmt.Printf("FOLLWER COMMIT: follower %d follower index %d leader index %d", rf.me, rf.commitIndex, args.LeaderCommit)
		if rf.commitIndex < args.LeaderCommit {
			for k := rf.commitIndex + 1 - 1; k <= min(len(rf.log)-1, args.LeaderCommit-1); k++ {
				//fmt.Printf("CALLING APPLY COMMIT FOR %d\n", rf.me)
				rf.applycommit(rf.log[k], k+1)

			}
			//update commit index
			rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		}

		//return success
		reply.Success = true
		rf.timeAtWhichLastResetOccurred = float64(time.Now().UnixNano()) / float64(1e9)

	}

	//}

	rf.mu.Unlock()

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// Read the fields in "args",
	// and accordingly assign the values for fields in "reply".
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return

	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		rf.votedFor = -1
		rf.timeAtWhichLastResetOccurred = float64(time.Now().UnixNano()) / float64(1e9)
		rf.currentRole = 0

	}

	candidateMoreUpToDate := true
	var lasttermv int
	if rf.lastApplied > 0 {
		lasttermv = rf.log[rf.lastApplied-1].EntryReceivedTerm
	} else {
		lasttermv = -1 //if nothing in the log, always want leader to get voted for, if leader also has nothing in the log it will win this check
	}
	if lasttermv > args.LastLogTerm || (lasttermv == args.LastLogTerm && rf.lastApplied > args.LastLogIndex) {
		candidateMoreUpToDate = false
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateMoreUpToDate == true {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true

	} else {
		reply.VoteGranted = false
	}

	rf.mu.Unlock()

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	if ok && reply.VoteGranted == true {
		rf.votesReceived += 1
	}
	if rf.votesReceived > len(rf.peers)/2 && rf.currentRole == 1 { //now a leader
		//fmt.Printf("Process %d is New Leader", rf.me)
		rf.currentRole = 2
		rf.nextIndex = make(map[int]int)
		rf.matchIndex = make(map[int]int)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.lastApplied + 1
			rf.matchIndex[i] = 0
			if i == rf.me {
				rf.matchIndex[i] = rf.lastApplied
			}

		}

	}
	if ok && reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.currentRole = 0 //now a follower again
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	if ok == true {
		//fmt.Printf("Follower %d connected \n", server)
	}
	rf.mu.Lock()
	if ok && reply.Term > rf.currentTerm { //&& reply.FollowerMoreUpToDate == true {
		rf.currentTerm = reply.Term
		rf.currentRole = 0
		rf.mu.Unlock()
		return ok
	}
	rf.currentTerm = max(rf.currentTerm, reply.Term)

	//you've just received a response from a follower
	if ok && rf.currentRole == 2 {
		//if successful, update next index and match index
		if reply.Success == true {
			rf.nextIndex[server] = rf.lastApplied + 1
			//where must they be matching now?
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries) //MIGHT BE AN ISSUE, WHAT IF THIS JUST SENDS A HEARTBEAT?

			//fmt.Println("AppendEntries was success")

		}

		//if failed, decrement next index
		if reply.Success == false {
			rf.nextIndex[server] = rf.nextIndex[server] - 1
			//fmt.Println("PROPERLY CHANGING NEXT INDEX")
			rf.nextIndex[server] = max(1, rf.nextIndex[server])
			//fmt.Printf("Follower %d PrevLogIndex %d\n", server, args.PrevLogIndex)
		}

		//fmt.Printf("MATCH INDEX: %v\n", rf.matchIndex)

		//might need to commit new entries
		//for each entry starting at 1 above commitIndex to the end of the log, if it's term is the current term, check to see if majority of servers have a matching index greater than or equal to
		//if so, set commitIndex to this value, and have the leader commit these messages (this should only happen once) - MIGHT BE AN ISSUE HERE
		oldCommitIndex := rf.commitIndex
		for k := rf.commitIndex + 1 - 1; k <= len(rf.log)-1; k++ {
			//fmt.Println("Leader updating commits")
			if rf.log[k].EntryReceivedTerm == rf.currentTerm {
				numberOfUpToDate := 0
				for l := 0; l < len(rf.matchIndex); l++ {
					if rf.matchIndex[l] >= (k + 1) {
						numberOfUpToDate++
					}

				}
				//fmt.Printf("NUMBER UP TO DATE %d \n", numberOfUpToDate)
				if numberOfUpToDate > len(rf.peers)/2 {
					rf.commitIndex = k + 1
					//fmt.Printf("NUMBER OF UP TO DATE: %d COMMIT INDEX: %d\n", numberOfUpToDate, rf.commitIndex)
				}
			}

		}
		//leader should commit new entries once
		//fmt.Printf("COMMIT INDEX: %d \n", rf.commitIndex)
		if oldCommitIndex < rf.commitIndex {
			for k := oldCommitIndex + 1 - 1; k <= min(rf.commitIndex-1, len(rf.log)-1); k++ {

				rf.applycommit(rf.log[k], k+1)
			}
		}

	}

	rf.mu.Unlock()
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.currentRole != 2 {
		return -1, -1, false
	}
	rf.mu.Lock()
	index := len(rf.log) + 1
	term := rf.currentTerm
	isLeader := true

	//append log entry to leader
	rf.lastApplied = rf.lastApplied + 1
	rf.log = append(rf.log, LogEntry{LogCommand: command, EntryReceivedTerm: rf.currentTerm})
	rf.matchIndex[rf.me] = rf.lastApplied

	//fmt.Printf("Start Message: %d %d %v \n", rf.currentRole, rf.me, ApplyMsg{CommandValid: true, Command: command, CommandIndex: index})
	//fmt.Printf("%v\n", command)
	rf.mu.Unlock()

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	//fmt.Printf("Process %d Killed\n", rf.me)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.currentRole = 0
	rf.votesReceived = 0

	// Your initialization code here (2A, 2B).

	//election timeout
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	rf.electionTimeout = random.Float64()*0.5 + 0.5
	rf.timeAtWhichLastResetOccurred = rf.electionTimeout

	//2B
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	rf.commit_channel = applyCh

	go rf.kickOffElection()

	go rf.heartbeats()

	return rf
}

func (rf *Raft) kickOffElection() {

	for {

		rf.mu.Lock()
		if float64(time.Now().UnixNano())/float64(1e9)-rf.timeAtWhichLastResetOccurred > rf.electionTimeout && rf.currentRole != 2 { //kickoff an election if timeout and not already a leader
			rf.votesReceived = 0
			rf.timeAtWhichLastResetOccurred = float64(time.Now().UnixNano()) / float64(1e9)
			rf.currentTerm = rf.currentTerm + 1
			rf.currentRole = 1
			rf.votedFor = rf.me
			for i := 0; i < len(rf.peers); i++ {
				rvr := &RequestVoteReply{}
				rva := &RequestVoteArgs{}
				rva.CandidateId = rf.me
				rva.Term = rf.currentTerm
				rva.LastLogIndex = rf.lastApplied
				if rf.lastApplied > 0 {
					rva.LastLogTerm = rf.log[rf.lastApplied-1].EntryReceivedTerm
				} else {
					rva.LastLogTerm = 0
				}

				go rf.sendRequestVote(i, rva, rvr)

			}

		}
		rf.mu.Unlock()

		time.Sleep(5 * time.Millisecond)

	}

}

func (rf *Raft) heartbeats() {
	for { // infinite foor loop , allows the leader to send heartbeats at regular intervals
		if rf.currentRole == 2 { //if leader
			rf.mu.Lock() //LOCKING HERE MIGHT BE AN ISSUE
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me { //don't send heartbeat to yourself
					aer := &AppendEntriesReply{} //This struture is used to recieve the reply from a follower after sending the appened entries RPC
					aea := &AppendEntriesArgs{}  //This structure is used to fill the neccessary information for the appened entries RPC
					aea.Term = rf.currentTerm
					aea.LeaderId = rf.me

					//find out which log entries to send to the peer
					//This block of code prepares the log entries to be sent with the heartbeat. It calculates the range of log entries that need to be sent to each follower
					//based on what the follower might be missing. The entries from startIndex to endIndex are included in the Entries field of the AppendEntriesArgs.

					var entriesOut []LogEntry     // This slice will be populated with log entries that needs to be sent from the leader to follower
					startIndex := rf.nextIndex[i] // The next index will hold the index of the next log entry that the leader will send to each follower
					endIndex := rf.lastApplied    // The highest log position that has been applied to the state machine

					//append the entries into the log
					for j := startIndex - 1; j <= endIndex-1; j++ {
						entriesOut = append(entriesOut, rf.log[j])
					}
					aea.Entries = entriesOut

					// for temp := 0; temp < len(aea.Entries); temp++ {
					// 	fmt.Printf("Sending Message: %d %d %v \n", rf.currentRole, rf.me, aea.Entries[temp])
					// }

					aea.PrevLogIndex = rf.nextIndex[i] - 1
					if aea.PrevLogIndex > 0 {
						aea.PrevLogTerm = rf.log[aea.PrevLogIndex-1].EntryReceivedTerm
					}
					aea.LeaderCommit = rf.commitIndex

					//fmt.Printf("Sending Message to follower %d %v \n", i, aea.Entries)

					go rf.sendAppendEntries(i, aea, aer)
				}

			}
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)

		}
	}

}

func (rf *Raft) applycommit(entry LogEntry, index int) {

	rf.commit_channel <- ApplyMsg{CommandValid: true, Command: entry.LogCommand, CommandIndex: index}
	//fmt.Printf("COMMIT MESSAGE: Role %d Process %d %v \n", rf.currentRole, rf.me, ApplyMsg{CommandValid: true, Command: entry.LogCommand, CommandIndex: index})

}
