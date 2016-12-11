/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
        initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        
        // add my entry to my membership table
        memberNode->memberList.push_back(MemberListEntry());
        memberNode->myPos = memberNode->memberList.begin();
        memberNode->myPos->id = *(int*)(&memberNode->addr.addr);
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg) + sizeof(int), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg) + sizeof(int) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    MessageHdr msgHdr = *(MessageHdr *)data;

    if (msgHdr.msgType == JOINREQ) {
        joinreq_MessageHandler(data);
    }
    
    if (msgHdr.msgType == JOINREP) {
        joinrep_MessageHandler(data);
    }
}

void MP1Node::joinreq_MessageHandler(char* data) {
    // get sender's address of this message
    Address fromAddr;
    memcpy(&fromAddr, data + sizeof(int), sizeof(memberNode->addr.addr));
    
    // serialize my membership table 
    size_t table_buffer_sz = memberNode->memberList.size() * (sizeof(int) + sizeof(long) + sizeof(long));
    char* table = serializeMembershipList(table_buffer_sz);
    
    // send JOINREP message
    // serialize format: { JOINREP, memberListSize, membershipList }
    MessageHdr *msg;
    int num_rows = (int)memberNode->memberList.size();
    size_t buffer_sz = sizeof(MessageHdr) + sizeof(int) + table_buffer_sz;
    msg = (MessageHdr *) malloc(buffer_sz * sizeof(char));
    msg->msgType = JOINREP;
    memcpy((char*)msg + sizeof(MessageHdr), &num_rows, sizeof(int));
    memcpy((char*)msg + sizeof(MessageHdr) + sizeof(int), table, table_buffer_sz);
    
    emulNet->ENsend(&memberNode->addr, &fromAddr, (char *)msg, buffer_sz);
    free(table);
    free(msg);
    
    return;
}

void MP1Node::joinrep_MessageHandler(char* data) {
    // deserialize membership table 
    vector<MemberListEntry> mList;
    char* table = data + sizeof(MessageHdr);
    mList = deserializeMembershipList(table);
    memberNode->memberList = mList;
    // add my own row to this table
    MemberListEntry mEntry;
    mEntry.id = *(int*)(&memberNode->addr.addr);
    mEntry.heartbeat = memberNode->heartbeat;
    mEntry.timestamp = (long)par->getcurrtime();
    memberNode->memberList(MemberListEntry(mEntry));
    return;
}

char* MP1Node::serializeMembershipList(size_t buffer_sz) {
    vector<MemberListEntry>::iterator iterator;
    vector<MemberListEntry> mList = memberNode->memberList;
    // serialize format: { id, heartbeat, timestamp }
    size_t entry_size = sizeof(int) + sizeof(long) + sizeof(long);
    char *entry = (char *) malloc(entry_size);
    int index = 0;
    // buffer containing the whole table in byte
    char* buffer = (char *)malloc(buffer_sz);
   
    // serialize row by row and write into buffer
    for(iterator = mList.begin(); iterator != mList.end(); iterator++, index++) {
        memcpy(entry, &iterator->id, sizeof(int));
        memcpy(entry+sizeof(int), &iterator->heartbeat, sizeof(long));
        memcpy(entry+sizeof(int)+sizeof(long), &iterator->timestamp, sizeof(long));
        memcpy(buffer+(index*entry_size), entry, entry_size);  
    }
    free(entry);
    return buffer;
}

vector<MemberListEntry> MP1Node::deserializeMembershipList(char* buffer) {
    // get num rows
    int num_rows;
    char* table = buffer+sizeof(int);
    vector<MemberListEntry> mList;
    MemberListEntry mEntry;
    
    memcpy(&num_rows, buffer, sizeof(int));
    
    int id; 
    long heartbeat, timestamp;
    // deserialize row by row
    for (int i = 0; i < num_rows; i++) {
        memcpy(&id, table, sizeof(int));
        memcpy(&heartbeat, table + sizeof(int), sizeof(long));
        memcpy(&timestamp, table + sizeof(int) + sizeof(long), sizeof(long));
        mEntry.id = id;
        mEntry.heartbeat = heartbeat;
        mEntry.timestamp = timestamp;
        mList.push_back(MemberListEntry(mEntry));
    }
    
    return mList;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
