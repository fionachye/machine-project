/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include <cmath>

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
        MemberListEntry mEntry;
        mEntry.setid(*(int*)(&memberNode->addr.addr));
        mEntry.setport(*(short*)(&memberNode->addr.addr[4]));
        memberNode->memberList.push_back(MemberListEntry(mEntry));
        memberNode->myPos = memberNode->memberList.begin();
        memberNode->myPos->heartbeat += 1;
        memberNode->myPos->timestamp = par->getcurrtime();
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long);
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg) + sizeof(MessageHdr), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg) + sizeof(MessageHdr) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

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
    
    if (msgHdr.msgType == GOSSIP) {
        gossip_MessageHandler(data);
    }
}

void MP1Node::gossip_MessageHandler(char* data) {
    // { GOSSIP senderAddress memberListSize membershipList }
    Address senderAddress;
    memcpy(&senderAddress, data + sizeof(MessageHdr), sizeof(memberNode->addr));
    char* table = data + sizeof(MessageHdr) + sizeof(memberNode->addr);
    vector<MemberListEntry> mList = deserializeMembershipList(table);
    updateAndMergeMemberList(mList);
}

void MP1Node::joinreq_MessageHandler(char* data) {
    // get sender's address of this message
    Address fromAddr;
    long heartbeat;
    memcpy(&fromAddr, data + sizeof(int), sizeof(memberNode->addr.addr));
    memcpy(&heartbeat, data + sizeof(int) + sizeof(memberNode->addr.addr), sizeof(long));

    // serialize my membership table 
    size_t table_buffer_sz = memberNode->memberList.size() * (sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long));
    char *table = serializeMembershipList();
    
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
    // deserialize membership table from introducer
    vector<MemberListEntry> mList;
    char* table = data + sizeof(MessageHdr);
    mList = deserializeMembershipList(table);
    memberNode->memberList.assign(mList.begin(), mList.end());
    updateSelfEntry();
    // im in the group now
    memberNode->inGroup = true;
    return;
}

void MP1Node::updateAndMergeMemberList(vector<MemberListEntry> mList) {
    vector<MemberListEntry>::iterator it1;
    vector<MemberListEntry>::iterator it2;
    bool found_entry;
    
    // merge two tables, O(N^2) run time
    for (it1 = mList.begin(); it1 != mList.end(); it1++) {
        found_entry = false;
        for (it2 = memberNode->memberList.begin(); it2 != memberNode->memberList.end(); it2++ ) {
            if (it1->id == it2->id) {
                found_entry = true;
                if(it1->heartbeat > it2->heartbeat) {
                    // update the corresponding entry for this member
                    it2->port = it1->port;
                    it2->heartbeat = it1->heartbeat;
                    it2->timestamp = (long)par->getcurrtime();
                }
            }
            // ignore update if the heartbeat are the same or less
            // this member might have failed if its heartbeat has not increased after TFAIL seconds
            // we will verify this later in nodeopsloop
        }
        
        if (!found_entry) {
            // add a row if its not found
            MemberListEntry mEntry;
            mEntry.id = it1->id;
            mEntry.port = it1->port;
            mEntry.heartbeat = it1->heartbeat;
            mEntry.timestamp = (long)par->getcurrtime();
            memberNode->memberList.push_back(MemberListEntry(mEntry));
            Address entry_address = getMemberAdress(mEntry.id, mEntry.port);
            log->logNodeAdd(&(memberNode->addr), &entry_address);
        }
    }
    
    // while merging, if reallocation happens, the iterator that point to myPos is invalidated.
    // reassign myPos
    for (it1 = memberNode->memberList.begin(); it1 != memberNode->memberList.end(); it1++) {
        if (it1->id == *(int*)(&memberNode->addr.addr)) {
            memberNode->myPos = it1;
        }
    }
}

void MP1Node::updateSelfEntry() {
    vector<MemberListEntry>::iterator iterator;
    bool found_my_entry = false;
    
    for(iterator = memberNode->memberList.begin(); iterator != memberNode->memberList.end() && !found_my_entry; iterator++) {
        if (iterator->id == *(int*)(&memberNode->addr.addr)) {
            // update my row, remember my position
            iterator->port = *(short*)(&memberNode->addr.addr[4]);
            iterator->heartbeat = memberNode->heartbeat;
            iterator->timestamp = (long)par->getcurrtime();
            memberNode->myPos = iterator;
            found_my_entry = true;
        }
    }
    
    // add my row if its not there
    if (!found_my_entry) {
        MemberListEntry mEntry;
        mEntry.id = *(int*)(&memberNode->addr.addr);
        mEntry.port = *(short*)(&memberNode->addr.addr[4]);
        mEntry.heartbeat = memberNode->heartbeat;
        mEntry.timestamp = (long)par->getcurrtime();
        memberNode->memberList.push_back(MemberListEntry(mEntry));
        memberNode->myPos = memberNode->memberList.end();
        --memberNode->myPos;
    }
}

char* MP1Node::serializeMembershipList() {
    vector<MemberListEntry>::iterator iterator;
    vector<MemberListEntry> mList = memberNode->memberList;

    // serialize format: { id, port, heartbeat, timestamp }
    size_t entry_size = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);
    size_t buffer_sz = memberNode->memberList.size() * entry_size;
    char *entry = (char *) malloc(entry_size);    
    int index = 0;

    // buffer containing the whole table in byte
    char* buffer = (char *)malloc(buffer_sz);
   
    // serialize row by row and write into buffer
    for(iterator = mList.begin(); iterator != mList.end(); iterator++, index++) {
        memcpy(entry, &iterator->id, sizeof(int));
        memcpy(entry+sizeof(int), &iterator->port, sizeof(short));
        memcpy(entry+sizeof(int)+sizeof(short), &iterator->heartbeat, sizeof(long));
        memcpy(entry+sizeof(int)+sizeof(short)+sizeof(long), &iterator->timestamp, sizeof(long));
        memcpy(buffer+(entry_size*index), entry, entry_size);
    }
    
    free(entry);    
    
    return buffer;
}

vector<MemberListEntry> MP1Node::deserializeMembershipList(char* buffer) {
    // get num rows
    int num_rows;
    char* table = buffer + sizeof(int);
    vector<MemberListEntry> mList;
    MemberListEntry mEntry;
    size_t entry_sz = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);
    memcpy(&num_rows, buffer, sizeof(int));
    
    int id;
    short port;
    long heartbeat, timestamp;
    char* table_ptr = table;
    // deserialize row by row
    for (int i = 0; i < num_rows; i++) {
        table_ptr = table + i*entry_sz;
        memcpy(&id, table_ptr, sizeof(int));
        memcpy(&port, table_ptr + sizeof(int), sizeof(short));
        memcpy(&heartbeat, table_ptr + sizeof(int) + sizeof(short), sizeof(long));
        memcpy(&timestamp, table_ptr + sizeof(int) + sizeof(short) + sizeof(long), sizeof(long));
        mEntry.id = id;
        mEntry.port = port;
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
    // update my own heartbeat and my entry in the table
    memberNode->heartbeat += 1;
    memberNode->myPos->heartbeat = memberNode->heartbeat;
    memberNode->myPos->timestamp = (long)par->getcurrtime();
    
    // identify which member in the list has failed
    // delete entry if heartbeat has not increased for more than 2*TFAIL 
    // ignore if heartbeat has not increased after TFAIL but still less than 2*TFAIL
    vector<MemberListEntry> mList = memberNode->memberList;
    vector<MemberListEntry>::iterator it = mList.begin();
    while (it != mList.end()) {
        long time_difference = abs(par->getcurrtime() - it->timestamp);
        if (time_difference >= 2*TFAIL) {
            // log then delete this member
            Address entry_address = getMemberAdress(it->id,it->port);
            log->logNodeRemove(&memberNode->addr, &entry_address);
            it = mList.erase(it);
            continue;
        }
        it++;
    }
    
    // update number of neighbours
    int table_len = memberNode->memberList.size();
    int nnb = (table_len == 1) ? 0 : table_len/3 + 1;
    memberNode->nnb = nnb;
    
    // pick random neighbours and send them my updated membershipList
    // make sure don't select the same neighbour twice
    bool is_repeated[table_len];
    for (int i = 0; i < table_len; i++){
        is_repeated[i] = false;
    }
    
    int rand_nums[nnb];
    int rand_num;
    srand(time(NULL));
    for (int i = 0; i < nnb; ) {
        // generate random numbers from 0 to tablen_len-1
        rand_num = rand() % table_len;
        // check if this number has been generated before
        if (is_repeated[rand_num]) continue;
        else if (memberNode->memberList[rand_num].id == memberNode->myPos->id) continue;
        else {
            rand_nums[i] = rand_num;
            // mark this number in the boolean array
            is_repeated[i] = true;
            i += 1;
        }
    }
    
    // send my membership tables to these selected neighbours
    for (int i = 0; i < nnb; i++) {
        sendGossipToNeighbours(rand_nums[i]);
    }
    return;
}

void MP1Node::sendGossipToNeighbours(int neighbour_id) {
        MemberListEntry mEntry = memberNode->memberList[neighbour_id];
        Address toAddress = getMemberAdress(mEntry.id, mEntry.port);
        
        size_t tablesize = memberNode->memberList.size() * ( sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long) );
        // { MessageHdr myAddress memberListSize membershipList }
        size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(int) + tablesize;
        char* table = serializeMembershipList();
        MessageHdr *msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        int num_rows = (int)memberNode->memberList.size();
        // serialize format: { GOSSIP, memberListSize, membershipList }
        msg->msgType = GOSSIP;
        memcpy((char*)msg + sizeof(MessageHdr), &memberNode->addr, sizeof(memberNode->addr));
        memcpy((char*)msg + sizeof(MessageHdr) + sizeof(memberNode->addr), &num_rows, sizeof(int));
        memcpy((char*)msg + sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(int), table, tablesize);
        // send GOSSIP message randomly selected members from membership list
        emulNet->ENsend(&memberNode->addr, &toAddress, (char *)msg, msgsize);
        free(msg);
        free(table);
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

Address MP1Node::getMemberAdress(int id, short port) {
    Address address = Address();
    memcpy(&address.addr[0], &id, sizeof(int));
    memcpy(&address.addr[4], &port, sizeof(short));
    return address;
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
