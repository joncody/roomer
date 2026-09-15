--------------------------- MODULE roomer ---------------------------
EXTENDS Integers, Sequences, TLC

CONSTANTS 
    Clients,   \* Set of active client IDs (e.g. {c1, c2})
    Rooms      \* Set of chat room IDs (e.g. {r1, r2})

VARIABLES
    connections,       \* Set of connected client IDs
    memberships,       \* Mapping of Room -> Set of Clients in it (local node)
    presence_sets,     \* Mapping of Room -> Set of Clients in Redis (SET-based presence)
    client_buffers,    \* Mapping of Client -> Sequence of 12-byte wire messages
    control_tokens     \* Mapping of Client -> Nat (token bucket for join/leave)

vars == <<connections, memberships, presence_sets, client_buffers, control_tokens>>

MaxTokens == 5
RefillAmount == 2

---------------------------------------------------------------------

\* 12-byte wire header contract:
\* [1B version][1B flags][2B room_len][room][2B event_len][event][1B dst_len][dst][1B src_len][src][4B payload_len][payload]
WirePacket == [
    version: {1},
    flags: {0},
    room_len: 0..65535,
    event_len: 0..65535,
    dst_len: 0..255,
    src_len: 0..255,
    payload_len: Nat,
    room: Rooms \union {"root", ""},
    event: STRING,
    dst: Clients \union {""},
    src: Clients \union {""},
    payload: STRING
]

\* Type invariant: ensures variables stay within expected types and ranges
TypeOK ==
    /\ connections \subseteq Clients
    /\ memberships \in [Rooms -> SUBSET Clients]
    /\ presence_sets \in [Rooms -> SUBSET Clients]
    /\ client_buffers \in [Clients -> Seq(WirePacket)]
    /\ control_tokens \in [Clients -> 0..MaxTokens]

\* Initial state of the system
Init ==
    /\ connections = {}
    /\ memberships = [r \in Rooms |-> {}]
    /\ presence_sets = [r \in Rooms |-> {}]
    /\ client_buffers = [c \in Clients |-> <<>>]
    /\ control_tokens = [c \in Clients |-> MaxTokens]

---------------------------------------------------------------------
\* Actions

Connect(c) ==
    /\ c \notin connections
    /\ connections' = connections \union {c}
    /\ control_tokens' = [control_tokens EXCEPT ![c] = MaxTokens]
    /\ UNCHANGED <<memberships, presence_sets, client_buffers>>

Disconnect(c) ==
    /\ c \in connections
    /\ connections' = connections \ {c}
    /\ memberships' = [r \in Rooms |-> memberships[r] \ {c}]
    /\ presence_sets' = [r \in Rooms |-> presence_sets[r] \ {c}]
    /\ client_buffers' = [client_buffers EXCEPT ![c] = <<>>]
    /\ control_tokens' = [control_tokens EXCEPT ![c] = 0]

RefillTokens(c) ==
    /\ c \in connections
    /\ control_tokens[c] < MaxTokens
    /\ control_tokens' = [control_tokens EXCEPT ![c] = 
                            IF control_tokens[c] + RefillAmount > MaxTokens 
                            THEN MaxTokens 
                            ELSE control_tokens[c] + RefillAmount]
    /\ UNCHANGED <<connections, memberships, presence_sets, client_buffers>>

JoinRoom(c, r) ==
    /\ c \in connections
    /\ c \notin memberships[r]
    /\ control_tokens[c] > 0  \* Token-bucket rate limiter guard
    /\ control_tokens' = [control_tokens EXCEPT ![c] = control_tokens[c] - 1]
    /\ memberships' = [memberships EXCEPT ![r] = memberships[r] \union {c}]
    /\ presence_sets' = [presence_sets EXCEPT ![r] = presence_sets[r] \union {c}]  \* Redis SADD
    /\ UNCHANGED <<connections, client_buffers>>

LeaveRoom(c, r) ==
    /\ c \in connections
    /\ c \in memberships[r]
    /\ control_tokens[c] > 0  \* Token-bucket rate limiter guard
    /\ control_tokens' = [control_tokens EXCEPT ![c] = control_tokens[c] - 1]
    /\ memberships' = [memberships EXCEPT ![r] = memberships[r] \ {c}]
    /\ presence_sets' = [presence_sets EXCEPT ![r] = presence_sets[r] \ {c}]  \* Redis SREM
    /\ UNCHANGED <<connections, client_buffers>>

ExpireAbandonedRoom(r) ==
    /\ memberships[r] = {}
    /\ presence_sets[r] /= {}
    /\ presence_sets' = [presence_sets EXCEPT ![r] = {}]  \* Redis key TTL expiration
    /\ UNCHANGED <<connections, memberships, client_buffers, control_tokens>>

Broadcast(c, r, msg) ==
    /\ c \in connections
    /\ c \in memberships[r]
    /\ client_buffers' = [dest \in Clients |->
                             IF dest \in memberships[r] /\ dest /= c
                             THEN Append(client_buffers[dest], [
                                     version |-> 1,
                                     flags |-> 0,
                                     room_len |-> 5,
                                     event_len |-> 4,
                                     dst_len |-> 0,
                                     src_len |-> 8,
                                     payload_len |-> 4,
                                     room |-> r,
                                     event |-> "chat",
                                     dst |-> "",
                                     src |-> c,
                                     payload |-> msg
                                 ])
                             ELSE client_buffers[dest]
                         ]
    /\ UNCHANGED <<connections, memberships, presence_sets, control_tokens>>

ReadMsg(c) ==
    /\ c \in connections
    /\ client_buffers[c] /= <<>>
    /\ client_buffers' = [client_buffers EXCEPT ![c] = Tail(client_buffers[c])]
    /\ UNCHANGED <<connections, memberships, presence_sets, control_tokens>>

---------------------------------------------------------------------
\* Next-state relation

Next ==
    \E c \in Clients :
        \/ Connect(c)
        \/ Disconnect(c)
        \/ ReadMsg(c)
        \/ RefillTokens(c)
        \/ \E r \in Rooms :
            \/ JoinRoom(c, r)
            \/ LeaveRoom(c, r)
            \/ Broadcast(c, r, "ping")
            \/ ExpireAbandonedRoom(r)

\* Complete specification
Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------
\* Correctness Properties

\* Safety Property 1: Clients cannot belong to rooms if they are not connected
NoUnconnectedMembers ==
    \A r \in Rooms : (memberships[r] \subseteq connections) /\ (presence_sets[r] \subseteq connections)

\* Safety Property 2: Disconnected clients must have empty buffers
NotConnectedBufferEmpty ==
    \A c \in Clients : (c \notin connections) => (client_buffers[c] = <<>>)

\* Safety Property 3: Every message in every buffer strictly satisfies the 12-byte wire contract
WireFormatValid ==
    \A c \in Clients :
        \A i \in 1..Len(client_buffers[c]) :
            LET pkt == client_buffers[c][i] IN
                /\ pkt.version = 1
                /\ pkt.flags = 0
                /\ pkt.room_len <= 65535
                /\ pkt.event_len <= 65535
                /\ pkt.dst_len <= 255
                /\ pkt.src_len <= 255

\* State Constraint: Keeps the state space finite for TLC
BufferLimit ==
    \A c \in Clients : Len(client_buffers[c]) <= 2

=====================================================================
