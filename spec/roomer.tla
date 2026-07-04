--------------------------- MODULE roomer ---------------------------
EXTENDS Integers, Sequences, TLC

CONSTANTS 
    Clients,   \* Set of active client IDs (e.g. {c1, c2})
    Rooms      \* Set of chat room IDs (e.g. {r1, r2})

VARIABLES
    connections,     \* Set of connected client IDs
    memberships,     \* Mapping of Room -> Set of Clients in it
    client_buffers   \* Mapping of Client -> Sequence of messages

vars == <<connections, memberships, client_buffers>>

---------------------------------------------------------------------

\* Type invariant: ensures variables stay within expected types
TypeOK ==
    /\ connections \subseteq Clients
    /\ memberships \in [Rooms -> SUBSET Clients]
    /\ client_buffers \in [Clients -> Seq([from: Clients, room: Rooms, payload: STRING])]

\* Initial state of the system
Init ==
    /\ connections = {}
    /\ memberships = [r \in Rooms |-> {}]
    /\ client_buffers = [c \in Clients |-> <<>>]

---------------------------------------------------------------------
\* Actions

Connect(c) ==
    /\ c \notin connections
    /\ connections' = connections \union {c}
    /\ UNCHANGED <<memberships, client_buffers>>

Disconnect(c) ==
    /\ c \in connections
    /\ connections' = connections \ {c}
    /\ memberships' = [r \in Rooms |-> memberships[r] \ {c}]
    /\ client_buffers' = [client_buffers EXCEPT ![c] = <<>>]

JoinRoom(c, r) ==
    /\ c \in connections
    /\ c \notin memberships[r]
    /\ memberships' = [memberships EXCEPT ![r] = memberships[r] \union {c}]
    /\ UNCHANGED <<connections, client_buffers>>

LeaveRoom(c, r) ==
    /\ c \in connections
    /\ c \in memberships[r]
    /\ memberships' = [memberships EXCEPT ![r] = memberships[r] \ {c}]
    /\ UNCHANGED <<connections, client_buffers>>

Broadcast(c, r, msg) ==
    /\ c \in connections
    /\ c \in memberships[r]
    /\ client_buffers' = [dest \in Clients |->
                             IF dest \in memberships[r] /\ dest /= c
                             THEN Append(client_buffers[dest], [from |-> c, room |-> r, payload |-> msg])
                             ELSE client_buffers[dest]
                         ]
    /\ UNCHANGED <<connections, memberships>>

ReadMsg(c) ==
    /\ c \in connections
    /\ client_buffers[c] /= <<>>
    /\ client_buffers' = [client_buffers EXCEPT ![c] = Tail(client_buffers[c])]
    /\ UNCHANGED <<connections, memberships>>

---------------------------------------------------------------------
\* Next-state relation: defines non-deterministic system progress

Next ==
    \E c \in Clients :
        \/ Connect(c)
        \/ Disconnect(c)
        \/ ReadMsg(c)
        \/ \E r \in Rooms :
            \/ JoinRoom(c, r)
            \/ LeaveRoom(c, r)
            \/ Broadcast(c, r, "ping")

\* Complete specification
Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------
\* Correctness Properties

\* Safety Property 1: Clients cannot belong to rooms if they are not connected
NoUnconnectedMembers ==
    \A r \in Rooms : memberships[r] \subseteq connections

\* Safety Property 2: Disconnected clients must have empty buffers
NotConnectedBufferEmpty ==
    \A c \in Clients : (c \notin connections) => (client_buffers[c] = <<>>)

\* State Constraint: Keeps the state space finite for TLC
BufferLimit ==
    \A c \in Clients : Len(client_buffers[c]) <= 2

=====================================================================
