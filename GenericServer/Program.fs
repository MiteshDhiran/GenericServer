open System
open System.Net
open System.Runtime.Serialization.Formatters.Binary

type connectionMessage<'a, 'b> =
    | Incoming of 'a
    | Outgoing of 'b
    | ConnectionLost
    
type connectionId = uint64

type 'a connectionEdit =
    | Add of ('a -> unit)
    | Remove
    
type destination =
    | To of connectionId
    | ToAll

type instruction<'a, 'b> =
    | ExecuteAndRelay of 'a * destination
    | EditConnection of connectionId * 'b connectionEdit

type result<'a> =
    | Failure
    | Success of 'a

let serialize (stream: System.IO.Stream) value =
    async {
            let formatter = System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()
            let bytes =
                        use memoryStream = new System.IO.MemoryStream()
                        formatter.Serialize(memoryStream, value)
                        memoryStream.ToArray()
            do! System.BitConverter.GetBytes bytes.Length |> stream.AsyncWrite
            do! stream.AsyncWrite bytes
            stream.Flush()
    }    
    
let deserialize (stream: System.IO.Stream) =
    async {
            let formatter = System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()
            let! header = stream.AsyncRead 4
            let length = System.BitConverter.ToInt32(header, 0)
            let! bytes = stream.AsyncRead length
            use memoryStream = new System.IO.MemoryStream(bytes)
            return formatter.Deserialize memoryStream |> unbox
    }

let streamConnection (stream: System.IO.Stream) =
    let received = Event<_>()
    async {
    while true do
        let! d = deserialize stream 
        received.Trigger d
    } |> Async.Start
    let send msg =
        try
            serialize stream msg |> Async.RunSynchronously
            Success()
        with _ ->
            Failure
    send, received.Publish
    
type MessagePassing<'a,'b> =
    static member Client(ipAddress: IPAddress, port) : result<('a -> result<unit>) * IEvent<'b>> =
        try
            let client = new Sockets.TcpClient(NoDelay=true)
            client.Connect(ipAddress, port)
            let stream = client.GetStream()
            let send, receive = streamConnection stream
            Success(send, receive)
        with _ -> Failure
    
    static member Server port : result<('b -> unit) * IEvent<'a>> =
        try
            let server = Sockets.TcpListener(IPAddress.Any, port)
            server.Start()
            let relay = Event<_>()
            let received = Event<_>()
            async {
                while true do
                    let client = server.AcceptTcpClient()
                    let stream = client.GetStream()
                    let sendToClient, receivedFromClient = streamConnection stream
                    receivedFromClient.Add received.Trigger
                    let rec handler = Handler<_>(fun _ msg ->
                        match sendToClient msg with
                        | Success() -> ()
                        | Failure -> relay.Publish.RemoveHandler handler)
                    relay.Publish.AddHandler handler
            } |> Async.Start
            Success(relay.Trigger, received.Publish)
            with _ -> Failure
    
    
let deserializer client stream post =
    MailboxProcessor.Start(fun inbox -> async{
        use client = client
        use stream = stream
        try
            while true do
                let! message = deserialize stream
                post(Incoming message)
        finally
            post ConnectionLost
    })
 
   
let server initialCoreState execute =
    MailboxProcessor<_>.Start(fun inbox ->
        let rec loop (connections: Map<_, _>) coreState = async {
            let! message = inbox.Receive()
            match message with
                    | EditConnection(id, edit) ->
                            let connections =
                                match edit with
                                    | Add post -> Map.add id post connections
                                    | Remove -> Map.remove id connections
                            return! loop connections coreState
                    | ExecuteAndRelay(message, destination) ->
                        let! coreState, message = execute(coreState, message)
                        match destination with
                                | To connectionId -> connections.[connectionId] message
                                | ToAll -> Map.iter (fun _ post -> post message) connections
                        return! loop connections coreState
            }
        initialCoreState() |> loop Map.empty)
    
let connectionAgent initialConnectionState validate filter post connectionId stream =
    MailboxProcessor.Start(fun inbox ->
        let send message = inbox.Post(Outgoing message)
        post(EditConnection(connectionId, Add send))
        let rec loop connectionState = async {
            let! message = inbox.Receive()
            let! state = async {
                match message with
                | Incoming message ->
                    let! connectionState, message, destination =
                        validate(connectionState, message)
                    post(ExecuteAndRelay(message, destination))
                    return Some connectionState
                | Outgoing message ->
                    let! connectionState, message = filter(connectionState, message)
                    match message with
                    | Some message -> do! serialize stream message
                    | None -> ()
                    return Some connectionState
                | ConnectionLost ->
                    post(EditConnection(connectionId, Remove))
                    return None
            }
            match state with
            | Some connectionState -> return! loop connectionState
            | None -> ()
        }
        initialConnectionState connectionId |> loop);    

open System.Net

type Sockets.TcpListener with
    member server.AsyncAcceptTcpClient() =
        Async.FromBeginEnd(server.BeginAcceptTcpClient, server.EndAcceptTcpClient)

let startServer (initialCoreState, execute) (initialConnectionState, validate, filter) address port =
    let server = server initialCoreState execute
    
    let rec handleConnection connectionId (client: Sockets.TcpClient) = async {
            //print connectionId
            // System.Console.WriteLine($"Connection {connectionId} established")
            let stream = client.GetStream()
            let connection = connectionAgent initialConnectionState validate filter server.Post connectionId stream
            deserializer client stream connection.Post |> ignore
    }
    async {
        let server = Sockets.TcpListener(address, port)
        server.Start()
        let rec loop connectionId = async {
            let! client = server.AsyncAcceptTcpClient()
            handleConnection connectionId client
            |> Async.Start
            return! loop (connectionId + 1UL)
    }
    do! loop 0UL
    } |> Async.Start;;
    
type initialServerState =
    {
    connectionString: string
    requestCounts: uint64
    }



type 'a imsg =
    | Hello of 'a

type 'a vimsg =
    | ValidatedHello of 'a

type 'a omsg =
    | World of 'a
    
type ExampleMessagePassing = MessagePassing<int imsg, int omsg> 
    
let test_initialCoreState() =
    {connectionString = "localhost"; requestCounts = 0UL}
let test_execute = fun (coreState:initialServerState, message:int imsg) -> async {
    let newCoreState = {coreState with requestCounts = coreState.requestCounts + 1UL}
    // Console.WriteLine("...Executing connection string:{0} current request count: {1} on message:{2}", coreState.connectionString, coreState.requestCounts ,message)
    match message with
            | Hello n -> return newCoreState, World (n+1)
    
}

let test_execute_validated_message = fun (coreState:initialServerState, message:int vimsg) -> async {
    let newCoreState = {coreState with requestCounts = coreState.requestCounts + 1UL}
    Console.WriteLine("...Executing connection string:{0} current request count: {1} on message:{2}", coreState.connectionString, coreState.requestCounts ,message)
    match message with
            | ValidatedHello n -> return newCoreState, World (n+1)
    
}

type sessionState =
    {
     SessionID: connectionId
     NumberOfRequestsInSession: uint64
    }
let test_initialConnectionState = fun connectionId ->  { SessionID = connectionId; NumberOfRequestsInSession = 0UL }
let test_validate = fun (state:sessionState, message: int imsg) -> async {
                                                                            let validatedMessage = match message with
                                                                                                        | Hello i -> ValidatedHello i 
                                                                            return state, validatedMessage, To state.SessionID
                                                                          }
//let test_validate = fun (state:sessionState, message: int imsg) -> async { return state, message, To state.SessionID }
let test_filter = fun (state:sessionState, message: int omsg) -> async { return state, Some message }

//((unit -> 'a) * ('a * 'b -> Async<'a * 'c>)) -> ((connectionId -> 'd) * ('d * 'e -> Async<'d * 'b * destination>) * ('d * 'c -> Async<'d * 'f option>)) -> IPAddress -> int -> unit
startServer (test_initialCoreState, test_execute_validated_message) (test_initialConnectionState, test_validate, test_filter)
    IPAddress.Loopback
    8081

let print s = System.Console.WriteLine((s()).ToString())

for i in 1..10 do
    match ExampleMessagePassing.Client (IPAddress.Loopback, 8081) with
            | Failure -> failwith "Client failed to connect to server"
            | Success(sendToServer, receivedFromServer) ->
                let clientHandler msg =
                    print(fun () -> sprintf "Client %d received: %A on %s" i msg (DateTime.Now.ToShortTimeString()))
                receivedFromServer.Add clientHandler
                // Threading.Thread.Sleep(100)
                match sendToServer (Hello i) with
                | Failure -> failwith "Client failed to send message to server"
                | Success() -> ()
                match sendToServer (Hello (i*1000)) with
                | Failure -> failwith "Client failed to send message to server"
                | Success() -> ()
    
stdin.ReadLine() |> ignore

// For more information see https://aka.ms/fsharp-console-apps
printfn "Done!"