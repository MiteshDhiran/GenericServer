open System

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
    
let serialize (stream: System.IO.Stream) value =
    async {
            let formatter = System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()
            let bytes =
                        use memoryStream = new System.IO.MemoryStream()
                        formatter.Serialize(memoryStream, value)
                        memoryStream.ToArray()
            do! System.BitConverter.GetBytes bytes.Length |> stream.AsyncWrite
            do! stream.AsyncWrite bytes
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
    
// (unit -> 'a) -> ('a * 'b -> Async<'a * 'c>) -> MailboxProcessor<instruction<'b,'c>>    
let test_initialCoreState() =  0 
let test_execute = fun (s, message) -> async {
    Console.WriteLine("...Executing {0} on {1}", message, s)
    return s + 1, s.ToString()
}
                                        
let test_initialConnectionState = fun connectionId ->  0
let test_validate = fun (state, message) -> async { return state, message, ToAll }
let test_filter = fun (state, message) -> async { return state, Some message }

//((unit -> 'a) * ('a * 'b -> Async<'a * 'c>)) -> ((connectionId -> 'd) * ('d * 'e -> Async<'d * 'b * destination>) * ('d * 'c -> Async<'d * 'f option>)) -> IPAddress -> int -> unit

startServer (test_initialCoreState, test_execute) (test_initialConnectionState, test_validate, test_filter)
    IPAddress.Loopback
    8081
   

let client = new Sockets.TcpClient()
client.Connect(IPAddress.Loopback, 8081)
let stream = client.GetStream()
let formatter = System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()
let bytes =
    use memoryStream = new System.IO.MemoryStream()
    formatter.Serialize(memoryStream, 200)
    memoryStream.ToArray()
System.BitConverter.GetBytes bytes.Length |> stream.Write
stream.Write(bytes, 0, bytes.Length)
stream.Flush()

// let xx:int = deserialize stream |> Async.RunSynchronously
// Console.WriteLine(xx.ToString())


for i in 1..10 do
    printfn "Sending %d" i
    let client = new Sockets.TcpClient()
    client.Connect(IPAddress.Loopback, 8081)
    let stream = client.GetStream()
    
    let formatter = System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()
    let bytes =
        use memoryStream = new System.IO.MemoryStream()
        formatter.Serialize(memoryStream, i)
        memoryStream.ToArray()
    let bytes_2 =
        use memoryStream = new System.IO.MemoryStream()
        formatter.Serialize(memoryStream, 150)
        memoryStream.ToArray()    
    System.BitConverter.GetBytes bytes.Length |> stream.Write
    stream.Write(bytes, 0, bytes.Length)
    stream.Write(bytes, 0, bytes_2.Length)
    stream.Flush()
    
    client.Close()


// let data:string = deserialize stream |> Async.RunSynchronously
// Console.WriteLine(data)

// let! header = stream.AsyncRead 4
// let length = System.BitConverter.ToInt32(header, 0)
// let! bytes = stream.AsyncRead length
// use memoryStream = new System.IO.MemoryStream(bytes)
// formatter.Deserialize memoryStream |> unbox



// startServer(test_initialCoreState, test_execute) (test_initialConnectionState, test_validate, test_filter) IPAddress.Loopback 8080



// For more information see https://aka.ms/fsharp-console-apps
printfn "Hello from F#"