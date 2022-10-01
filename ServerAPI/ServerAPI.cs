namespace ServerAPI;

public class ServerState<T>
{
    public T  Data { get; init; }

    public ServerState(T data)
    {
        this.Data = data;
    }
    
}

public class InputMessage<T>
{
    public T Data { get; init; }

    public InputMessage(T data)
    {
        this.Data = data;
    }
}

public class OutputMessage<T>
{
    public T Data { get; init; }

    public OutputMessage(T data)
    {
        this.Data = data;
    }
}

public class ServerMethods<TServerState,TInputMessage,TOutputMessage,TValidatedMessage>
{
    public TServerState ServerState { get; init; }
    public Func<TValidatedMessage, TOutputMessage> ExecuteFunc { get; init; }

    public ServerMethods(TServerState serverState, Func<TValidatedMessage, TOutputMessage> executeFunc)
    {
        this.ServerState = serverState;
        this.ExecuteFunc = executeFunc;
    }
}


