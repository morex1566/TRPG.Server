using TRPG.Core;
using TRPG.Networking;
using TRPG.Protocol;

namespace TRPG;

static partial class Program
{
    private static TerminalApp app = new TerminalApp();

    private static TcpClient? tcpClient;


    public static void Main()
    {
        app.ConnectRequested += OnConnectAsync;
        app.ChatRequested += OnChat;
        try
        {
            app.Run(Tick);
        }
        finally
        {
            tcpClient?.Close();
        }
    }

    private static bool Tick()
    {
        if (tcpClient is null) return true;

        // TCP 수신 패킷 처리
        int requestCount = tcpClient.RecvQueue.Count;
        for (int i = 0, count = requestCount; i < count; i++)
        {
            // 수신 패킷 없음?
            if (!tcpClient.RecvQueue.TryDequeue(out var context))
            {
                break;
            }

            HandlePacket(ref context);
        }

        return true;
    }

    private static bool SendPacket(PacketContext packet)
    {
        if (tcpClient?.IsConnected != true)
        {
            app.AppendLog("not connected.");
            return false;
        }

        NetResult sendResult = tcpClient.Send(packet.Buffer);
        if (sendResult.IsFailed)
        {
            app.AppendLog(sendResult.Error.ToString());
            return false;
        }

        return true;
    }

    private static void HandlePacket(ref PacketContext context)
    {
        var payloadType = context.GetPayloadType();

        switch (payloadType)
        {
            case PayloadType.ConnectResponse:
                ReceiveConnectResponse(ref context);
                break;

            case PayloadType.ChatMessageResponse:
                ReceiveChatMessageResponse(ref context);
                break;

            case PayloadType.ChatMessageNotify:
                ReceiveChatMessageNotify(ref context);
                break;

            default:
                app.AppendLog($"unknown packet: {payloadType}");
                break;
        }
    }

    private static void ReceiveConnectResponse(ref PacketContext context)
    {
        ConnectResponse response = context.PayloadAs<ConnectResponse>()!.Value;

        tcpClient?.SetGuid(response.Guid);
        if (string.IsNullOrWhiteSpace(response.Ip) == false)
        {
            app.SetOwnIp(response.Ip);
        }
    }

    private static void ReceiveChatMessageResponse(ref PacketContext context)
    {
        ChatMessageResponse response = context.PayloadAs<ChatMessageResponse>()!.Value;

        app.AppendChat("server", response.Message);
    }

    private static void ReceiveChatMessageNotify(ref PacketContext context)
    {
        ChatMessageNotify notification = context.PayloadAs<ChatMessageNotify>()!.Value;

        // 서버가 확인한 발신자 IP를 채팅 표시명으로 사용한다.
        string sender = string.IsNullOrWhiteSpace(notification.FromIp) == true
            ? notification.From.ToString()
            : $"ㅇㅇ({notification.FromIp})";
        app.AppendChat(sender, notification.Message);
    }
}

static partial class Program
{
    private static async Task OnConnectAsync(string host, int port)
    {
        if (tcpClient?.IsConnected == true)
        {
            app.AppendLog("already connected.");
            return;
        }

        TcpClient client = new TcpClient(host, port);
        tcpClient = client;

        NetResult startConnectResult = await client.StartConnectAsync();
        if (!ReferenceEquals(tcpClient, client))
        {
            client.Close();
            return;
        }

        if (startConnectResult.IsFailed)
        {
            app.AppendLog(startConnectResult.Error.ToString());
            tcpClient = null;
            return;
        }

        // 서버의 접속 IP 응답을 받기 전에는 소켓의 로컬 IP를 표시한다.
        if (string.IsNullOrWhiteSpace(client.LocalIp) == false)
        {
            app.SetOwnIp(client.LocalIp);
        }

        NetResult startWriteResult = await client.StartWriteAsync();
        if (!ReferenceEquals(tcpClient, client))
        {
            client.Close();
            return;
        }

        if (startWriteResult.IsFailed)
        {
            app.AppendLog(startWriteResult.Error.ToString());
            client.Close();
            tcpClient = null;
            return;
        }

        PacketContext packet = PacketContext.Create(PayloadType.ConnectRequest, builder =>
        {
            ConnectRequest.StartConnectRequest(builder);
            return ConnectRequest.EndConnectRequest(builder).Value;
        });

        if (SendPacket(packet))
        {
            app.AppendLog("connected.");
        }
    }

    private static void OnChat(string message)
    {
        if (string.IsNullOrWhiteSpace(message))
        {
            return;
        }

        // 로컬 채팅은 서버 전송 성공 여부와 관계없이 즉시 표시한다.
        app.AppendOwnChat(message);
        app.ClearMessage();

        if (tcpClient?.IsConnected != true)
        {
            app.AppendChat("공지", "서버에 연결되어 있지 않습니다.");
            return;
        }

        PacketContext packet = PacketContext.Create(PayloadType.ChatMessageRequest, builder =>
        {
            var messageOffset = builder.CreateString(message);
            return ChatMessageRequest.CreateChatMessageRequest(
                builder,
                ChatType.ToServer,
                0,
                messageOffset).Value;
        });

        if (SendPacket(packet) == false)
        {
            app.AppendChat("공지", "서버에 연결되어 있지 않습니다.");
        }
    }
}
