using TRPG.Common;
using TRPG.Networking;
using TRPG.Protocol;

namespace TRPG.Core;

/// <summary>
/// 단일 채팅방의 접속과 패킷 처리를 실행한다.
/// </summary>
public static class ChatServer
{
    private const int Port = 60000;

    public static async Task RunAsync(CancellationToken cancellation)
    {
        TcpServer tcpServer = new TcpServer(Port);
        try
        {
            NetResult startAcceptResult = await tcpServer.StartAcceptAsync();
            if (startAcceptResult.IsFailed)
            {
                Console.WriteLine(startAcceptResult.Error.ToString());
                return;
            }

            NetResult startWriteResult = await tcpServer.StartWriteAsync();
            if (startWriteResult.IsFailed)
            {
                Console.WriteLine(startWriteResult.Error.ToString());
                return;
            }

            Console.WriteLine($"server listening. port={Port}");

            while (cancellation.IsCancellationRequested == false)
            {
                Time.Instance.Update();

                // 한 틱에 이미 도착한 패킷만 처리한다.
                int requestCount = tcpServer.RecvQueue.Count;
                for (int i = 0; i < requestCount; i++)
                {
                    if (tcpServer.RecvQueue.TryDequeue(out PacketContext context) == false) break;

                    PayloadType payloadType = context.GetPayloadType();
                    if (payloadType == PayloadType.ChatMessageRequest)
                    {
                        ChatService.Broadcast(ref context, tcpServer);
                        continue;
                    }

                    PacketHandler? handle = PacketHandleHelper.Get(payloadType);
                    if (handle is null)
                    {
                        Console.WriteLine($"unknown packet: {payloadType}");
                        continue;
                    }

                    handle.Invoke(ref context);
                }

                await Task.Delay(10);
            }
        }
        finally
        {
            tcpServer.Close();
        }
    }
}
