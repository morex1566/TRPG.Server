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
        // 이번 서버 실행에서 발생한 채팅을 같은 세션으로 구분한다.
        string sessionId = Guid.NewGuid().ToString("N");
        TcpServer tcpServer = new TcpServer(Port);
        await using var performance = new ServerPerformance(tcpServer, sessionId);
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

            long nextIdleCheck = Environment.TickCount64;

            while (cancellation.IsCancellationRequested == false)
            {
                Time.Instance.Update();

                // 한 틱에 이미 도착한 패킷만 처리한다.
                int requestCount = tcpServer.RecvQueue.Count;
                for (int i = 0; i < requestCount; i++)
                {
                    if (tcpServer.RecvQueue.TryDequeue(out PacketContext context) == false) break;

                    PayloadType payloadType = context.GetPayloadType();
                    if (payloadType == PayloadType.ConnectRequest && context.Owner is Connection joining)
                    {
                        // 최초 접속 요청만 입장 공지로 전달한다.
                        if (joining.TryMarkJoined() == true)
                        {
                            ConnectionService.RequestConnect(ref context);
                            ChatService.NotifyJoined(joining, tcpServer);
                        }
                        continue;
                    }
                    if (payloadType == PayloadType.ChatMessageRequest)
                    {
                        try
                        {
                            await ChatService.BroadcastAsync(context, tcpServer, sessionId, cancellation);
                        }
                        catch (Oracle.ManagedDataAccess.Client.OracleException error)
                        {
                            // 접속 정보와 채팅 내용 없이 오류 번호만 기록한다.
                            Console.WriteLine($"채팅 DB 저장 실패: ORA-{error.Number:D5}. 해당 메시지는 중계하지 않았습니다.");
                        }
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

                // 대기 중인 채팅을 반영한 뒤 1초마다 비활성 사용자를 정리한다.
                if (Environment.TickCount64 >= nextIdleCheck)
                {
                    tcpServer.CloseInactiveConnections();
                    nextIdleCheck = Environment.TickCount64 + 1000;
                }

                // 입장 패킷 처리 뒤에 퇴장을 처리해 입장/퇴장 공지 순서를 유지한다.
                while (tcpServer.ClosedQueue.TryDequeue(out Connection? leaving) == true)
                {
                    if (leaving.HasJoined == true)
                    {
                        ChatService.NotifyLeft(leaving, tcpServer);
                    }
                }

                await Task.Delay(10);
            }
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested == true)
        {
            // DB 저장을 기다리는 중에도 정상 종료 요청을 처리한다.
        }
        finally
        {
            tcpServer.Close();
        }
    }
}
