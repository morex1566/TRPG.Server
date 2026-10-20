using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using TRPG.Common;

namespace TRPG.Networking;

/// <summary>
/// TCP 서버 accept와 connection 목록 관리
/// </summary>
public sealed class TcpServer
{
    private const int MaxConnections = 10;
    /// <summary>
    /// tcp 서버 실행 상태
    /// </summary>
    [Flags]
    public enum State
    {
        None = 0,
        AcceptEnabled = 1 << 0,
        WriteEnabled = 1 << 1,
    }

    /// <summary>
    /// 서버 실행 상태
    /// </summary>
    private AtomicFlag<State> currentState;

    /// <summary>
    /// tcp accept / close / write 직렬화용
    /// </summary>
    private readonly Strand strand;

    /// <summary>
    /// Rate Limit용
    /// </summary>
    private readonly TokenBucket acceptTokenBucket;

    /// <summary>
    /// accept용
    /// </summary>
    private TcpListener listener;

    /// <summary>
    /// 연결된 클라이언트 목록
    /// </summary>
    private readonly ConcurrentDictionary<ulong, Connection> connections = new();

    /// <summary>
    /// 검증 완료된 수신 패킷 처리 큐
    /// </summary>
    private readonly ConcurrentQueue<PacketContext> recvQueue = new();

    private readonly ConcurrentQueue<Connection> closedQueue = new();

    /// <summary>
    /// write tick-rate용 타이머 취소
    /// </summary>
    private CancellationTokenSource? writeCancellation;

    /// <summary>
    /// accept 루프 취소
    /// </summary>
    private CancellationTokenSource? acceptCancellation;


    public ConcurrentQueue<PacketContext> RecvQueue => recvQueue;

    public ConcurrentQueue<Connection> ClosedQueue => closedQueue;


    /// <summary>
    /// tcp 인스턴스 생성
    /// </summary>
    public TcpServer(int port)
    {
        strand = new Strand();
        listener = new TcpListener(IPAddress.Any, port);
        acceptTokenBucket = new TokenBucket(SystemConfig.TcpAcceptTokenBucket.Capacity,SystemConfig.TcpAcceptTokenBucket.RefillIntervalMs);
        currentState = new AtomicFlag<State>(State.None);
    }

    /// <summary>
    /// 클라이언트 접속 받기 시작
    /// </summary>
    public Task<NetResult> StartAcceptAsync()
    {
        return strand.Post(() =>
        {
            // 이미 AcceptEnabled 상태?
            if (!currentState.TrySet(State.AcceptEnabled))
            {
                return NetResult.Fail(new NetError(NetErrorType.Already));
            }

            // 클라 받기 루프 시작
            try
            {
                listener.Start();
                acceptCancellation = new CancellationTokenSource();
                Task.Run(() => AcceptLoopAsync(acceptCancellation.Token));

                return NetResult.Success();
            }
            catch (Exception ex)
            {
                currentState.TryUnset(State.AcceptEnabled);
                return NetResult.Fail(new NetError(NetErrorType.Unknown, ex.Message));
            }
        });
    }

    /// <summary>
    /// accept 루프
    /// </summary>
    private async Task AcceptLoopAsync(CancellationToken cancellation)
    {
        while (!cancellation.IsCancellationRequested)
        {
            NetResult<System.Net.Sockets.TcpClient> acceptResult = await TryAcceptAsync(cancellation);

            // TryAcceptAsync 실패 (심각)
            if (acceptResult.IsFailed && (acceptResult.Error.Type is NetErrorType.Canceled or NetErrorType.Disposed))
            {
                return;
            }

            // TryAcceptAsync 실패 (주의)
            if (acceptResult.IsFailed)
            {
                continue;
            }

            // AcceptEnabled 상태 아님?
            // 지금 들어온 클라 버림
            // AcceptEnabled 종료
            if (!currentState.Has(State.AcceptEnabled))
            {
                acceptResult.Value.Close();
                return;
            }

            // Rate Limit 걸림?
            // 지금 들어온 클라 버림
            if (!acceptTokenBucket.Consume())
            {
                acceptResult.Value.Close();
                continue;
            }

            // 연결을 받을 수 있을 만큼 메모리 충분?
            // 지금 들어온 클라 버림
            if (SystemConfig.CurrentRamPercentage() > SystemConfig.LimitRamPercentage)
            {
                acceptResult.Value.Close();
                continue;
            }

            // 한 채팅방에 최대 10명만 입장시킨다.
            if (connections.Count >= MaxConnections)
            {
                acceptResult.Value.Close();
                continue;
            }

            // 커넥션 등록
            ulong newConnectionId = ConnectionIdGenerator.Generate();
            ulong newConnectionGuid = GuidGenerator.Generate();
            Console.WriteLine($"accept: connectionId={newConnectionId}, guid={newConnectionGuid}");
            var newConnection = new Connection(acceptResult.Value, newConnectionGuid, recvQueue, () =>
            {
                // 종료 콜백이 반복되어도 퇴장 처리는 한 번만 큐에 넣는다.
                if (connections.TryRemove(newConnectionId, out Connection? closed) == true)
                {
                    closedQueue.Enqueue(closed);
                    Console.WriteLine($"close: connectionId={newConnectionId}");
                }
            });
            connections[newConnectionId] = newConnection;
            connections[newConnectionId].StartReadAsync();
        }
    }

    /// <summary>
    /// 접속 중인 발신자의 패킷을 채팅방의 다른 접속자에게 전달한다.
    /// </summary>
    public void BroadcastExcept(Connection sender, byte[] buffer)
    {
        // 연결 종료 후 큐에 남은 패킷은 중계하지 않는다.
        if (connections.Values.Any(connection => ReferenceEquals(connection, sender)) == false) return;

        foreach (Connection connection in connections.Values)
        {
            if (ReferenceEquals(connection, sender) == false)
            {
                connection.Send(buffer);
            }
        }
    }

    /// <summary>
    /// 퇴장한 연결을 제외하고 현재 남아 있는 접속자에게 공지를 전달한다.
    /// </summary>
    public void Broadcast(byte[] buffer)
    {
        foreach (Connection connection in connections.Values)
        {
            connection.Send(buffer);
        }
    }

    private async Task<NetResult<System.Net.Sockets.TcpClient>> TryAcceptAsync(CancellationToken cancellation)
    {
        try
        {
            System.Net.Sockets.TcpClient client = await listener.AcceptTcpClientAsync(cancellation);
            return NetResult<System.Net.Sockets.TcpClient>.Success(client);
        }
        catch (OperationCanceledException)
        {
            return NetResult<System.Net.Sockets.TcpClient>.Fail(new NetError(NetErrorType.Canceled));
        }
        catch (ObjectDisposedException)
        {
            return NetResult<System.Net.Sockets.TcpClient>.Fail(new NetError(NetErrorType.Disposed));
        }
        catch (SocketException)
        {
            return NetResult<System.Net.Sockets.TcpClient>.Fail(new NetError(NetErrorType.Socket));
        }
        catch (Exception ex)
        {
            return NetResult<System.Net.Sockets.TcpClient>.Fail(new NetError(NetErrorType.Unknown, ex.Message));
        }
    }

    /// <summary>
    /// write tick-rate마다 모든 Connection에 StartWriteAsync 명령
    /// </summary>
    public Task<NetResult> StartWriteAsync()
    {
        return strand.Post(() =>
        {
            // 이미 WriteEnabled 상태?
            if (!currentState.TrySet(State.WriteEnabled))
            {
                return NetResult.Fail(new NetError(NetErrorType.Already));
            }

            // 쓰기 루프 시작
            try
            {
                writeCancellation = new CancellationTokenSource();
                Task.Run(() => WriteLoopAsync(writeCancellation.Token));

                return NetResult.Success();
            }
            catch (Exception ex)
            {
                currentState.TryUnset(State.WriteEnabled);
                return NetResult.Fail(new NetError(NetErrorType.Unknown, ex.Message));
            }
        });
    }

    /// <summary>
    /// Tick-rate마다 모든 connection에 async_write 명령
    /// </summary>
    private async Task WriteLoopAsync(CancellationToken cancellation)
    {
        var interval = TimeSpan.FromMilliseconds(SystemConfig.Tcp.TickIntervalMs);
        using var timer = new PeriodicTimer(interval);

        while (true)
        {
            NetResult<bool> waitResult = await TryWaitForNextWriteTickAsync(timer, cancellation);

            // TryWaitForNextWriteTickAsync 실패 (ex. 작업 취소)
            if (waitResult.IsFailed)
            {
                return;
            }

            // timer 종료. Dispose 됨
            if (!waitResult.Value)
            {
                return;
            }

            // WriteEnabled 상태 아님?
            if (!currentState.Has(State.WriteEnabled))
            {
                return;
            }

            // 모든 커넥션에 Write 명령
            foreach (var connection in connections)
            {
                connection.Value.WriteAsync();
            }
        }
    }

    private static async Task<NetResult<bool>> TryWaitForNextWriteTickAsync(PeriodicTimer timer, CancellationToken cancellation)
    {
        try
        {
            bool hasNextTick = await timer.WaitForNextTickAsync(cancellation);
            return NetResult<bool>.Success(hasNextTick);
        }
        catch (OperationCanceledException)
        {
            return NetResult<bool>.Fail(new NetError(NetErrorType.Canceled));
        }
        catch (ObjectDisposedException)
        {
            return NetResult<bool>.Fail(new NetError(NetErrorType.Disposed));
        }
        catch (Exception ex)
        {
            return NetResult<bool>.Fail(new NetError(NetErrorType.Unknown, ex.Message));
        }
    }

    /// <summary>
    /// tcp 서버 종료
    /// </summary>
    public void Close()
    {
        strand.Post(() =>
        {
            // Write() 취소
            currentState.TryUnset(State.WriteEnabled);
            writeCancellation?.Cancel();

            // Accept() 취소
            currentState.TryUnset(State.AcceptEnabled);
            acceptCancellation?.Cancel();
            try
            {
                listener.Stop();
            }
            catch 
            {
            }

            // 연결 종료
            foreach (var connection in connections.Values.ToArray())
            {
                connection.Close();
            }
            connections.Clear();

            return Task.CompletedTask;
        });
    }
}
