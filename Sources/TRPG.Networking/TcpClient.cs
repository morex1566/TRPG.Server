using System;
using System.Collections.Concurrent;
using System.Net.Sockets;
using TRPG.Common;

namespace TRPG.Networking;

public sealed class TcpClient
{
    /// <summary>
    /// tcp 클라이언트 실행 상태
    /// </summary>
    [Flags]
    public enum State
    {
        None = 0,
        ConnectEnabled = 1 << 0,
        WriteEnabled = 1 << 1,
    }

    /// <summary>
    /// 클라이언트 실행 상태
    /// </summary>
    private AtomicFlag<State> currentState;

    /// <summary>
    /// tcp connect / close / write 직렬화용
    /// </summary>
    private readonly Strand strand;

    /// <summary>
    /// 연결할 서버 host
    /// </summary>
    private readonly string host;

    /// <summary>
    /// 연결할 서버 port
    /// </summary>
    private readonly int port;

    /// <summary>
    /// 서버와 연결된 connection
    /// </summary>
    private Connection? connection;

    /// <summary>
    /// 검증 완료된 수신 패킷 처리 큐
    /// </summary>
    private readonly ConcurrentQueue<PacketContext> recvQueue = new();

    /// <summary>
    /// write tick-rate용 타이머 취소
    /// </summary>
    private CancellationTokenSource? writeCancellation;

    /// <summary>
    /// connect 취소
    /// </summary>
    private CancellationTokenSource? connectCancellation;


    public ConcurrentQueue<PacketContext> RecvQueue => recvQueue;

    public bool IsConnected => connection is not null;

    public ulong Guid => connection?.Guid ?? 0;

    public string LocalIp => connection?.LocalIp ?? "";


    /// <summary>
    /// tcp 클라이언트 인스턴스 생성
    /// </summary>
    public TcpClient(string host, int port)
    {
        this.host = host;
        this.port = port;

        strand = new Strand();
        currentState = new AtomicFlag<State>(State.None);
    }


    /// <summary>
    /// 서버 연결 시작
    /// </summary>
    public async Task<NetResult> StartConnectAsync()
    {
        CancellationTokenSource? cancellationSource = null;
        NetResult startResult = await strand.Post(() =>
        {
            // 이미 ConnectEnabled 상태?
            if (!currentState.TrySet(State.ConnectEnabled))
            {
                return NetResult.Fail(new NetError(NetErrorType.Already));
            }

            // 서버 연결 시작
            connectCancellation = new CancellationTokenSource();
            cancellationSource = connectCancellation;

            return NetResult.Success();
        });

        if (startResult.IsFailed)
        {
            return startResult;
        }

        NetResult<System.Net.Sockets.TcpClient> connectResult = await TryConnectAsync(cancellationSource!.Token);

        return await strand.Post(() =>
        {
            // TryConnectAsync 실패
            if (connectResult.IsFailed)
            {
                currentState.TryUnset(State.ConnectEnabled);
                return NetResult.Fail(connectResult.Error);
            }

            // ConnectEnabled 상태 아님?
            // 지금 연결된 소켓 버림
            if (!currentState.Has(State.ConnectEnabled))
            {
                connectResult.Value.Close();
                return NetResult.Fail(new NetError(NetErrorType.Canceled));
            }

            // 기존 connection이 있으면 종료
            connection?.Close();
            connection = null;

            // 커넥션 등록
            connection = new Connection(connectResult.Value, 0, recvQueue, () =>
            {
                connection = null;
                currentState.TryUnset(State.ConnectEnabled);
            });

            connection.StartReadAsync();

            return NetResult.Success();
        });
    }

    /// <summary>
    /// connection GUID 갱신
    /// </summary>
    public void SetGuid(ulong guid)
    {
        connection?.SetGuid(guid);
    }

    /// <summary>
    /// 송신 패킷 enqueue 요청
    /// </summary>
    public NetResult Send(byte[] buffer)
    {
        if (connection is null)
        {
            return NetResult.Fail(new NetError(NetErrorType.Socket, "not connected."));
        }

        connection.Send(buffer);
        return NetResult.Success();
    }

    private async Task<NetResult<System.Net.Sockets.TcpClient>> TryConnectAsync(CancellationToken cancellation)
    {
        System.Net.Sockets.TcpClient client = new();

        try
        {
            await client.ConnectAsync(host, port, cancellation);
            return NetResult<System.Net.Sockets.TcpClient>.Success(client);
        }
        catch (OperationCanceledException)
        {
            client.Close();
            return NetResult<System.Net.Sockets.TcpClient>.Fail(new NetError(NetErrorType.Canceled));
        }
        catch (ObjectDisposedException)
        {
            client.Close();
            return NetResult<System.Net.Sockets.TcpClient>.Fail(new NetError(NetErrorType.Disposed));
        }
        catch (SocketException)
        {
            client.Close();
            return NetResult<System.Net.Sockets.TcpClient>.Fail(new NetError(NetErrorType.Socket));
        }
        catch (Exception ex)
        {
            client.Close();
            return NetResult<System.Net.Sockets.TcpClient>.Fail(new NetError(NetErrorType.Unknown, ex.Message));
        }
    }

    /// <summary>
    /// write tick-rate마다 Connection에 AsyncWrite 명령
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
            writeCancellation = new CancellationTokenSource();
            Task.Run(() => WriteLoopAsync(writeCancellation.Token));

            return NetResult.Success();
        });
    }

    /// <summary>
    /// Tick-rate마다 connection에 async_write 명령
    /// </summary>
    private async Task WriteLoopAsync(CancellationToken cancellation)
    {
        var interval = TimeSpan.FromMilliseconds(SystemConfig.Tcp.TickIntervalMs);
        using var timer = new PeriodicTimer(interval);

        while (true)
        {
            NetResult<bool> waitResult = await TryWaitForNextWriteTickAsync(timer, cancellation);

            // TryWaitForNextWriteTickAsync 실패
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

            // 연결된 connection 없음?
            if (connection == null)
            {
                continue;
            }

            connection.WriteAsync();
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
    /// tcp 클라이언트 종료
    /// </summary>
    public void Close()
    {
        strand.Post(() =>
        {
            // Write() 취소
            currentState.TryUnset(State.WriteEnabled);
            writeCancellation?.Cancel();

            // Connect() 취소
            currentState.TryUnset(State.ConnectEnabled);
            connectCancellation?.Cancel();

            // 연결 종료
            connection?.Close();
            connection = null;

            return Task.CompletedTask;
        });
    }
}
