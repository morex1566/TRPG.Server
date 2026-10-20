using System.Buffers.Binary;
using System.Net.Sockets;
using TRPG.Networking;
using TRPG.Protocol;

namespace TRPG.Test;

internal sealed class StressClient : IAsyncDisposable
{
    private readonly System.Net.Sockets.TcpClient socket = new() { NoDelay = true };
    private readonly CancellationTokenSource lifetime;
    private readonly TaskCompletionSource<bool> ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TestMetrics metrics;
    private readonly string prefix;
    private NetworkStream stream = null!;
    private Task receiver = Task.CompletedTask;
    private int active;
    private int plannedClose;

    public int Index { get; }
    public bool IsConnected => Volatile.Read(ref active) == 1;
    public string? Error { get; private set; }

    public StressClient(int index, string prefix, TestMetrics metrics, CancellationToken cancellation)
    {
        Index = index;
        this.prefix = prefix;
        this.metrics = metrics;
        lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
    }

    public async Task ConnectAsync(TestOptions options)
    {
        long started = System.Diagnostics.Stopwatch.GetTimestamp();
        Interlocked.Increment(ref metrics.ConnectionAttempts);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(lifetime.Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        try
        {
            await socket.ConnectAsync(options.Host, options.Port, timeout.Token);
            stream = socket.GetStream();
            receiver = ReceiveAsync();
            PacketContext request = PacketContext.Create(PayloadType.ConnectRequest, builder =>
            {
                ConnectRequest.StartConnectRequest(builder);
                return ConnectRequest.EndConnectRequest(builder).Value;
            });
            await stream.WriteAsync(request.Buffer, timeout.Token);
            Interlocked.Add(ref metrics.SentBytes, request.Buffer.Length);

            // TCP 연결뿐 아니라 서버의 ConnectResponse까지 받아야 접속 성공이다.
            if (await ready.Task.WaitAsync(timeout.Token) == false || IsConnected == false)
                throw new IOException(Error ?? "입장 응답 전에 연결이 종료됐습니다.");
            Interlocked.Increment(ref metrics.Connected);
            metrics.RecordConnectionTime(started);
        }
        catch (Exception error)
        {
            Error = error.Message;
            metrics.RecordClientError(Index, error.Message);
            Interlocked.Increment(ref metrics.ConnectFailures);
            socket.Dispose();
        }
    }

    public async Task SendAsync(byte[] buffer, CancellationToken cancellation)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation, lifetime.Token);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        try
        {
            await stream.WriteAsync(buffer, timeout.Token);
        }
        catch
        {
            // 중간에 실패한 프레임 뒤로 새 프레임을 이어 보내지 않는다.
            socket.Dispose();
            throw;
        }
    }

    private async Task ReceiveAsync()
    {
        try
        {
            byte[] header = new byte[sizeof(int)];
            while (lifetime.IsCancellationRequested == false)
            {
                await stream.ReadExactlyAsync(header, lifetime.Token);
                int length = BinaryPrimitives.ReadInt32LittleEndian(header);
                if (length <= 0 || length > 64 * 1024 - sizeof(int))
                    throw new InvalidDataException($"잘못된 패킷 크기: {length}");

                byte[] buffer = new byte[length + sizeof(int)];
                header.CopyTo(buffer, 0);
                await stream.ReadExactlyAsync(buffer.AsMemory(sizeof(int)), lifetime.Token);
                Interlocked.Add(ref metrics.ReceivedBytes, buffer.Length);
                var packet = new PacketContext(buffer);
                if (packet.GetPayloadType() == PayloadType.ConnectResponse)
                {
                    if (Interlocked.Exchange(ref active, 1) == 0) metrics.ConnectionOpened();
                    ready.TrySetResult(true);
                }
                else if (packet.GetPayloadType() == PayloadType.ChatMessageNotify)
                {
                    string? message = packet.PayloadAs<ChatMessageNotify>()?.Message;
                    if (metrics.RandomWorkload == true)
                    {
                        // random 본문의 첫 8바이트는 추가 길이 없이 메시지 번호를 담는다.
                        if (message is not null && message.Length is >= 8 and <= 16 &&
                            int.TryParse(message.AsSpan(0, 8), System.Globalization.NumberStyles.HexNumber,
                                System.Globalization.CultureInfo.InvariantCulture, out int randomId) == true)
                            metrics.ObserveRandom(randomId, Index);
                        continue;
                    }
                    if (message is null || message.StartsWith(prefix, StringComparison.Ordinal) == false) continue;
                    int end = message.IndexOf('~', prefix.Length);
                    if (end >= 0 && int.TryParse(message.AsSpan(prefix.Length, end - prefix.Length), out int id) == true)
                        metrics.Observe(id, Index);
                }
            }
        }
        catch (Exception error)
        {
            if (lifetime.IsCancellationRequested == false)
            {
                Error = error.Message;
                metrics.RecordClientError(Index, error.Message);
            }
        }
        finally
        {
            if (Interlocked.Exchange(ref active, 0) == 1)
            {
                metrics.ConnectionClosed();
                if (metrics.Stopping == false)
                {
                    if (Volatile.Read(ref plannedClose) == 1)
                        Interlocked.Increment(ref metrics.PlannedDisconnects);
                    else
                        Interlocked.Increment(ref metrics.Disconnected);
                }
            }
            ready.TrySetResult(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        Interlocked.Exchange(ref plannedClose, 1);
        lifetime.Cancel();
        socket.Dispose();
        await receiver;
        lifetime.Dispose();
    }
}
