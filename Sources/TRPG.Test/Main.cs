using System.Diagnostics;
using System.Text;
using TRPG.Networking;
using TRPG.Protocol;

namespace TRPG.Test;

internal static class Program
{
    private static TestMetrics? currentMetrics;
    internal static TestMetrics? CurrentMetrics => Volatile.Read(ref currentMetrics);
    internal static void SetMetrics(TestMetrics? metrics) => Volatile.Write(ref currentMetrics, metrics);

    public static async Task<int> Main(string[] args)
    {
        Console.OutputEncoding = Encoding.UTF8;
        if (args.Contains("--help") == true)
        {
            TestOptions.PrintHelp();
            return 0;
        }

        TestOptions options;
        try
        {
            options = TestOptions.Parse(args);
        }
        catch (Exception error) when (error is ArgumentException or FormatException or OverflowException)
        {
            Console.Error.WriteLine(error.Message);
            TestOptions.PrintHelp();
            return 2;
        }

        using var cancellation = new CancellationTokenSource();
        ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cancellation.Cancel();
        };
        Console.CancelKeyPress += cancelHandler;
        try
        {
            return await DashboardHost.RunAsync(options, cancellation.Token);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested == true)
        {
            return 0;
        }
        finally
        {
            Console.CancelKeyPress -= cancelHandler;
        }
    }

    internal static async Task<int> RunAsync(TestOptions options, CancellationToken cancellation)
    {
        if (options.RandomWorkload == true) return await RandomScenario.RunAsync(options, cancellation);

        string prefix = $"~trpg:{Guid.NewGuid():N}:";
        var metrics = new TestMetrics();
        SetMetrics(metrics);
        var clients = new List<StressClient>();
        var connects = new List<Task>();
        var sendWatch = new Stopwatch();
        bool cancelled = false;
        Console.WriteLine($"대상: {options.Host}:{options.Port}, 접속 시도: {options.Clients}, 시도 간격: {options.RampMs}ms");
        Console.WriteLine($"유지 시간: {options.Seconds}초, 방 전체 송신 목표: {options.Rate}개/초, 한글 본문: {options.Characters}자 + 식별자");
        if (options.IsLoopback == false)
            Console.WriteLine("외부 서버 대상입니다. 중계 트래픽과 실제 서비스 접속자에게 영향을 줄 수 있습니다.");

        try
        {
            for (int i = 0; i < options.Clients; i++)
            {
                cancellation.ThrowIfCancellationRequested();
                var client = new StressClient(i, prefix, metrics, cancellation);
                clients.Add(client);
                connects.Add(client.ConnectAsync(options));
                if (options.RampMs > 0) await Task.Delay(options.RampMs, cancellation);
            }
            await Task.WhenAll(connects);
            StressClient[] participants = clients.Where(client => client.IsConnected).ToArray();
            metrics.Recipients = Math.Max(0, participants.Length - 1);
            Console.WriteLine($"부하 시작 시 연결 유지: {participants.Length}, 실패: {metrics.ConnectFailures}");

            // 구성원은 부하 시작 시 고정한다. 이후 연결 종료로 인한 미수신도 실패로 집계한다.
            if (participants.Length == 0)
            {
                foreach (StressClient client in clients.Where(client => client.Error is not null).Take(5))
                    Console.WriteLine($"클라이언트 {client.Index} 오류: {client.Error}");
                metrics.PrintResult(0, false);
                return 1;
            }
            metrics.BeginLoad();
            sendWatch.Start();
            if (options.Rate == 0)
            {
                await Task.Delay(TimeSpan.FromSeconds(options.Seconds), cancellation);
            }
            else
            {
                string body = new('가', options.Characters);
                int id = 0;
                double nextSend = 0;
                while (sendWatch.Elapsed.TotalSeconds < options.Seconds)
                {
                    cancellation.ThrowIfCancellationRequested();
                    double elapsed = sendWatch.Elapsed.TotalSeconds;
                    double wait = Math.Min(nextSend - elapsed, options.Seconds - elapsed);
                    if (wait > 0) await Task.Delay(TimeSpan.FromSeconds(wait), cancellation);
                    if (sendWatch.Elapsed.TotalSeconds >= options.Seconds) break;
                    if (id >= TestOptions.MaxMessages) break;

                    StressClient sender = participants[id % participants.Length];
                    if (sender.IsConnected == false)
                    {
                        // 종료된 클라이언트를 조용히 제외하면 부하가 낮아지므로 실패로 기록한다.
                        Interlocked.Increment(ref metrics.SendErrors);
                    }
                    else
                    {
                        string message = $"{prefix}{id}~{body}";
                        PacketContext packet = PacketContext.Create(PayloadType.ChatMessageRequest, builder =>
                        {
                            var text = builder.CreateString(message);
                            return ChatMessageRequest.CreateChatMessageRequest(builder, ChatType.ToServer, 0, text).Value;
                        });
                        metrics.Register(id, sender.Index, options.Clients);
                        try
                        {
                            await sender.SendAsync(packet.Buffer, cancellation);
                            metrics.ConfirmSent(id, packet.Buffer.Length);
                        }
                        catch (Exception error) when (error is IOException or System.Net.Sockets.SocketException or OperationCanceledException or ObjectDisposedException)
                        {
                            if (cancellation.IsCancellationRequested == true) throw new OperationCanceledException(cancellation);
                            Interlocked.Increment(ref metrics.SendErrors);
                        }
                    }
                    id++;
                    // 처리 지연 뒤 밀린 메시지를 한꺼번에 보내는 폭주를 방지한다.
                    nextSend = Math.Max(nextSend + 1 / options.Rate, sendWatch.Elapsed.TotalSeconds);
                }
            }
            sendWatch.Stop();
            metrics.EndLoad();
            Console.WriteLine($"송신 종료. 남은 중계 수신을 {options.DrainSeconds}초 기다립니다.");
            await Task.Delay(TimeSpan.FromSeconds(options.DrainSeconds), cancellation);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
        {
            cancelled = true;
            sendWatch.Stop();
            metrics.EndLoad();
        }
        finally
        {
            // 정리를 위한 연결 종료는 예기치 않은 연결 종료에 포함하지 않는다.
            metrics.Stopping = true;
            await Task.WhenAll(connects);
            foreach (StressClient client in clients) await client.DisposeAsync();
        }

        foreach (StressClient client in clients.Where(client => client.Error is not null).Take(5))
            Console.WriteLine($"클라이언트 {client.Index} 오류: {client.Error}");
        return metrics.PrintResult(sendWatch.Elapsed.TotalSeconds, cancelled) == true ? 0 : 1;
    }
}
