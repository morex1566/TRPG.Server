using System.Diagnostics;
using System.Security.Cryptography;
using TRPG.Networking;
using TRPG.Protocol;

namespace TRPG.Test;

internal static class RandomScenario
{
    public static async Task<int> RunAsync(TestOptions options, CancellationToken cancellation)
    {
        var metrics = new TestMetrics { RandomWorkload = true };
        var slots = new StressClient?[options.Clients];
        var attempts = new List<Task>();
        var watch = new Stopwatch();
        var ids = new MessageIds();
        bool cancelled = false;
        Console.WriteLine($"대상: {options.Host}:{options.Port}, 최대 동시 슬롯: {options.Clients}, 부하 시간: {options.Seconds}초");
        Console.WriteLine("각 접속자 500~1000ms 간격, 본문 총 8~16B. 10~30초 접속 / 1~5초 재접속 대기.");
        Console.WriteLine("초기 접속 중에는 채팅을 보내지 않습니다. 송신 성공은 서버의 DB 저장 성공을 뜻하지 않습니다.");
        if (options.IsLoopback == false) Console.WriteLine("실제 VM에 대량의 중계 트래픽과 DB 저장 부하가 발생합니다.");

        try
        {
            // 동시 소켓 수는 슬롯 수를 넘지 않으며 초기 연결은 기존 간격으로 분산한다.
            for (int i = 0; i < slots.Length; i++)
            {
                cancellation.ThrowIfCancellationRequested();
                var client = new StressClient(i, "", metrics, cancellation);
                slots[i] = client;
                attempts.Add(client.ConnectAsync(options));
                if (options.RampMs > 0) await Task.Delay(options.RampMs, cancellation);
            }
            await Task.WhenAll(attempts);
            Console.WriteLine($"초기 접속 성공: {slots.Count(client => client?.IsConnected == true)}, 실패: {metrics.ConnectFailures}");
            metrics.BeginLoad();
            watch.Start();

            // 각 슬롯이 독립적으로 채팅과 접속 수명을 결정한다.
            Task[] workers = Enumerable.Range(0, slots.Length)
                .Select(index => RunSlotAsync(index, slots, options, metrics, ids, watch, cancellation)).ToArray();
            await Task.WhenAll(workers.Append(ReportProgressAsync(slots, metrics, watch, options.Seconds, cancellation)));
            watch.Stop();
            metrics.EndLoad();
            Console.WriteLine($"송신 종료. 마지막 연결을 유지하며 {options.DrainSeconds}초 수신을 기다립니다.");
            await Task.Delay(TimeSpan.FromSeconds(options.DrainSeconds), cancellation);
            await VerifyDeliveryAsync(slots, options, metrics, ids, cancellation);
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested == true)
        {
            cancelled = true;
            watch.Stop();
            metrics.EndLoad();
        }
        finally
        {
            metrics.Stopping = true;
            await Task.WhenAll(attempts);
            foreach (StressClient? client in slots)
            {
                if (client is not null) await client.DisposeAsync();
            }
        }

        return metrics.PrintResult(watch.Elapsed.TotalSeconds, cancelled) == true ? 0 : 1;
    }

    private static async Task VerifyDeliveryAsync(StressClient?[] slots, TestOptions options,
        TestMetrics metrics, MessageIds ids, CancellationToken cancellation)
    {
        // 무작위 접속을 끝낸 뒤 구성원을 고정해 기대 수신자 수를 정확히 정한다.
        StressClient[] participants = slots.OfType<StressClient>().Where(client => client.IsConnected == true).ToArray();
        if (participants.Length < 2)
        {
            Console.WriteLine("고정 구성 누락/중복 검증: 수신자가 없어 측정 불가");
            return;
        }
        Console.WriteLine($"고정 구성 검증: {participants.Length}명, 메시지 3개, 수신 대기 {options.DrainSeconds}초");
        for (int i = 0; i < 3; i++)
        {
            StressClient sender = participants[i % participants.Length];
            int id = Interlocked.Increment(ref ids.Next) ^ ids.Salt;
            metrics.RegisterProbe(id, sender.Index, options.Clients, participants.Length - 1);
            PacketContext packet = PacketContext.Create(PayloadType.ChatMessageRequest, builder =>
            {
                var text = builder.CreateString(id.ToString("X8"));
                return ChatMessageRequest.CreateChatMessageRequest(builder, ChatType.ToServer, 0, text).Value;
            });
            try
            {
                await sender.SendAsync(packet.Buffer, cancellation);
                metrics.ConfirmSent(id, packet.Buffer.Length);
            }
            catch (Exception error) when (error is IOException or System.Net.Sockets.SocketException or OperationCanceledException or ObjectDisposedException)
            {
                cancellation.ThrowIfCancellationRequested();
                Interlocked.Increment(ref metrics.SendErrors);
            }
        }
        await Task.Delay(TimeSpan.FromSeconds(options.DrainSeconds), cancellation);
    }

    private static async Task RunSlotAsync(int index, StressClient?[] slots, TestOptions options,
        TestMetrics metrics, MessageIds ids, Stopwatch watch, CancellationToken cancellation)
    {
        double disconnectAt = watch.Elapsed.TotalSeconds + Random.Shared.Next(10, 31);
        while (watch.Elapsed.TotalSeconds < options.Seconds)
        {
            cancellation.ThrowIfCancellationRequested();
            StressClient client = slots[index]!;
            if (client.IsConnected == false || watch.Elapsed.TotalSeconds >= disconnectAt)
            {
                await client.DisposeAsync();
                slots[index] = null;
                await DelayUntilEndAsync(Random.Shared.Next(1000, 5001), options, watch, cancellation);
                if (watch.Elapsed.TotalSeconds >= options.Seconds) break;

                client = new StressClient(index, "", metrics, cancellation);
                slots[index] = client;
                await client.ConnectAsync(options);
                disconnectAt = watch.Elapsed.TotalSeconds + Random.Shared.Next(10, 31);
                continue;
            }

            await DelayUntilEndAsync(Random.Shared.Next(500, 1001), options, watch, cancellation);
            if (watch.Elapsed.TotalSeconds >= options.Seconds) break;
            int sequence = Interlocked.Increment(ref ids.Next);
            if (sequence > TestOptions.MaxMessages) throw new InvalidOperationException("메시지 추적 한도를 초과했습니다.");

            // 8자리 ASCII 식별자를 본문 안에 포함해 UTF-8 총 길이를 정확히 8~16B로 맞춘다.
            int id = sequence ^ ids.Salt;
            string message = id.ToString("X8") + new string('a', Random.Shared.Next(0, 9));
            PacketContext packet = PacketContext.Create(PayloadType.ChatMessageRequest, builder =>
            {
                var text = builder.CreateString(message);
                return ChatMessageRequest.CreateChatMessageRequest(builder, ChatType.ToServer, 0, text).Value;
            });
            metrics.Register(id, index, 0);
            try
            {
                await client.SendAsync(packet.Buffer, cancellation);
                metrics.ConfirmSent(id, packet.Buffer.Length);
            }
            catch (Exception error) when (error is IOException or System.Net.Sockets.SocketException or OperationCanceledException or ObjectDisposedException)
            {
                cancellation.ThrowIfCancellationRequested();
                Interlocked.Increment(ref metrics.SendErrors);
            }
        }
    }

    private static async Task DelayUntilEndAsync(int milliseconds, TestOptions options, Stopwatch watch, CancellationToken cancellation)
    {
        double remaining = Math.Max(0, options.Seconds - watch.Elapsed.TotalSeconds);
        await Task.Delay(TimeSpan.FromMilliseconds(Math.Min(milliseconds, remaining * 1000)), cancellation);
    }

    private static async Task ReportProgressAsync(StressClient?[] slots, TestMetrics metrics, Stopwatch watch,
        int seconds, CancellationToken cancellation)
    {
        while (watch.Elapsed.TotalSeconds < seconds)
        {
            await Task.Delay(TimeSpan.FromSeconds(Math.Min(5, Math.Max(0, seconds - watch.Elapsed.TotalSeconds))), cancellation);
            Console.WriteLine($"[{watch.Elapsed.TotalSeconds:F0}초] 연결 {slots.Count(client => client?.IsConnected == true)}, " +
                $"접속 성공 누계 {metrics.Connected}, 실패 {metrics.ConnectFailures}, 예기치 않은 종료 {metrics.Disconnected}, " +
                $"송신 오류 {metrics.SendErrors}, 송신 {Interlocked.Read(ref metrics.SentBytes):N0}B, " +
                $"수신 {Interlocked.Read(ref metrics.ReceivedBytes):N0}B");
        }
    }

    private sealed class MessageIds
    {
        // 실행별 식별 공간을 달리해 일반 채팅을 테스트 수신으로 잘못 집계할 가능성을 낮춘다.
        public readonly int Salt = RandomNumberGenerator.GetInt32(int.MaxValue);
        public int Next;
    }
}
