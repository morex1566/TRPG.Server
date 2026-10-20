using System.Collections.Concurrent;
using System.Diagnostics;

namespace TRPG.Test;

internal sealed class TestMetrics
{
    private readonly ConcurrentDictionary<int, Delivery> messages = new();
    private readonly long[] latencyBuckets = new long[10002];
    private readonly long[] connectBuckets = new long[10002];
    private readonly ConcurrentQueue<string> errorSamples = new();
    private readonly ConcurrentQueue<int> probeMessages = new();
    private int errorSampleCount;
    public int Recipients;
    private long latencyMicroseconds;
    private long latencyCount;
    private long connectMicroseconds;
    private long connectSamples;
    private long receivedDeliveries;
    private long successfulSends;
    private long loadSentMessages;
    private long uniqueReceipts;
    private long loadStarted;
    private long loadStopped;
    private long loadDeliveries;
    private int activeConnections;
    private int peakConnections;
    private int connectionsAtLoadEnd;

    public int ConnectionAttempts;

    public int Connected;
    public int ConnectFailures;
    public int Disconnected;
    public int PlannedDisconnects;
    public bool RandomWorkload;
    public int SendErrors;
    public long SentBytes;
    public long ReceivedBytes;
    public long Duplicates;
    public long Unexpected;
    public volatile bool Stopping;

    public void ConnectionOpened()
    {
        int active = Interlocked.Increment(ref activeConnections);
        int peak = Volatile.Read(ref peakConnections);
        while (active > peak)
        {
            int observed = Interlocked.CompareExchange(ref peakConnections, active, peak);
            if (observed == peak) break;
            peak = observed;
        }
    }

    public void ConnectionClosed() => Interlocked.Decrement(ref activeConnections);

    public void RecordConnectionTime(long started)
    {
        double milliseconds = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        Interlocked.Add(ref connectMicroseconds, (long)(milliseconds * 1000));
        Interlocked.Increment(ref connectBuckets[(int)Math.Min(10001, Math.Ceiling(milliseconds))]);
        Interlocked.Increment(ref connectSamples);
    }

    public void BeginLoad() => loadStarted = Stopwatch.GetTimestamp();

    public void EndLoad()
    {
        if (loadStarted == 0 || loadStopped != 0) return;
        loadStopped = Stopwatch.GetTimestamp();
        loadDeliveries = Interlocked.Read(ref receivedDeliveries);
        connectionsAtLoadEnd = Volatile.Read(ref activeConnections);
    }

    public void RegisterProbe(int id, int sender, int clients, int recipients)
    {
        messages[id] = new Delivery(sender, clients) { Probe = true, ExpectedCount = recipients };
        probeMessages.Enqueue(id);
    }

    public void RecordClientError(int index, string message)
    {
        // 재접속으로 객체가 교체돼도 처음 발생한 오류 몇 건은 결과에 남긴다.
        if (RandomWorkload == true && Interlocked.Increment(ref errorSampleCount) <= 5)
            errorSamples.Enqueue($"클라이언트 {index}: {message}");
    }

    public void Register(int id, int sender, int clients)
    {
        messages[id] = new Delivery(sender, clients);
    }

    public void ConfirmSent(int id, int bytes)
    {
        messages[id].Confirmed = true;
        Interlocked.Increment(ref successfulSends);
        if (messages[id].Probe == false) Interlocked.Increment(ref loadSentMessages);
        Interlocked.Add(ref SentBytes, bytes);
    }

    public void Observe(int id, int receiver)
    {
        if (messages.TryGetValue(id, out Delivery? delivery) == false || receiver == delivery.Sender)
        {
            Interlocked.Increment(ref Unexpected);
            return;
        }

        Interlocked.Increment(ref receivedDeliveries);

        // 메시지별 비트셋으로 수신자를 추적해 중복을 누락으로 잘못 집계하지 않는다.
        lock (delivery)
        {
            int word = receiver / 64;
            ulong bit = 1UL << (receiver % 64);
            if ((delivery.Seen[word] & bit) != 0)
            {
                Interlocked.Increment(ref Duplicates);
                return;
            }
            delivery.Seen[word] |= bit;
            delivery.Received++;
            Interlocked.Increment(ref uniqueReceipts);
        }

        RecordLatency(delivery.Started);
    }

    public void ObserveRandom(int id, int receiver)
    {
        // 재접속 중 구성원이 바뀌므로 수신 건수만 집계하며 누락/중복 판정은 하지 않는다.
        if (messages.TryGetValue(id, out Delivery? delivery) == false) return;
        if (delivery.Probe == true)
        {
            Observe(id, receiver);
            return;
        }
        Interlocked.Increment(ref delivery.Received);
        Interlocked.Increment(ref receivedDeliveries);
        RecordLatency(delivery.Started);
    }

    private void RecordLatency(long started)
    {
        double milliseconds = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        Interlocked.Add(ref latencyMicroseconds, (long)(milliseconds * 1000));
        Interlocked.Increment(ref latencyCount);
        int bucket = (int)Math.Min(10001, Math.Ceiling(milliseconds));
        Interlocked.Increment(ref latencyBuckets[bucket]);
    }

    public object CaptureLive()
    {
        long started = Interlocked.Read(ref loadStarted);
        long stopped = Interlocked.Read(ref loadStopped);
        double elapsed = started == 0 ? 0 : Stopwatch.GetElapsedTime(started,
            stopped == 0 ? Stopwatch.GetTimestamp() : stopped).TotalSeconds;
        long samples = Interlocked.Read(ref latencyCount);
        long connections = Interlocked.Read(ref connectSamples);
        long sent = Interlocked.Read(ref successfulSends);
        long expected = 0;
        long checkedReceived = 0;
        if (RandomWorkload == true)
        {
            // 검증용 메시지 세 개만 조회해 화면 갱신 때 전체 추적 목록을 복사하지 않는다.
            foreach (int id in probeMessages)
            {
                Delivery message = messages[id];
                if (message.Confirmed == false) continue;
                expected += message.ExpectedCount;
                checkedReceived += Volatile.Read(ref message.Received);
            }
        }
        else
        {
            expected = sent * Recipients;
            checkedReceived = Interlocked.Read(ref uniqueReceipts);
        }
        long missing = Math.Max(0, expected - checkedReceived);
        return new
        {
            phase = started == 0 ? "접속 준비" : stopped == 0 ? "부하 진행" : "수신 대기 / 검증",
            elapsedSeconds = elapsed,
            activeConnections = Volatile.Read(ref activeConnections),
            peakConnections = Volatile.Read(ref peakConnections),
            connectionAttempts = Volatile.Read(ref ConnectionAttempts),
            connected = Volatile.Read(ref Connected),
            connectFailures = Volatile.Read(ref ConnectFailures),
            plannedDisconnects = Volatile.Read(ref PlannedDisconnects),
            disconnected = Volatile.Read(ref Disconnected),
            sendErrors = Volatile.Read(ref SendErrors),
            sentMessages = sent,
            receivedDeliveries = Interlocked.Read(ref receivedDeliveries),
            sentBytes = Interlocked.Read(ref SentBytes),
            receivedBytes = Interlocked.Read(ref ReceivedBytes),
            sendRate = Interlocked.Read(ref loadSentMessages) / Math.Max(0.001, elapsed),
            receiveRate = (stopped == 0 ? Interlocked.Read(ref receivedDeliveries) : Interlocked.Read(ref loadDeliveries)) / Math.Max(0.001, elapsed),
            connectSuccessPercent = ConnectionAttempts == 0 ? (double?)null : 100.0 * Connected / ConnectionAttempts,
            connectFailurePercent = ConnectionAttempts == 0 ? (double?)null : 100.0 * ConnectFailures / ConnectionAttempts,
            disconnectPercent = Connected == 0 ? (double?)null : 100.0 * Disconnected / Connected,
            sendErrorPercent = sent + SendErrors == 0 ? (double?)null : 100.0 * SendErrors / (sent + SendErrors),
            connectAverageMs = connections == 0 ? (double?)null : Interlocked.Read(ref connectMicroseconds) / 1000.0 / connections,
            connectP50Ms = PercentileValue(connectBuckets, connections, 0.5),
            connectP95Ms = PercentileValue(connectBuckets, connections, 0.95),
            connectP99Ms = PercentileValue(connectBuckets, connections, 0.99),
            latencyAverageMs = samples == 0 ? (double?)null : Interlocked.Read(ref latencyMicroseconds) / 1000.0 / samples,
            latencyP50Ms = PercentileValue(latencyBuckets, samples, 0.5),
            latencyP95Ms = PercentileValue(latencyBuckets, samples, 0.95),
            latencyP99Ms = PercentileValue(latencyBuckets, samples, 0.99),
            validationExpected = expected,
            validationReceived = checkedReceived,
            validationMissing = missing,
            duplicates = Interlocked.Read(ref Duplicates),
            lossPercent = expected == 0 ? (double?)null : 100.0 * missing / expected,
            duplicatePercent = expected == 0 ? (double?)null : 100.0 * Duplicates / expected,
            randomWorkload = RandomWorkload
        };
    }

    public bool PrintResult(double elapsedSeconds, bool cancelled)
    {
        Delivery[] confirmed = messages.Values.Where(message => message.Confirmed).ToArray();
        Delivery[] probes = confirmed.Where(message => message.Probe == true).ToArray();
        long expected = RandomWorkload == true ? probes.Sum(message => (long)message.ExpectedCount) : (long)confirmed.Length * Recipients;
        long received = confirmed.Sum(message => (long)message.Received);
        long checkedReceived = RandomWorkload == true ? probes.Sum(message => (long)message.Received) : received;
        long missing = Math.Max(0, expected - checkedReceived);
        long samples = Interlocked.Read(ref latencyCount);
        double average = samples == 0 ? 0 : Interlocked.Read(ref latencyMicroseconds) / 1000.0 / samples;
        Console.WriteLine();
        Console.WriteLine($"접속 성공 / 시도: {Connected:N0} / {ConnectionAttempts:N0}, 접속 실패: {ConnectFailures:N0}");
        Console.WriteLine($"최대 동시 연결: {peakConnections:N0}, 부하 종료 시 연결: {connectionsAtLoadEnd:N0}");
        Console.WriteLine($"접속 성공률: {Ratio(Connected, ConnectionAttempts)}, 실패율: {Ratio(ConnectFailures, ConnectionAttempts)}, " +
            $"예기치 않은 종료 / 접속 성공: {Ratio(Disconnected, Connected)}");
        long connectionSamples = Interlocked.Read(ref connectSamples);
        Console.WriteLine(connectionSamples == 0 ? "접속 시간: 측정 없음" :
            $"접속 시간(TCP+입장 응답) 평균 {connectMicroseconds / 1000.0 / connectionSamples:F2}ms, " +
            $"p50 {Percentile(connectBuckets, connectionSamples, 0.5)}, p95 {Percentile(connectBuckets, connectionSamples, 0.95)}, " +
            $"p99 {Percentile(connectBuckets, connectionSamples, 0.99)}");
        Console.WriteLine($"계획된 연결 종료: {PlannedDisconnects:N0}, 예기치 않은 연결 종료: {Disconnected:N0}, 송신 오류: {SendErrors:N0}");
        int loadSent = confirmed.Count(message => message.Probe == false);
        Console.WriteLine($"송신 오류율: {Ratio(SendErrors, confirmed.Length + SendErrors)}");
        Console.WriteLine($"부하 성공 송신: {loadSent:N0}개, 실제 방 전체 송신률: {loadSent / Math.Max(0.001, elapsedSeconds):F2}개/초");
        double receiveSeconds = loadStarted > 0 && loadStopped > loadStarted
            ? Stopwatch.GetElapsedTime(loadStarted, loadStopped).TotalSeconds : 0;
        Console.WriteLine($"부하 중 관측 중계 처리량: {loadDeliveries / Math.Max(0.001, receiveSeconds):F2}건/초 (드레인·검증 구간 제외)");
        if (RandomWorkload == true)
        {
            Console.WriteLine($"성공 송신분의 관측 수신: {received:N0}건 (무작위 구간의 손실률은 판정하지 않음)");
        }
        else
        {
            Console.WriteLine($"성공 송신분의 예상 전달: {expected:N0}, 고유 수신: {received:N0}, 대기 종료 시 미수신: {missing:N0}");
            Console.WriteLine($"중복: {Duplicates:N0}, 예상 밖 수신: {Unexpected:N0}");
        }
        Console.WriteLine($"{(RandomWorkload ? "별도 고정 구성 검증" : "고정 접속 검증")}: 예상 {expected:N0}, 고유 수신 {checkedReceived:N0}, " +
            $"미수신 {missing:N0}, 누락률 {Ratio(missing, expected)}, 중복 {Duplicates:N0}, 중복/예상 전달 {Ratio(Duplicates, expected)}");
        Console.WriteLine($"평균 전달 지연: {(samples == 0 ? "측정 없음" : $"{average:F2}ms")}, " +
            $"p50: {Percentile(latencyBuckets, samples, 0.5)}, p95: {Percentile(latencyBuckets, samples, 0.95)}, p99: {Percentile(latencyBuckets, samples, 0.99)}");
        Console.WriteLine($"테스트 도구 송신: {SentBytes:N0}B, 수신: {ReceivedBytes:N0}B (연결·공지 포함, TCP/IP 헤더 제외)");
        Console.WriteLine($"테스트 도구 피크 메모리: {Process.GetCurrentProcess().PeakWorkingSet64 / 1024.0 / 1024:F1}MiB (서버 메모리 아님)");
        foreach (string error in errorSamples) Console.WriteLine(error);

        bool deliveryPassed = RandomWorkload == true
            ? loadSent > 0 && missing == 0 && Duplicates == 0 && Unexpected == 0
            : missing == 0 && Duplicates == 0 && Unexpected == 0;
        bool passed = cancelled == false && ConnectFailures == 0 && Disconnected == 0 && SendErrors == 0 && deliveryPassed;
        if (RandomWorkload == true) Console.WriteLine("누락/중복 검증은 마지막 고정 구성 구간만 대상입니다. 예상 전달이 0이면 측정 불가입니다.");
        Console.WriteLine(cancelled ? "중단됨: 결과는 부분 집계입니다." : passed ? "통과" : "실패: 위 항목을 확인하세요.");
        return passed;
    }

    private static string Ratio(long value, long total) => total == 0 ? "측정 불가" : $"{100.0 * value / total:F3}%";

    private static string Percentile(long[] buckets, long samples, double percentile)
    {
        double? value = PercentileValue(buckets, samples, percentile);
        return value is null ? "측정 없음" : value == 10001 ? ">10000ms" : $"≤{value}ms";
    }

    private static double? PercentileValue(long[] buckets, long samples, double percentile)
    {
        if (samples == 0) return null;
        long target = (long)Math.Ceiling(samples * percentile);
        long count = 0;
        for (int i = 0; i < buckets.Length; i++)
        {
            count += Interlocked.Read(ref buckets[i]);
            if (count >= target) return i;
        }
        return null;
    }

    private sealed class Delivery
    {
        public readonly int Sender;
        public readonly long Started = Stopwatch.GetTimestamp();
        public readonly ulong[] Seen;
        public bool Confirmed;
        public int Received;
        public bool Probe;
        public int ExpectedCount;

        public Delivery(int sender, int clients)
        {
            Sender = sender;
            Seen = new ulong[(clients + 63) / 64];
        }
    }
}
