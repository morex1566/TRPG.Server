using System.Diagnostics;
using System.Net.NetworkInformation;
using System.Text.Json;
using TRPG.Common;
using TRPG.Networking;

namespace TRPG.Core;

public sealed class ServerPerformance : IAsyncDisposable
{
    private static readonly object databaseLock = new();
    private static long databaseAttempts;
    private static long databaseCompleted;
    private static long databaseFailures;
    private static long databaseTicks;
    private static long databaseMaxTicks;
    private readonly CancellationTokenSource cancellation = new();
    private readonly Task sampler;

    public ServerPerformance(TcpServer server, string sessionId)
    {
        // 채팅 처리 루프가 DB 응답을 기다릴 때도 독립적으로 서버 상태를 측정한다.
        sampler = Task.Run(() => SampleAsync(server, sessionId, cancellation.Token));
    }

    public static DatabaseWriteMeasurement MeasureDatabaseWrite() => new();

    private static async Task SampleAsync(TcpServer server, string sessionId, CancellationToken cancellation)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "server-metrics.json");
        using Process process = Process.GetCurrentProcess();
        var watch = Stopwatch.StartNew();
        double previousTime = 0;
        TimeSpan previousCpu = process.TotalProcessorTime;
        (long Total, long Idle)? previousHostCpu = ReadHostCpu();
        (long Received, long Sent)? previousNetwork = ReadNetwork();
        bool reportedFileError = false;

        while (cancellation.IsCancellationRequested == false)
        {
            await Task.Delay(1000, cancellation);
            process.Refresh();
            double sampledAt = watch.Elapsed.TotalSeconds;
            double elapsed = Math.Max(0.001, sampledAt - previousTime);
            TimeSpan cpu = process.TotalProcessorTime;
            var hostCpu = ReadHostCpu();
            var network = ReadNetwork();
            var memory = ReadHostMemory();
            var database = ReadDatabaseStatistics();
            double? hostPercent = null;
            if (hostCpu is { } current && previousHostCpu is { } prior && current.Total > prior.Total)
                hostPercent = 100.0 * (1 - (double)(current.Idle - prior.Idle) / (current.Total - prior.Total));

            var snapshot = new ServerPerformanceSnapshot
            {
                SessionId = sessionId,
                CollectedAt = DateTimeOffset.UtcNow,
                Connections = server.ConnectionCount,
                ReceiveQueue = server.RecvQueue.Count,
                ProcessCpuPercent = Math.Clamp((cpu - previousCpu).TotalSeconds / elapsed / Environment.ProcessorCount * 100, 0, 100),
                ProcessMemoryBytes = process.WorkingSet64,
                HostCpuPercent = hostPercent,
                HostMemoryUsedBytes = memory?.Used,
                HostMemoryTotalBytes = memory?.Total,
                NetworkReceivedBytes = network?.Received,
                NetworkSentBytes = network?.Sent,
                NetworkReceiveBytesPerSecond = network is { } rx && previousNetwork is { } oldRx
                    ? Math.Max(0, rx.Received - oldRx.Received) / elapsed : null,
                NetworkSendBytesPerSecond = network is { } tx && previousNetwork is { } oldTx
                    ? Math.Max(0, tx.Sent - oldTx.Sent) / elapsed : null,
                DatabaseAttempts = database.Attempts,
                DatabaseCompleted = database.Completed,
                DatabaseFailures = database.Failures,
                DatabaseTotalMilliseconds = database.Ticks * 1000.0 / Stopwatch.Frequency,
                DatabaseMaxMilliseconds = database.MaxTicks * 1000.0 / Stopwatch.Frequency
            };

            try
            {
                // 임시 파일에서 교체해 읽는 쪽에 불완전한 JSON을 전달하지 않는다.
                await File.WriteAllTextAsync(path + ".tmp", JsonSerializer.Serialize(snapshot), cancellation);
                File.Move(path + ".tmp", path, true);
            }
            catch (Exception error) when (error is IOException or UnauthorizedAccessException)
            {
                if (reportedFileError == false) Console.WriteLine($"서버 지표 파일 저장 실패: {error.GetType().Name}");
                reportedFileError = true;
            }
            previousCpu = cpu;
            previousTime = sampledAt;
            previousHostCpu = hostCpu;
            previousNetwork = network;
        }
    }

    private static (long Attempts, long Completed, long Failures, long Ticks, long MaxTicks) ReadDatabaseStatistics()
    {
        // 완료 건수와 시간의 표본 경계가 어긋나지 않도록 함께 읽는다.
        lock (databaseLock) return (databaseAttempts, databaseCompleted, databaseFailures, databaseTicks, databaseMaxTicks);
    }

    private static (long Total, long Idle)? ReadHostCpu()
    {
        if (OperatingSystem.IsLinux() == false) return null;
        try
        {
            string[] parts = File.ReadLines("/proc/stat").First().Split(' ', StringSplitOptions.RemoveEmptyEntries);
            return (parts.Skip(1).Take(8).Sum(long.Parse), long.Parse(parts[4]) + long.Parse(parts[5]));
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or FormatException) { return null; }
    }

    private static (long Used, long Total)? ReadHostMemory()
    {
        if (OperatingSystem.IsLinux() == false) return null;
        try
        {
            var values = File.ReadLines("/proc/meminfo")
                .Select(line => line.Split(' ', StringSplitOptions.RemoveEmptyEntries))
                .ToDictionary(parts => parts[0], parts => long.Parse(parts[1]) * 1024);
            return (values["MemTotal:"] - values["MemAvailable:"], values["MemTotal:"]);
        }
        catch (Exception error) when (error is IOException or UnauthorizedAccessException or FormatException) { return null; }
    }

    private static (long Received, long Sent)? ReadNetwork()
    {
        try
        {
            var statistics = NetworkInterface.GetAllNetworkInterfaces()
                .Where(network => network.OperationalStatus == OperationalStatus.Up && network.NetworkInterfaceType != NetworkInterfaceType.Loopback)
                .Select(network => network.GetIPStatistics()).ToArray();
            return (statistics.Sum(value => value.BytesReceived), statistics.Sum(value => value.BytesSent));
        }
        catch (NetworkInformationException) { return null; }
    }

    public async ValueTask DisposeAsync()
    {
        cancellation.Cancel();
        try { await sampler; }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested == true) { }
        cancellation.Dispose();
    }

    public sealed class DatabaseWriteMeasurement : IDisposable
    {
        private readonly long started = Stopwatch.GetTimestamp();
        private bool succeeded;

        public DatabaseWriteMeasurement()
        {
            lock (databaseLock) databaseAttempts++;
        }
        public void Succeed() => succeeded = true;

        public void Dispose()
        {
            // 연결 풀 대기와 실제 INSERT를 포함한 저장 지연 및 실패를 집계한다.
            long ticks = Stopwatch.GetTimestamp() - started;
            lock (databaseLock)
            {
                databaseTicks += ticks;
                databaseMaxTicks = Math.Max(databaseMaxTicks, ticks);
                if (succeeded == false) databaseFailures++;
                databaseCompleted++;
            }
        }
    }
}
