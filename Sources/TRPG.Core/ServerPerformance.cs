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
    private int running;
    private ServerPerformanceSnapshot? lastSnapshot;

    public void SetRunning(bool value) => Volatile.Write(ref running, value == true ? 1 : 0);

    public ServerPerformance(TcpServer server, string sessionId)
    {
        // 채팅 처리 루프가 DB 응답을 기다릴 때도 독립적으로 서버 상태를 측정한다.
        sampler = Task.Run(() => SampleAsync(server, sessionId, cancellation.Token));
    }

    public static DatabaseWriteMeasurement MeasureDatabaseWrite() => new();

    private async Task SampleAsync(TcpServer server, string sessionId, CancellationToken cancellation)
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
            await Task.Delay(500, cancellation);
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
                Version = 2,
                SessionId = sessionId,
                CollectedAt = DateTimeOffset.UtcNow,
                ServerRunning = Volatile.Read(ref running) == 1,
                DatabaseEnabled = ChatDatabase.Enabled,
                ProcessId = Environment.ProcessId,
                LogicalProcessors = Environment.ProcessorCount,
                OperatingSystem = System.Runtime.InteropServices.RuntimeInformation.OSDescription,
                Architecture = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString(),
                UptimeSeconds = watch.Elapsed.TotalSeconds,
                Threads = process.Threads.Count,
                ManagedMemoryBytes = GC.GetTotalMemory(false),
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
            lastSnapshot = snapshot;

            try
            {
                // 임시 파일에서 교체해 읽는 쪽에 불완전한 JSON을 전달하지 않는다.
                await File.WriteAllTextAsync(path + ".tmp", JsonSerializer.Serialize(snapshot), cancellation);
                ReplaceSnapshotFile(path);
                if (reportedFileError == true) Console.WriteLine("서버 지표 파일 저장이 복구되었습니다.");
                reportedFileError = false;
            }
            catch (Exception error) when (error is IOException or UnauthorizedAccessException)
            {
                if (reportedFileError == false)
                    Console.WriteLine($"서버 지표 파일 저장 실패: {path} | {error.GetType().Name} (0x{error.HResult:X8}): {error.Message}");
                reportedFileError = true;
            }
            previousCpu = cpu;
            previousTime = sampledAt;
            previousHostCpu = hostCpu;
            previousNetwork = network;
        }
    }

    private static void ReplaceSnapshotFile(string path)
    {
        // 기존 파일은 원자적으로 교체한다. Windows에서 열린 파일의 덮어쓰기 이동을 피한다.
        if (File.Exists(path) == true) File.Replace(path + ".tmp", path, null);
        else File.Move(path + ".tmp", path);
    }

    private static (long Attempts, long Completed, long Failures, long Ticks, long MaxTicks) ReadDatabaseStatistics()
    {
        // 완료 건수와 시간의 표본 경계가 어긋나지 않도록 함께 읽는다.
        lock (databaseLock) return (databaseAttempts, databaseCompleted, databaseFailures, databaseTicks, databaseMaxTicks);
    }

    private static (long Total, long Idle)? ReadHostCpu()
    {
        if (OperatingSystem.IsWindows() == true) return WindowsHostResources.ReadCpu();
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
        if (OperatingSystem.IsWindows() == true) return WindowsHostResources.ReadMemory();
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
        if (lastSnapshot is not null)
        {
            try
            {
                // 정상 종료를 명시적으로 남긴다. 강제 종료는 갱신 중단으로만 감지할 수 있다.
                string path = Path.Combine(AppContext.BaseDirectory, "server-metrics.json");
                await File.WriteAllTextAsync(path + ".tmp", JsonSerializer.Serialize(lastSnapshot with
                {
                    ServerRunning = false,
                    CollectedAt = DateTimeOffset.UtcNow
                }));
                ReplaceSnapshotFile(path);
            }
            catch (Exception error) when (error is IOException or UnauthorizedAccessException)
            {
                Console.WriteLine($"서버 종료 상태 파일을 기록하지 못했습니다: {error.Message}");
            }
        }
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
