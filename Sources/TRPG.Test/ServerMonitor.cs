using System.Diagnostics;
using System.Text.Json;
using TRPG.Common;

namespace TRPG.Test;

internal sealed record ServerMonitorState(ServerPerformanceSnapshot? Latest, bool Fresh, bool SourceReachable,
    string? Error, int RestartCount, double? AgeSeconds);

internal sealed class ServerMonitor : IAsyncDisposable
{
    private readonly object sync = new();
    private readonly CancellationTokenSource cancellation = new();
    private readonly List<ServerPerformanceSnapshot> samples = new();
    private readonly Task worker;
    private string? error;
    private int restartCount;
    private DateTimeOffset lastRead;
    private DateTimeOffset lastChanged;

    public ServerMonitor(TestOptions options)
    {
        // 테스트가 끝나거나 서버가 재시작돼도 대시보드의 감시는 계속한다.
        worker = Task.Run(() => ReadAsync(options, cancellation.Token));
    }

    public ServerMonitorState Capture()
    {
        lock (sync)
        {
            ServerPerformanceSnapshot? latest = samples.LastOrDefault();
            double? age = latest is null || lastChanged == DateTimeOffset.MinValue
                ? null : (DateTimeOffset.UtcNow - lastChanged).TotalSeconds;
            bool reachable = error is null && (DateTimeOffset.UtcNow - lastRead).TotalSeconds <= 2;
            return new ServerMonitorState(latest, reachable == true && latest?.ServerRunning == true && age <= 2,
                reachable, error, restartCount, age);
        }
    }

    public async Task<bool> WaitForReadyAsync(CancellationToken cancellation)
    {
        // 단순히 오래된 파일이 존재하는 것과 현재 서버 실행을 구분한다.
        var timeout = Stopwatch.StartNew();
        while (timeout.Elapsed.TotalSeconds < 10)
        {
            if (Capture().Fresh == true) return true;
            await Task.Delay(100, cancellation);
        }
        return false;
    }

    private async Task ReadAsync(TestOptions options, CancellationToken cancellation)
    {
        try
        {
            while (cancellation.IsCancellationRequested == false)
            {
                try
                {
                    if (options.IsLoopback == true)
                    {
                        // Windows에서도 서버의 원자적 파일 교체를 방해하지 않는다.
                        using (var file = new FileStream(options.ServerMetricsFile, FileMode.Open, FileAccess.Read,
                            FileShare.ReadWrite | FileShare.Delete, 4096, FileOptions.Asynchronous))
                        using (var reader = new StreamReader(file))
                        {
                            Collect(await reader.ReadToEndAsync(cancellation));
                        }
                        // 다음 표본을 기다리는 동안에는 파일 핸들을 유지하지 않는다.
                        await Task.Delay(500, cancellation);
                    }
                    else
                    {
                        await ReadRemoteAsync(options, cancellation);
                    }
                }
                catch (Exception failure) when (failure is IOException or UnauthorizedAccessException or System.ComponentModel.Win32Exception)
                {
                    lock (sync) error = $"서버 지표 수집 실패: {failure.Message}";
                    await Task.Delay(1000, cancellation);
                }
            }
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested == true) { }
    }

    private async Task ReadRemoteAsync(TestOptions options, CancellationToken cancellation)
    {
        if (File.Exists(options.SshKey) == false) throw new IOException("SSH 키 파일이 없습니다. --ssh-key 경로를 확인하세요.");
        var start = new ProcessStartInfo("ssh")
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };
        // 기존 SSH 인증과 known_hosts를 사용하고 공개 지표 포트는 열지 않는다.
        foreach (string argument in new[] { "-i", options.SshKey, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
            "-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=2", "-o", "ServerAliveCountMax=2", $"ubuntu@{options.Host}",
            "while true; do if test -r /home/ubuntu/trpg-server/server-metrics.json; then cat /home/ubuntu/trpg-server/server-metrics.json; printf '\\n'; fi; sleep 0.5; done" })
            start.ArgumentList.Add(argument);
        using Process process = Process.Start(start) ?? throw new IOException("SSH 지표 수집을 시작하지 못했습니다.");
        Task<string> stderr = process.StandardError.ReadToEndAsync(cancellation);
        try
        {
            while (await process.StandardOutput.ReadLineAsync(cancellation) is string line) Collect(line);
            string detail = await stderr;
            throw new IOException($"SSH 연결 종료: {detail[..Math.Min(300, detail.Length)]}");
        }
        finally
        {
            try { if (process.HasExited == false) process.Kill(entireProcessTree: true); }
            catch (InvalidOperationException) { } // 확인 직후 SSH가 종료된 경우다.
        }
    }

    private void Collect(string json)
    {
        ServerPerformanceSnapshot? sample;
        try { sample = JsonSerializer.Deserialize<ServerPerformanceSnapshot>(json); }
        catch (JsonException)
        {
            lock (sync) error = "서버 지표 JSON 형식이 올바르지 않습니다.";
            return;
        }
        if (sample is null || string.IsNullOrEmpty(sample.SessionId) == true) return;
        if (sample.Version != 2)
        {
            lock (sync) error = "구버전 서버 지표입니다. 최신 서버를 재배포해 주세요.";
            return;
        }
        lock (sync)
        {
            error = null;
            lastRead = DateTimeOffset.UtcNow;
            if (samples.Count > 0)
            {
                if (sample.SessionId != samples[^1].SessionId)
                {
                    restartCount++;
                    samples.Clear();
                }
                else if (sample.CollectedAt <= samples[^1].CollectedAt) return;
            }
            // 파일을 처음 읽었을 때는 실제 갱신 여부가 확인되지 않아 준비로 판정하지 않는다.
            lastChanged = samples.Count == 0 ? DateTimeOffset.MinValue : lastRead;
            samples.Add(sample);
            if (samples.Count > 3600) samples.RemoveAt(0);
        }
    }

    public void PrintResult()
    {
        ServerPerformanceSnapshot[] values;
        lock (sync) values = samples.ToArray();
        Console.WriteLine("서버 측 지표 (0.5초 목표 간격, 최근 최대 30분 표본)");
        if (values.Length == 0) { Console.WriteLine("표본 없음"); return; }
        var last = values[^1];
        Console.WriteLine($"논리 프로세서 {last.LogicalProcessors}, RAM {last.HostMemoryTotalBytes / 1073741824.0:F1}GiB, " +
            $"CPU 표본 최대 {values.Max(value => value.ProcessCpuPercent):F1}%, 메모리 표본 최대 {values.Max(value => value.ProcessMemoryBytes) / 1048576.0:F1}MiB");
        Console.WriteLine($"수신 큐 표본 최대 {values.Max(value => value.ReceiveQueue):N0}, 마지막 {last.ReceiveQueue:N0}, " +
            $"DB 활성 {last.DatabaseEnabled}, DB 완료 누계 {last.DatabaseCompleted:N0}, 실패 누계 {last.DatabaseFailures:N0}");
    }

    public async ValueTask DisposeAsync()
    {
        cancellation.Cancel();
        await worker;
        cancellation.Dispose();
    }
}
