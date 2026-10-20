using System.Diagnostics;
using System.Text.Json;
using TRPG.Common;

namespace TRPG.Test;

internal sealed class ServerMonitor : IAsyncDisposable
{
    private readonly CancellationTokenSource cancellation = new();
    private readonly List<ServerPerformanceSnapshot> samples = new();
    private readonly Task worker;
    private readonly TaskCompletionSource<bool> ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Process? ssh;
    private string? error;
    private bool restarted;
    private DateTimeOffset lastReceived;
    private DateTimeOffset lastPrinted;

    public bool HasFreshData => samples.Count >= 2 && restarted == false && error is null &&
        (DateTimeOffset.UtcNow - lastReceived).TotalSeconds <= 5;

    public bool HasDatabaseFailures => samples.Count > 1 && samples[^1].DatabaseFailures > samples[0].DatabaseFailures;

    public ServerMonitor(TestOptions options)
    {
        worker = Task.Run(() => ReadAsync(options, cancellation.Token));
    }

    public async Task<bool> WaitForReadyAsync(CancellationToken cancellation)
    {
        try { return await ready.Task.WaitAsync(TimeSpan.FromSeconds(10), cancellation); }
        catch (TimeoutException)
        {
            error ??= "서버 지표가 10초 안에 갱신되지 않았습니다. 최신 서버 실행 여부를 확인하세요.";
            return false;
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested == true) { return false; }
    }

    private async Task ReadAsync(TestOptions options, CancellationToken cancellation)
    {
        try
        {
            if (options.IsLoopback == true)
            {
                while (cancellation.IsCancellationRequested == false)
                {
                    try { Collect(await File.ReadAllTextAsync(options.ServerMetricsFile, cancellation)); }
                    catch (IOException) { error = $"로컬 지표 파일을 읽을 수 없습니다: {options.ServerMetricsFile}"; }
                    await Task.Delay(1000, cancellation);
                }
                return;
            }

            if (File.Exists(options.SshKey) == false)
            {
                error = "SSH 키 파일이 없습니다. --ssh-key로 기존 개인 키 경로를 지정하세요.";
                return;
            }
            var start = new ProcessStartInfo("ssh")
            {
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };
            // 기존 SSH 인증을 사용한다. 비밀번호 입력, 호스트 키 확인 우회, 공개 지표 포트는 없다.
            foreach (string argument in new[] { "-i", options.SshKey, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=2", $"ubuntu@{options.Host}",
                "while true; do if test -r /home/ubuntu/trpg-server/server-metrics.json; then cat /home/ubuntu/trpg-server/server-metrics.json; printf '\\n'; fi; sleep 1; done" })
                start.ArgumentList.Add(argument);
            ssh = Process.Start(start) ?? throw new IOException("SSH 지표 수집을 시작하지 못했습니다.");
            Task<string> stderr = ssh.StandardError.ReadToEndAsync(cancellation);
            while (await ssh.StandardOutput.ReadLineAsync(cancellation) is string line) Collect(line);
            await ssh.WaitForExitAsync(cancellation);
            string detail = await stderr;
            error = $"SSH 지표 수집 종료({ssh.ExitCode}): {detail[..Math.Min(300, detail.Length)]}";
        }
        catch (OperationCanceledException) when (cancellation.IsCancellationRequested == true) { }
        catch (Exception failure) when (failure is IOException or UnauthorizedAccessException or System.ComponentModel.Win32Exception)
        {
            error = $"서버 지표 수집 실패: {failure.Message}";
        }
        finally
        {
            ready.TrySetResult(false);
        }
    }

    private void Collect(string json)
    {
        ServerPerformanceSnapshot? sample;
        try { sample = JsonSerializer.Deserialize<ServerPerformanceSnapshot>(json); }
        catch (JsonException) { error = "서버 지표 JSON 형식이 올바르지 않습니다."; return; }
        if (sample is null || string.IsNullOrEmpty(sample.SessionId) == true) return;
        if (samples.Count > 0)
        {
            if (sample.SessionId != samples[0].SessionId)
            {
                restarted = true;
                error = "측정 중 서버가 재시작돼 지표를 합산하지 않습니다.";
                return;
            }
            if (sample.CollectedAt <= samples[^1].CollectedAt) return;
        }
        samples.Add(sample);
        lastReceived = DateTimeOffset.UtcNow;
        error = null;
        // 갱신되는 표본 두 개를 먼저 확보해 DB 카운터의 기준 시점을 부하 전에 잡는다.
        if (samples.Count >= 2) ready.TrySetResult(true);
        if ((lastReceived - lastPrinted).TotalSeconds >= 5)
        {
            Console.WriteLine($"[서버] 연결 {sample.Connections}, 수신 큐 {sample.ReceiveQueue:N0}, " +
                $"프로세스 CPU {sample.ProcessCpuPercent:F1}%, 메모리 {sample.ProcessMemoryBytes / 1048576.0:F1}MiB, " +
                $"DB 완료 {sample.DatabaseCompleted:N0}, 실패 {sample.DatabaseFailures:N0}");
            lastPrinted = lastReceived;
        }
    }

    public async Task StopAsync()
    {
        cancellation.Cancel();
        // 읽기 취소 후 수집 태스크가 SSH 프로세스를 생성하는 경합도 정리한다.
        await worker;
        try
        {
            if (ssh is not null && ssh.HasExited == false) ssh.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException) { } // 확인 직후 SSH가 스스로 종료한 경우다.
    }

    public void PrintResult()
    {
        Console.WriteLine();
        Console.WriteLine("서버 측 지표 (1초 간격 표본, 초기 접속·드레인·검증 구간 포함)");
        if (HasFreshData == false)
            Console.WriteLine($"지표 수집 불완전: {error ?? "새 표본이 없습니다. 서버 재배포 및 지표 파일 생성 여부를 확인하세요."}");
        if (samples.Count == 0) return;
        ServerPerformanceSnapshot first = samples[0];
        ServerPerformanceSnapshot last = samples[^1];
        Console.WriteLine($"표본 {samples.Count}, 관측 최대 연결 {samples.Max(value => value.Connections)}, " +
            $"수신 큐 표본 최대 {samples.Max(value => value.ReceiveQueue):N0}, 마지막 {last.ReceiveQueue:N0}");
        if (last.ReceiveQueue > 0) Console.WriteLine("측정 종료 시에도 서버에 처리 대기가 남아 있습니다.");
        Console.WriteLine($"서버 프로세스 CPU 평균 {samples.Average(value => value.ProcessCpuPercent):F1}%, " +
            $"최대 {samples.Max(value => value.ProcessCpuPercent):F1}% (할당 코어 전체 기준), " +
            $"프로세스 메모리 표본 최대 {samples.Max(value => value.ProcessMemoryBytes) / 1048576.0:F1}MiB");
        var hostCpu = samples.Where(value => value.HostCpuPercent.HasValue == true).ToArray();
        Console.WriteLine(hostCpu.Length == 0 ? "VM 전체 CPU/메모리: 이 OS에서는 수집하지 않음" :
            $"VM 전체 CPU 평균 {hostCpu.Average(value => value.HostCpuPercent!.Value):F1}%, " +
            $"최대 {hostCpu.Max(value => value.HostCpuPercent!.Value):F1}%, " +
            $"마지막 사용 메모리 {last.HostMemoryUsedBytes / 1048576.0:F1}/{last.HostMemoryTotalBytes / 1048576.0:F1}MiB (캐시 회수 가능량 제외)");
        var network = samples.Where(value => value.NetworkSendBytesPerSecond.HasValue == true).ToArray();
        Console.WriteLine(network.Length == 0 ? "VM 네트워크: 측정 없음" :
            $"VM NIC 평균 RX {network.Average(value => value.NetworkReceiveBytesPerSecond!.Value) / 1048576:F2}MiB/s, " +
            $"TX {network.Average(value => value.NetworkSendBytesPerSecond!.Value) / 1048576:F2}MiB/s, " +
            $"최대 TX {network.Max(value => value.NetworkSendBytesPerSecond!.Value) / 1048576:F2}MiB/s (DB·SSH 등 다른 통신 포함)");
        long completed = Math.Max(0, last.DatabaseCompleted - first.DatabaseCompleted);
        long failures = Math.Max(0, last.DatabaseFailures - first.DatabaseFailures);
        Console.WriteLine(completed == 0 ? "DB 저장 완료 표본: 없음" :
            $"DB 저장 완료 {completed:N0}, 실패 {failures:N0} ({100.0 * failures / completed:F3}%), " +
            $"평균 {(last.DatabaseTotalMilliseconds - first.DatabaseTotalMilliseconds) / completed:F2}ms (연결 풀 대기 포함)");
        Console.WriteLine($"DB 진행 중 {Math.Max(0, last.DatabaseAttempts - last.DatabaseCompleted)}, " +
            $"서버 시작 이후 DB 최대 지연 {last.DatabaseMaxMilliseconds:F2}ms");
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        ssh?.Dispose();
        cancellation.Dispose();
    }
}
