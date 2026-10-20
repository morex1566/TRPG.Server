namespace TRPG.Common;

// 서버가 기록하고 테스트 도구가 읽는 측정 값입니다. 비밀번호와 채팅 내용은 포함하지 않습니다.
public sealed record ServerPerformanceSnapshot
{
    public string SessionId { get; init; } = "";
    public DateTimeOffset CollectedAt { get; init; }
    public int Connections { get; init; }
    public int ReceiveQueue { get; init; }
    public double ProcessCpuPercent { get; init; }
    public long ProcessMemoryBytes { get; init; }
    public double? HostCpuPercent { get; init; }
    public long? HostMemoryUsedBytes { get; init; }
    public long? HostMemoryTotalBytes { get; init; }
    public long? NetworkReceivedBytes { get; init; }
    public long? NetworkSentBytes { get; init; }
    public double? NetworkReceiveBytesPerSecond { get; init; }
    public double? NetworkSendBytesPerSecond { get; init; }
    public long DatabaseAttempts { get; init; }
    public long DatabaseCompleted { get; init; }
    public long DatabaseFailures { get; init; }
    public double DatabaseTotalMilliseconds { get; init; }
    public double DatabaseMaxMilliseconds { get; init; }
}
