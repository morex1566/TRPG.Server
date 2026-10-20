using System;
using System.Diagnostics;

namespace TRPG.Common;

/// <summary>
/// 서버 전역 설정값 제공
/// </summary>
public static class SystemConfig
{
    /// <summary>
    /// 서버가 허용하는 최대 RAM 사용률
    /// </summary>
    public const double LimitRamPercentage = 92.5;

    /// <summary>
    /// tcp 설정값
    /// </summary>
    public static class Tcp
    {
        /// <summary>
        /// tcp tick rate
        /// </summary>
        private const float TickRate = 15f;

        /// <summary>
        /// tcp tick interval(ms)
        /// </summary>
        public const float TickIntervalMs = 1000f / TickRate;
    }

    /// <summary>
    /// connection 설정값
    /// </summary>
    public static class Connection
    {
        /// <summary>
        /// connection 단위 최대 buffer size
        /// </summary>
        public const int BufferSize = 64 * 1024;

        /// <summary>
        /// connection 단위 최대 queue size
        /// </summary>
        public const int QueueSize = 1024;

        /// <summary>
        /// read 타임아웃(ms). half-open 연결 방어용
        /// </summary>
        public const int ReadTimeoutMs = 30_000;

        /// <summary>
        /// 채팅을 보내지 않은 사용자의 자동 퇴장 시간(ms).
        /// </summary>
        public const int ChatIdleTimeoutMs = 30 * 60 * 1000;
    }

    /// <summary>
    /// tcp accept rate limit 설정값
    /// </summary>
    public static class TcpAcceptTokenBucket
    {
        /// <summary>
        /// accept token bucket capacity
        /// </summary>
        public const ulong Capacity = 100;

        /// <summary>
        /// accept token refill interval(ms)
        /// </summary>
        public const double RefillIntervalMs = 10.0;
    }

    /// <summary>
    /// 현재 RAM 사용률 반환
    /// </summary>
    public static double CurrentRamPercentage()
    {
        var process = Process.GetCurrentProcess();
        long workingSet = process.WorkingSet64;
        long totalMemory = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;

        return totalMemory > 0 ? (100.0 * workingSet / totalMemory) : 0.0;
    }
}
