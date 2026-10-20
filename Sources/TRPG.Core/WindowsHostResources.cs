using System.Runtime.InteropServices;

namespace TRPG.Core;

// 로컬 Debug 서버에서도 실제 PC의 메모리와 전체 CPU를 읽는다.
internal static class WindowsHostResources
{
    [StructLayout(LayoutKind.Sequential)]
    private struct FileTime
    {
        public uint Low;
        public uint High;
        public readonly long Ticks => ((long)High << 32) | Low;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct MemoryStatus
    {
        public uint Length;
        public uint Load;
        public ulong TotalPhysical;
        public ulong AvailablePhysical;
        public ulong TotalPage;
        public ulong AvailablePage;
        public ulong TotalVirtual;
        public ulong AvailableVirtual;
        public ulong AvailableExtended;
    }

    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetSystemTimes(out FileTime idle, out FileTime kernel, out FileTime user);

    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GlobalMemoryStatusEx(ref MemoryStatus status);

    public static (long Total, long Idle)? ReadCpu()
    {
        return GetSystemTimes(out FileTime idle, out FileTime kernel, out FileTime user) == true
            ? (kernel.Ticks + user.Ticks, idle.Ticks) : null;
    }

    public static (long Used, long Total)? ReadMemory()
    {
        var status = new MemoryStatus { Length = (uint)Marshal.SizeOf<MemoryStatus>() };
        return GlobalMemoryStatusEx(ref status) == true
            ? ((long)(status.TotalPhysical - status.AvailablePhysical), (long)status.TotalPhysical) : null;
    }
}
