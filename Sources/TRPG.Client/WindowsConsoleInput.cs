using System.ComponentModel;
using System.Runtime.InteropServices;

namespace TRPG;

/// <summary>
/// 한글 키 입력과 마우스 이벤트를 같은 콘솔 입력 큐에서 읽는다.
/// </summary>
internal sealed class WindowsConsoleInput : IDisposable
{
    private readonly IntPtr input = GetStdHandle(-10);
    private readonly IntPtr output = GetStdHandle(-11);
    private readonly uint inputMode;
    private readonly uint outputMode;

    public WindowsConsoleInput()
    {
        if (GetConsoleMode(input, out inputMode) == false || GetConsoleMode(output, out outputMode) == false)
            throw new Win32Exception(Marshal.GetLastWin32Error());

        // Quick Edit와 VT 키 입력을 끄고 마우스 입력 및 VT 출력을 활성화한다.
        if (SetConsoleMode(input, (inputMode | 0x0080u | 0x0010u) & ~(0x0040u | 0x0200u)) == false)
            throw new Win32Exception(Marshal.GetLastWin32Error());
        if (SetConsoleMode(output, outputMode | 0x0004u) == false)
        {
            SetConsoleMode(input, inputMode);
            throw new Win32Exception(Marshal.GetLastWin32Error());
        }
    }

    public bool Poll(Action<ConsoleKeyInfo> onKey, Action<int, int, int, bool> onMouse)
    {
        if (GetNumberOfConsoleInputEvents(input, out uint count) == false)
            throw new Win32Exception(Marshal.GetLastWin32Error());
        if (count == 0) return false;
        if (ReadConsoleInputW(input, out InputRecord record, 1, out uint read) == false)
            throw new Win32Exception(Marshal.GetLastWin32Error());
        if (read == 0) return false;

        if (record.Type == 1 && record.KeyDown != 0)
        {
            var key = new ConsoleKeyInfo((char)record.Character, (ConsoleKey)record.VirtualKey,
                (record.ControlState & 0x0010) != 0, (record.ControlState & 0x0003) != 0,
                (record.ControlState & 0x000c) != 0);
            for (int i = 0; i < record.Repeat; i++) onKey(key);
        }
        else if (record.Type == 2)
        {
            int wheel = (record.MouseFlags & 0x0004) != 0 ? (short)(record.Buttons >> 16) / 120 : 0;
            onMouse(record.X - Console.WindowLeft, record.Y - Console.WindowTop, wheel, (record.Buttons & 1) != 0);
        }
        return true;
    }

    public void Dispose()
    {
        Console.Write("\u001b[23m");
        SetConsoleMode(input, inputMode);
        SetConsoleMode(output, outputMode);
    }

    // Win32 INPUT_RECORD의 키보드와 마우스 공용체 레이아웃이다.
    [StructLayout(LayoutKind.Explicit, Size = 20)]
    private struct InputRecord
    {
        [FieldOffset(0)] public ushort Type;
        [FieldOffset(4)] public int KeyDown;
        [FieldOffset(8)] public ushort Repeat;
        [FieldOffset(10)] public ushort VirtualKey;
        [FieldOffset(14)] public ushort Character;
        [FieldOffset(16)] public uint ControlState;
        [FieldOffset(4)] public short X;
        [FieldOffset(6)] public short Y;
        [FieldOffset(8)] public uint Buttons;
        [FieldOffset(16)] public uint MouseFlags;
    }

    [DllImport("kernel32.dll")] private static extern IntPtr GetStdHandle(int handle);
    [DllImport("kernel32.dll", SetLastError = true)] private static extern bool GetConsoleMode(IntPtr handle, out uint mode);
    [DllImport("kernel32.dll", SetLastError = true)] private static extern bool SetConsoleMode(IntPtr handle, uint mode);
    [DllImport("kernel32.dll", SetLastError = true)] private static extern bool GetNumberOfConsoleInputEvents(IntPtr handle, out uint count);
    [DllImport("kernel32.dll", SetLastError = true)] private static extern bool ReadConsoleInputW(IntPtr handle, out InputRecord record, uint length, out uint read);
}
