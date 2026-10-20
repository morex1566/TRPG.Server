using System.Text;

namespace TRPG;

public sealed class TerminalApp
{
    private const int MessageColumn = 27;
    private readonly List<(string Sender, string Message, bool IsOwn, bool IsNotice, DateTimeOffset Time)> messages = new();
    private readonly List<(string Sender, string Text, bool IsOwn, bool IsNotice)> displayRows = new();
    private readonly StringBuilder input = new();
    private string ownIp = "-";
    private int screenWidth;
    private int screenHeight;
    private int scrollOffset;
    private bool draggingScroll;
    private int dragOffset;

    public event Func<string, int, Task>? ConnectRequested;
    public event Action<string>? ChatRequested;

    public void Run(Func<bool> tick)
    {
        Console.OutputEncoding = Encoding.UTF8;
        Console.InputEncoding = Encoding.UTF8;
        using var consoleInput = new WindowsConsoleInput();
        Console.CursorVisible = true;
        DrawScreen();

        // 클라이언트를 시작하면 지정된 서버에 자동 접속한다.
        // 빌드 구성으로 기본 접속 대상을 선택하고 명시적인 환경 변수는 우선한다.
#if DEBUG
        const string defaultHost = "127.0.0.1";
#else
        const string defaultHost = "168.107.37.255";
#endif
        string host = Environment.GetEnvironmentVariable("TRPG_SERVER_HOST") ?? defaultHost;
        string? portValue = Environment.GetEnvironmentVariable("TRPG_SERVER_PORT");
        int port = int.TryParse(portValue, out int configuredPort) == true ? configuredPort : 60000;
        _ = ConnectRequested?.Invoke(host, port);

        while (tick() == true)
        {
            if (screenWidth != Console.WindowWidth || screenHeight != Console.WindowHeight) DrawScreen();
            if (consoleInput.Poll(HandleKey, HandleMouse) == true) continue;
            else Thread.Sleep(20);
        }
    }

    public void ClearMessage()
    {
        input.Clear();
        DrawInput();
    }

    public void AppendLog(string message)
    {
        System.Diagnostics.Debug.WriteLine(message);
    }

    public void AppendChat(string sender, string message, DateTimeOffset? time = null)
    {
        AppendChat(sender, message, false, sender == "공지", time ?? DateTimeOffset.Now);
    }

    private void AppendChat(string sender, string message, bool isOwn, bool isNotice, DateTimeOffset time)
    {
        // 사용자 메시지의 제어 문자가 화면 위치와 공지 서식을 바꾸지 못하게 한다.
        string safeSender = string.Concat(sender.Select(character => char.IsControl(character) == true ? ' ' : character));
        string safeMessage = string.Concat(message.Select(character => char.IsControl(character) == true ? ' ' : character));
        messages.Add((safeSender, safeMessage, isOwn, isNotice, time));
        int previousRows = displayRows.Count;
        RebuildRows();
        // 이전 채팅을 읽는 중이면 새 메시지가 와도 같은 위치를 유지한다.
        if (scrollOffset > 0) scrollOffset += displayRows.Count - previousRows;
        DrawMessages();
    }

    public void AppendOwnChat(string message)
    {
        AppendChat($"ㅇㅇ({ownIp})", message, true, false, DateTimeOffset.Now);
    }

    public void SetOwnIp(string ip)
    {
        ownIp = ip;
    }

    private void HandleKey(ConsoleKeyInfo key)
    {
        if (key.Key == ConsoleKey.Enter)
        {
            ChatRequested?.Invoke(input.ToString());
            return;
        }
        if (key.Key == ConsoleKey.Backspace)
        {
            if (input.Length > 0)
            {
                input.Remove(input.Length - 1, 1);
                DrawInput();
            }
            return;
        }
        if (char.IsControl(key.KeyChar) == false)
        {
            input.Append(key.KeyChar);
            DrawInput();
        }
    }

    private void DrawScreen()
    {
        screenWidth = Console.WindowWidth;
        screenHeight = Console.WindowHeight;
        if (screenWidth < 8 || screenHeight < 4) return;
        RebuildRows();

        Console.BackgroundColor = ConsoleColor.Black;
        Console.ForegroundColor = ConsoleColor.White;
        Console.Clear();
        Console.SetCursorPosition(0, 0);
        Console.Write('┌' + new string('─', screenWidth - 2) + '┐');
        for (int row = 1; row < screenHeight - 1; row++)
        {
            Console.SetCursorPosition(0, row);
            Console.Write('│');
            Console.SetCursorPosition(screenWidth - 1, row);
            Console.Write('│');
        }
        Console.SetCursorPosition(0, screenHeight - 1);
        Console.Write('└' + new string('─', screenWidth - 2) + '┘');
        DrawMessages();
    }

    private void DrawMessages()
    {
        if (screenWidth < 8 || screenHeight < 4) return;
        int rows = screenHeight - 3;
        scrollOffset = Math.Min(scrollOffset, Math.Max(0, displayRows.Count - rows));
        int first = Math.Max(0, displayRows.Count - rows - scrollOffset);
        for (int row = 0; row < rows; row++)
        {
            Console.SetCursorPosition(1, row + 1);
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(new string(' ', screenWidth - 2));
            int index = first + row;
            if (index >= displayRows.Count) continue;
            Console.SetCursorPosition(1, row + 1);
            Console.ForegroundColor = displayRows[index].IsOwn ? ConsoleColor.DarkYellow : ConsoleColor.White;
            // 발신자 길이와 한글 폭에 관계없이 구분자를 같은 열에 표시한다.
            int separatorColumn = Math.Min(MessageColumn, Math.Max(2, screenWidth - 25));
            if (displayRows[index].IsNotice == true) Console.Write("\u001b[3m");
            Console.Write(FitToWidth(displayRows[index].Sender, separatorColumn - 2));
            Console.SetCursorPosition(separatorColumn, row + 1);
            Console.Write(displayRows[index].Sender.Length > 0 ? ": " : "  ");
            Console.Write(displayRows[index].Text);
            if (displayRows[index].IsNotice == true) Console.Write("\u001b[23m");
        }
        Console.ForegroundColor = ConsoleColor.White;
        DrawScrollBar();
        DrawInput();
    }

    private void RebuildRows()
    {
        displayRows.Clear();
        int separatorColumn = Math.Min(MessageColumn, Math.Max(2, screenWidth - 25));
        int available = Math.Max(1, screenWidth - separatorColumn - 4);
        foreach (var message in messages)
        {
            string sender = message.Sender;
            string remaining = message.Message;
            string timestamp = message.Time.ToOffset(TimeSpan.FromHours(9)).ToString("(yyyy-MM-dd, HH:mm)");
            // 본문과 시간을 잘라 버리지 않고 다음 화면 줄로 이어서 표시한다.
            while (GetDisplayWidth(remaining) > available)
            {
                string part = FitToWidth(remaining, available);
                if (part.Length == 0) part = remaining[..1];
                displayRows.Add((sender, part, message.IsOwn, message.IsNotice));
                sender = "";
                remaining = remaining[part.Length..];
            }
            if (GetDisplayWidth(remaining) + timestamp.Length + 1 <= available)
            {
                displayRows.Add((sender, remaining + " " + timestamp, message.IsOwn, message.IsNotice));
            }
            else
            {
                if (remaining.Length > 0)
                {
                    displayRows.Add((sender, remaining, message.IsOwn, message.IsNotice));
                    sender = "";
                }
                while (timestamp.Length > available)
                {
                    displayRows.Add((sender, timestamp[..available], message.IsOwn, message.IsNotice));
                    sender = "";
                    timestamp = timestamp[available..];
                }
                displayRows.Add((sender, timestamp, message.IsOwn, message.IsNotice));
            }
        }
    }

    private (int Rows, int MaxOffset, int ThumbSize, int ThumbTop) GetScrollMetrics()
    {
        int rows = Math.Max(1, screenHeight - 3);
        int maxOffset = Math.Max(0, displayRows.Count - rows);
        int size = Math.Clamp(rows * rows / Math.Max(rows, displayRows.Count), 1, rows);
        int top = maxOffset == 0 ? 0 : (int)Math.Round((double)(maxOffset - scrollOffset) * (rows - size) / maxOffset);
        return (rows, maxOffset, size, top);
    }

    private void DrawScrollBar()
    {
        var metrics = GetScrollMetrics();
        for (int row = 0; row < metrics.Rows; row++)
        {
            Console.SetCursorPosition(screenWidth - 2, row + 1);
            Console.Write(row >= metrics.ThumbTop && row < metrics.ThumbTop + metrics.ThumbSize ? '█' : '│');
        }
    }

    private void HandleMouse(int x, int y, int wheel, bool leftDown)
    {
        if (screenWidth < 8 || screenHeight < 4) return;
        var metrics = GetScrollMetrics();
        if (wheel != 0)
        {
            scrollOffset = Math.Clamp(scrollOffset + wheel * 3, 0, metrics.MaxOffset);
            DrawMessages();
            return;
        }
        if (leftDown == false)
        {
            draggingScroll = false;
            return;
        }
        int row = Math.Clamp(y - 1, 0, metrics.Rows - 1);
        if (draggingScroll == false)
        {
            if (x != screenWidth - 2 || y < 1 || y > metrics.Rows) return;
            if (row < metrics.ThumbTop || row >= metrics.ThumbTop + metrics.ThumbSize)
            {
                scrollOffset = Math.Clamp(scrollOffset + (row < metrics.ThumbTop ? metrics.Rows : -metrics.Rows), 0, metrics.MaxOffset);
                DrawMessages();
                return;
            }
            draggingScroll = true;
            dragOffset = row - metrics.ThumbTop;
        }
        // 스크롤 손잡이의 위치를 채팅 목록 위치로 변환한다.
        int travel = metrics.Rows - metrics.ThumbSize;
        if (travel == 0) return;
        int top = Math.Clamp(row - dragOffset, 0, travel);
        scrollOffset = metrics.MaxOffset - (int)Math.Round((double)top * metrics.MaxOffset / travel);
        DrawMessages();
    }

    private void DrawInput()
    {
        if (screenWidth < 8 || screenHeight < 4) return;
        Console.SetCursorPosition(1, screenHeight - 2);
        Console.ForegroundColor = ConsoleColor.White;
        Console.Write(new string(' ', screenWidth - 2));
        Console.SetCursorPosition(1, screenHeight - 2);
        Console.Write("> ");
        // 입력이 화면보다 길어지면 끝부분과 입력 커서를 유지한다.
        Console.Write(FitFromEnd(input.ToString(), screenWidth - 4));
        Console.CursorVisible = true;
    }

    private static string FitToWidth(string value, int availableWidth)
    {
        var result = new StringBuilder();
        int width = 0;
        foreach (char character in value)
        {
            int characterWidth = GetDisplayWidth(character);
            if (width + characterWidth > availableWidth) break;
            result.Append(character);
            width += characterWidth;
        }
        return result.ToString();
    }

    private static string FitFromEnd(string value, int availableWidth)
    {
        int start = value.Length;
        int width = 0;
        while (start > 0)
        {
            int characterWidth = GetDisplayWidth(value[start - 1]);
            if (width + characterWidth > availableWidth) break;
            width += characterWidth;
            start--;
        }
        return value[start..];
    }

    private static int GetDisplayWidth(string value)
    {
        int width = 0;
        foreach (char character in value) width += GetDisplayWidth(character);
        return width;
    }

    private static int GetDisplayWidth(char character)
    {
        // 한글 음절과 자모는 콘솔에서 두 칸을 차지한다.
        return character is >= '\u1100' and <= '\u11FF'
            or >= '\u3130' and <= '\u318F'
            or >= '\uAC00' and <= '\uD7A3' ? 2 : 1;
    }
}
