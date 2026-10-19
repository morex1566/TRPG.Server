using System.Text;

namespace TRPG;

public sealed class TerminalApp
{
    private const int MessageColumn = 27;
    private readonly List<(string Sender, string Message, bool IsOwn)> messages = new();
    private readonly StringBuilder input = new();
    private string ownIp = "-";
    private int screenWidth;
    private int screenHeight;
    private int scrollOffset;

    public event Func<string, int, Task>? ConnectRequested;
    public event Action<string>? ChatRequested;

    public void Run(Func<bool> tick)
    {
        Console.OutputEncoding = Encoding.UTF8;
        Console.InputEncoding = Encoding.UTF8;
        Console.CursorVisible = true;
        DrawScreen();

        // 클라이언트를 시작하면 지정된 서버에 자동 접속한다.
        string host = Environment.GetEnvironmentVariable("TRPG_SERVER_HOST") ?? "127.0.0.1";
        string? portValue = Environment.GetEnvironmentVariable("TRPG_SERVER_PORT");
        int port = int.TryParse(portValue, out int configuredPort) == true ? configuredPort : 60000;
        _ = ConnectRequested?.Invoke(host, port);

        while (tick() == true)
        {
            if (screenWidth != Console.WindowWidth || screenHeight != Console.WindowHeight) DrawScreen();
            if (Console.KeyAvailable == true) HandleKey(Console.ReadKey(intercept: true));
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

    public void AppendChat(string sender, string message)
    {
        AppendChat(sender, message, false);
    }

    private void AppendChat(string sender, string message, bool isOwn)
    {
        string safeSender = sender.Replace('\r', ' ').Replace('\n', ' ');
        string safeMessage = message.Replace('\r', ' ').Replace('\n', ' ');
        messages.Add((safeSender, safeMessage, isOwn));
        // 이전 채팅을 읽는 중이면 새 메시지가 와도 같은 위치를 유지한다.
        if (scrollOffset > 0) scrollOffset++;
        DrawMessages();
    }

    public void AppendOwnChat(string message)
    {
        AppendChat($"ㅇㅇ({ownIp})", message, true);
    }

    public void SetOwnIp(string ip)
    {
        ownIp = ip;
    }

    private void HandleKey(ConsoleKeyInfo key)
    {
        int pageSize = Math.Max(1, screenHeight - 3);
        int maxOffset = Math.Max(0, messages.Count - pageSize);
        if (key.Key == ConsoleKey.PageUp || key.Key == ConsoleKey.UpArrow || key.Key == ConsoleKey.Home)
        {
            int amount = key.Key == ConsoleKey.UpArrow ? 1 : pageSize;
            scrollOffset = key.Key == ConsoleKey.Home ? maxOffset : Math.Min(maxOffset, scrollOffset + amount);
            DrawMessages();
            return;
        }
        if (key.Key == ConsoleKey.PageDown || key.Key == ConsoleKey.DownArrow || key.Key == ConsoleKey.End)
        {
            int amount = key.Key == ConsoleKey.DownArrow ? 1 : pageSize;
            scrollOffset = key.Key == ConsoleKey.End ? 0 : Math.Max(0, scrollOffset - amount);
            DrawMessages();
            return;
        }
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
        if (screenWidth < 4 || screenHeight < 4) return;

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
        if (screenWidth < 4 || screenHeight < 4) return;
        int rows = screenHeight - 3;
        scrollOffset = Math.Min(scrollOffset, Math.Max(0, messages.Count - rows));
        int first = Math.Max(0, messages.Count - rows - scrollOffset);
        for (int row = 0; row < rows; row++)
        {
            Console.SetCursorPosition(1, row + 1);
            Console.ForegroundColor = ConsoleColor.White;
            Console.Write(new string(' ', screenWidth - 2));
            int index = first + row;
            if (index >= messages.Count) continue;
            Console.SetCursorPosition(1, row + 1);
            Console.ForegroundColor = messages[index].IsOwn ? ConsoleColor.DarkYellow : ConsoleColor.White;
            // 발신자 길이와 한글 폭에 관계없이 구분자를 같은 열에 표시한다.
            int separatorColumn = Math.Min(MessageColumn, screenWidth - 3);
            Console.Write(FitToWidth(messages[index].Sender, separatorColumn - 2));
            Console.SetCursorPosition(separatorColumn, row + 1);
            Console.Write(": ");
            Console.Write(FitToWidth(messages[index].Message, screenWidth - separatorColumn - 3));
        }
        Console.ForegroundColor = ConsoleColor.White;
        DrawInput();
    }

    private void DrawInput()
    {
        if (screenWidth < 4 || screenHeight < 4) return;
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
