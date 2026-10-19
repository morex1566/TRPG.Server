using TRPG.Core;

namespace TRPG;

static class Program
{
    static async Task Main()
    {
        using CancellationTokenSource shutdownCancellation = new CancellationTokenSource();
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            eventArgs.Cancel = true;
            shutdownCancellation.Cancel();
        };

        // 채팅방 서버를 실행하고 Ctrl+C를 누르면 종료한다.
        await ChatServer.RunAsync(shutdownCancellation.Token);
    }
}
