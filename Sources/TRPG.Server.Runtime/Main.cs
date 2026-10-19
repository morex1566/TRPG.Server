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

        // 운영 서버도 개발 서버와 같은 채팅방 로직을 실행한다.
        await ChatServer.RunAsync(shutdownCancellation.Token);
    }
}
