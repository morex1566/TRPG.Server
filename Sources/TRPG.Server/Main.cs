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

        // 서버 실행 전에 DB 연결 확인
        await ChatDatabase.CheckConnectionAsync(shutdownCancellation.Token);

        // 채팅방 서버를 실행
        await ChatServer.RunAsync(shutdownCancellation.Token);
    }
}
