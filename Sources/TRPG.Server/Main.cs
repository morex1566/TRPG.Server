using TRPG.Core;
using System.Runtime.InteropServices;

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

        // systemd 기본 종료 신호도 정상 종료 경로로 연결한다.
        using PosixSignalRegistration? terminate = OperatingSystem.IsLinux() == true
            ? PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
            {
                context.Cancel = true;
                shutdownCancellation.Cancel();
            }) : null;

        // 서버 실행 전에 DB 연결 확인
        await ChatDatabase.CheckConnectionAsync(shutdownCancellation.Token);

        // 채팅방 서버를 실행
        await ChatServer.RunAsync(shutdownCancellation.Token);
    }
}
