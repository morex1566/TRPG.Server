using System.Diagnostics;
using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TRPG.Test;

internal static class DashboardHost
{
    private sealed record StartRequest(int Clients, int Seconds);

    public static async Task<int> RunAsync(TestOptions options, CancellationToken cancellation)
    {
        await using var monitor = new ServerMonitor(options);
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [], ContentRootPath = AppContext.BaseDirectory });
        builder.Logging.ClearProviders();
        // 동적 포트를 PC 루프백에만 바인딩한다. VM 방화벽 설정은 추가하지 않는다.
        builder.WebHost.ConfigureKestrel(server => server.Listen(IPAddress.Loopback, 0));
        await using WebApplication app = builder.Build();
        object sync = new();
        bool running = false;
        string phase = "대기";
        string? result = null;
        string runId = "";
        string address = "";
        int selectedClients = options.Clients;
        int selectedSeconds = options.Seconds;
        CancellationTokenSource? runCancellation = null;
        Task activeRun = Task.CompletedTask;

        using Stream resource = typeof(DashboardHost).Assembly.GetManifestResourceStream("TRPG.Test.dashboard.html")
            ?? throw new InvalidOperationException("HTML 대시보드 리소스가 없습니다.");
        using var reader = new StreamReader(resource);
        string html = await reader.ReadToEndAsync(cancellation);

        app.Use(async (context, next) =>
        {
            // 로컬 주소로 우회하는 외부 페이지의 부하 시작 요청도 거절한다.
            if (context.Request.Host.Host != "127.0.0.1" ||
                (HttpMethods.IsPost(context.Request.Method) == true && context.Request.Headers["Origin"].ToString() != address))
            {
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                return;
            }
            context.Response.Headers["Cache-Control"] = "no-store";
            context.Response.Headers["X-Content-Type-Options"] = "nosniff";
            await next(context);
        });
        app.MapGet("/", () => Results.Content(html, "text/html; charset=utf-8"));
        app.MapGet("/api/state", () =>
        {
            lock (sync)
            {
                return Results.Json(new
                {
                    target = $"{options.Host}:{options.Port}",
                    intervalMilliseconds = 500,
                    mode = options.RandomWorkload == true ? "random" : "fixed",
                    fixedRate = options.Rate,
                    server = monitor.Capture(),
                    test = new { running, phase, result, runId, selectedClients, selectedSeconds, metrics = Program.CurrentMetrics?.CaptureLive() }
                });
            }
        });
        app.MapPost("/api/test/start", (StartRequest request) =>
        {
            TestOptions selected = options with { Clients = request.Clients, Seconds = request.Seconds };
            try { selected.Validate(); }
            catch (ArgumentException error) { return Results.BadRequest(new { error = error.Message }); }
            lock (sync)
            {
                if (running == true) return Results.Conflict(new { error = "이미 부하 테스트가 실행 중입니다." });
                running = true;
                phase = "서버 연결 확인";
                result = null;
                runId = Guid.NewGuid().ToString("N");
                selectedClients = selected.Clients;
                selectedSeconds = selected.Seconds;
                Program.SetMetrics(null);
                var testCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellation, app.Lifetime.ApplicationStopping);
                runCancellation = testCancellation;
                activeRun = Task.Run(async () =>
                {
                    try
                    {
                        if (await monitor.WaitForReadyAsync(testCancellation.Token) == false)
                            throw new IOException("서버의 최신 실행 지표가 없습니다. 서버 실행·재배포·SSH 연결을 확인하세요.");
                        ServerMonitorState before = monitor.Capture();
                        lock (sync) phase = "실행 중";
                        int code = await Program.RunAsync(selected, testCancellation.Token);
                        ServerMonitorState after = monitor.Capture();
                        bool passed = code == 0 && after.Fresh == true && before.RestartCount == after.RestartCount &&
                            after.Latest?.SessionId == before.Latest?.SessionId &&
                            after.Latest?.DatabaseFailures == before.Latest?.DatabaseFailures;
                        monitor.PrintResult();
                        lock (sync)
                        {
                            phase = testCancellation.IsCancellationRequested == true ? "중단" : passed == true ? "완료" : "오류 관측";
                            result = passed == true ? "관측 오류 없이 측정 완료. 성능 목표 달성을 보장하는 판정은 아닙니다." :
                                "클라이언트 오류, DB 오류, 서버 재시작 또는 불완전한 지표를 확인하세요.";
                        }
                    }
                    catch (OperationCanceledException) when (testCancellation.IsCancellationRequested == true)
                    {
                        lock (sync) { phase = "중단"; result = "부하를 중단했습니다. 감시는 계속됩니다."; }
                    }
                    catch (Exception error)
                    {
                        lock (sync) { phase = "오류"; result = error.Message; }
                    }
                    finally
                    {
                        lock (sync)
                        {
                            running = false;
                            runCancellation = null;
                            testCancellation.Dispose();
                        }
                    }
                });
                return Results.Json(new { message = "부하 테스트를 시작합니다." }, statusCode: StatusCodes.Status202Accepted);
            }
        });
        app.MapPost("/api/test/stop", () =>
        {
            lock (sync) runCancellation?.Cancel();
            return Results.Ok(new { message = "부하 중단을 요청했습니다. 채팅 서버는 종료하지 않습니다." });
        });

        await app.StartAsync(cancellation);
        address = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.Single().TrimEnd('/');
        Console.WriteLine($"HTML 대시보드: {address}");
        Console.WriteLine("브라우저에서 인원과 시간을 정해 시작하세요. 감시는 0.5초 목표 간격입니다. 종료는 Ctrl+C입니다.");
        try { Process.Start(new ProcessStartInfo(address) { UseShellExecute = true }); }
        catch (Exception error) when (error is System.ComponentModel.Win32Exception or InvalidOperationException)
        {
            Console.WriteLine("브라우저를 자동으로 열지 못했습니다. 위 주소를 직접 여세요.");
        }
        try { await app.WaitForShutdownAsync(cancellation); }
        finally
        {
            lock (sync) runCancellation?.Cancel();
            await activeRun;
            await app.StopAsync(CancellationToken.None);
        }
        return 0;
    }
}
