using System.Globalization;
using System.Net;

namespace TRPG.Test;

internal sealed record TestOptions(string Host, int Port, int Clients, int Seconds, double Rate, int Characters, int RampMs, int DrainSeconds, bool RandomWorkload)
{
    public const int MaxMessages = 100_000;
    public string SshKey { get; init; } = @"D:\Docs\us2d-server-main\ssh-key-2026-04-01.key";
    public string ServerMetricsFile { get; init; } = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory,
        "../../../../TRPG.Server/bin/Debug/net9.0/server-metrics.json"));

    // 테스트 도구는 Windows에서 실행하되 빌드 구성에 따라 기본 서버를 선택한다.
#if DEBUG
    private const string DefaultHost = "127.0.0.1";
#else
    private const string DefaultHost = "168.107.37.255";
#endif

    public static TestOptions Parse(string[] args)
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        string[] known = ["--host", "--port", "--clients", "--seconds", "--rate", "--chars", "--ramp-ms", "--drain-seconds", "--mode", "--ssh-key", "--server-metrics-file"];
        for (int i = 0; i < args.Length; i += 2)
        {
            if (known.Contains(args[i]) == false || i + 1 >= args.Length || values.TryAdd(args[i], args[i + 1]) == false)
                throw new ArgumentException($"인자를 확인하세요: {args[i]}");
        }

        string Get(string name, string fallback) => values.GetValueOrDefault(name, fallback);
        int Number(string name, int fallback) => int.Parse(Get(name, fallback.ToString(CultureInfo.InvariantCulture)), CultureInfo.InvariantCulture);
        string host = Get("--host", DefaultHost);
        if (string.IsNullOrWhiteSpace(host) == true) throw new ArgumentException("서버 주소가 비어 있습니다.");
        string mode = Get("--mode", "random");
        if (mode != "random" && mode != "fixed") throw new ArgumentException("--mode는 random 또는 fixed입니다.");
        bool randomWorkload = mode == "random";
        if (randomWorkload == true && (values.ContainsKey("--rate") == true || values.ContainsKey("--chars") == true))
            throw new ArgumentException("--rate와 --chars는 --mode fixed에서만 사용합니다.");
        var options = new TestOptions(host, Number("--port", 60000), Number("--clients", randomWorkload ? 999 : 10),
            Number("--seconds", 30), double.Parse(Get("--rate", "1"), CultureInfo.InvariantCulture),
            Number("--chars", 20), Number("--ramp-ms", 50), Number("--drain-seconds", 5), randomWorkload);
        options = options with
        {
            SshKey = Get("--ssh-key", options.SshKey),
            ServerMetricsFile = Get("--server-metrics-file", options.ServerMetricsFile)
        };

        options.Validate();
        return options;
    }

    public void Validate()
    {
        if (Port is < 1 or > 65535 || Clients is < 1 or > 999 || Seconds is < 1 or > 3600 ||
            double.IsFinite(Rate) == false || Rate is < 0 or > 10000 || Characters is < 1 or > 600 ||
            RampMs is < 0 or > 10000 || DrainSeconds is < 1 or > 60 ||
            Math.Ceiling((RandomWorkload == true ? Clients * 2.0 : Rate) * Seconds) > MaxMessages)
            throw new ArgumentException("옵션 범위를 벗어났습니다. 인원 1~999, 시간 1~3600초, 추적 한도 100,000건을 확인하세요.");
    }

    public bool IsLoopback => Host.Equals("localhost", StringComparison.OrdinalIgnoreCase) == true ||
        (IPAddress.TryParse(Host, out IPAddress? address) == true && IPAddress.IsLoopback(address) == true);

    public static void PrintHelp()
    {
        Console.WriteLine($"""
            TRPG.Test — 가상 클라이언트 접속 및 채팅 전달 테스트
            서버는 별도로 실행해야 합니다. 현재 구성의 기본 대상: {DefaultHost}:60000

            --host            서버 주소                      기본 {DefaultHost}
            --port            포트                           기본 60000
            --mode            random / fixed                 기본 random
            --clients         HTML의 초기 인원 (1~999)       브라우저에서 변경 가능
            --seconds         부하 유지 시간 (1~3600초)       기본 30
            --rate            fixed 전용 방 전체 메시지/초    기본 1, 0이면 접속 유지
            --chars           fixed 전용 한글 수 (1~600)      기본 20 + 식별자
            --ramp-ms         연결 시도 간격 (0~10000ms)      기본 50
            --drain-seconds   종료 전 수신 대기 (1~60초)      기본 5
            --ssh-key         VM 지표 수집에 사용할 SSH 개인 키 경로
            --server-metrics-file 로컬 서버의 지표 JSON 경로 (기본 Debug 서버 출력 폴더)

            실행 시 로컬 HTML 대시보드를 엽니다. 부하는 브라우저의 시작 버튼을 눌러야 시작됩니다.
            서버 상태와 그래프는 0.5초 목표 간격으로 갱신하며 부하 종료 뒤에도 감시를 유지합니다.

            핸드셰이크/송신 제한 시간은 5초입니다.
            random: 각 접속자가 500~1000ms마다 8~16B ASCII 본문을 보냅니다(식별자 포함).
            random: 10~30초 접속 후 1~5초 쉬고 재접속합니다. 초기 접속 시도 후 부하 시간을 계산합니다.
            추적 한도는 fixed rate × seconds, random clients × 2 × seconds가 100,000 이하입니다.
            random은 종료 시 고정 구성으로 검증 메시지 3개를 보내 누락/중복률을 별도 측정합니다.
            1명 또는 검증 구간 수신자가 없으면 전달 지연/누락률은 측정 불가입니다.
            접속 실패, 예기치 않은 연결 종료, 송신 오류이면 실패입니다. fixed는 누락/중복도 검사합니다.
            인자 오류는 종료 코드 2, 정상 완료는 0입니다. Ctrl+C로 중단할 수 있습니다.
            """);
    }
}
