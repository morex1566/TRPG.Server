using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace TRPG.Core;

public static class ChatDatabase
{
    // Debug 서버는 환경 변수나 원격 DB를 사용하지 않는다.
    public static bool Enabled
    {
        get
        {
#if DEBUG
            return false;
#else
            return true;
#endif
        }
    }

    public static async Task CheckConnectionAsync(CancellationToken cancellation)
    {
        if (Enabled == false)
        {
            Console.WriteLine("Debug 서버: DB 연결 및 저장을 사용하지 않습니다.");
            return;
        }
        using OracleConnection connection = CreateConnection();
        await connection.OpenAsync(cancellation);

        // 데이터를 변경하지 않고 테이블 접근을 확인합니다.
        using OracleCommand command = connection.CreateCommand();
        command.CommandText = "SELECT MESSAGE_ID FROM CHAT_MESSAGES WHERE 1 = 0";
        command.CommandTimeout = 15;

        using var reader = await command.ExecuteReaderAsync(cancellation);

        Console.WriteLine("DB 연결 및 CHAT_MESSAGES 테이블 접근 성공");
    }

    private static OracleConnection CreateConnection()
    {
        // 연결 확인과 채팅 저장에서 같은 접속 설정을 사용합니다.
        OracleConnectionStringBuilder settings = new OracleConnectionStringBuilder
        {
            UserID = GetRequiredEnvironmentVariable("TRPG_DB_USER"),
            Password = GetRequiredEnvironmentVariable("TRPG_DB_PASSWORD"),
            DataSource = GetRequiredEnvironmentVariable("TRPG_DB_SOURCE"),
            Pooling = true,
            MinPoolSize = 0,
            MaxPoolSize = 10,
            ConnectionTimeout = 10
        };

        return new OracleConnection(settings.ConnectionString);
    }

    private static string GetRequiredEnvironmentVariable(string name)
    {
        string? value = Environment.GetEnvironmentVariable(name);

        if (string.IsNullOrWhiteSpace(value) == true)
        {
            throw new InvalidOperationException($"환경 변수 {name}이 설정되지 않았습니다.");
        }

        return value;
    }

    public static async Task SaveMessageAsync(string sessionId, ulong senderId, string senderIp, string message, long timestamp, CancellationToken cancellation)
    {
        if (Enabled == false) return;
        using var measurement = ServerPerformance.MeasureDatabaseWrite();
        using OracleConnection connection = CreateConnection();
        await connection.OpenAsync(cancellation);

        using OracleCommand command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandTimeout = 15;
        command.CommandText = """
        INSERT INTO CHAT_MESSAGES
            (SESSION_ID, SENDER_ID, SENDER_IP, MESSAGE_BODY, SENT_AT)
        VALUES
            (:session_id, :sender_id, :sender_ip, :message_body, :sent_at)
        """;

        // 채팅 내용은 SQL 문자열에 합치지 않고 매개변수로 전달합니다.
        command.Parameters.Add("session_id", OracleDbType.Varchar2, 32).Value = sessionId;
        command.Parameters.Add("sender_id", OracleDbType.Decimal).Value = (decimal)senderId;
        command.Parameters.Add("sender_ip", OracleDbType.Varchar2, 45).Value = senderIp;
        command.Parameters.Add("message_body", OracleDbType.Varchar2, 2048).Value = message;

        // 실시간 채팅에 사용한 시각을 UTC로 저장합니다.
        DateTime utcTime = DateTimeOffset.FromUnixTimeMilliseconds(timestamp).UtcDateTime;
        command.Parameters.Add("sent_at", OracleDbType.TimeStampTZ).Value =
            new OracleTimeStampTZ(utcTime, "+00:00");

        await command.ExecuteNonQueryAsync(cancellation);
        measurement.Succeed();
    }

    public static async Task ResetMessagesAsync(CancellationToken cancellation)
    {
        if (Enabled == false) return;
        using OracleConnection connection = CreateConnection();
        await connection.OpenAsync(cancellation);
        using OracleCommand command = connection.CreateCommand();
        // 계정이나 테이블은 유지하고 채팅 기록만 전체 초기화한다.
        command.CommandText = "DELETE FROM CHAT_MESSAGES";
        command.CommandTimeout = 15;
        int deleted = await command.ExecuteNonQueryAsync(cancellation);
        Console.WriteLine($"서버 종료: DB 채팅 기록 {deleted}건 초기화 완료");
    }
}
