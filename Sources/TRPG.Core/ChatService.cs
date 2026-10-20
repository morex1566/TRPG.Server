using System.Text;
using TRPG.Networking;
using TRPG.Protocol;

namespace TRPG.Core
{
    public static class ChatService
    {
        public static void NotifyLeft(Connection sender, TcpServer server)
        {
            PacketContext notice = PacketContext.Create(PayloadType.ChatMessageNotify, builder =>
            {
                string message = sender.ChatIdleTimedOut == true
                    ? $"ㅇㅇ({sender.RemoteIp})님이 30분 동안 채팅하지 않아 퇴장했습니다."
                    : $"ㅇㅇ({sender.RemoteIp})님이 퇴장했습니다.";
                var text = builder.CreateString(message);
                // 입장 공지와 같은 서버 공지 형식이므로 이탤릭체와 시간이 적용된다.
                return ChatMessageNotify.CreateChatMessageNotify(builder, ChatType.ToClient, 0, 0,
                    text, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()).Value;
            });
            server.Broadcast(notice.Buffer);
        }

        public static void NotifyJoined(Connection sender, TcpServer server)
        {
            PacketContext notice = PacketContext.Create(PayloadType.ChatMessageNotify, builder =>
            {
                var text = builder.CreateString($"ㅇㅇ({sender.RemoteIp})님이 입장했습니다.");
                // 발신자 0은 클라이언트가 아닌 서버 공지를 나타낸다.
                return ChatMessageNotify.CreateChatMessageNotify(builder, ChatType.ToClient, 0, 0,
                    text, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()).Value;
            });
            sender.Send(notice.Buffer);
            server.BroadcastExcept(sender, notice.Buffer);
        }

        public static async Task BroadcastAsync(PacketContext context, TcpServer server, string sessionId, CancellationToken cancellation)
        {
            Connection? sender = context.Owner;
            if (sender is null) return;

            ChatMessageRequest? request = context.PayloadAs<ChatMessageRequest>();
            if (request.HasValue == false) return;

            string message = request.Value.Message;
            // 서버에서 본문 크기를 제한해 과도한 중계를 막는다.
            if (string.IsNullOrWhiteSpace(message) == true || Encoding.UTF8.GetByteCount(message) > 2048) return;

            // 비어 있지 않고 크기 제한을 통과한 채팅만 활동으로 인정한다.
            sender.MarkChatActivity();

            // 저장 기록과 실시간 메시지에 같은 전송 시각을 사용한다.
            long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            // DB 저장에 성공한 메시지만 다른 사용자에게 중계한다.
            await ChatDatabase.SaveMessageAsync(sessionId, sender.Guid, sender.RemoteIp, message, timestamp, cancellation);

            PacketContext notification = PacketContext.Create(PayloadType.ChatMessageNotify, builder =>
            {
                var messageOffset = builder.CreateString(message);
                var ipOffset = builder.CreateString(sender.RemoteIp);
                return ChatMessageNotify.CreateChatMessageNotify(
                    builder,
                    ChatType.ToClient,
                    sender.Guid,
                    0,
                    messageOffset,
                    timestamp,
                    ipOffset).Value;
            });

            // 발신자 화면에는 이미 메시지가 있으므로 다른 접속자에게만 전달한다.
            server.BroadcastExcept(sender, notification.Buffer);
        }
    }
}
