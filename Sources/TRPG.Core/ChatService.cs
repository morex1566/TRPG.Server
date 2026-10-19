using System.Text;
using TRPG.Networking;
using TRPG.Protocol;

namespace TRPG.Core
{
    public static class ChatService
    {
        public static void Broadcast(ref PacketContext context, TcpServer server)
        {
            Connection? sender = context.Owner;
            if (sender is null) return;

            ChatMessageRequest? request = context.PayloadAs<ChatMessageRequest>();
            if (request.HasValue == false) return;

            string message = request.Value.Message;
            // 서버에서 본문 크기를 제한해 과도한 중계를 막는다.
            if (string.IsNullOrWhiteSpace(message) == true || Encoding.UTF8.GetByteCount(message) > 2048) return;

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
                    DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ipOffset).Value;
            });

            // 발신자 화면에는 이미 메시지가 있으므로 다른 접속자에게만 전달한다.
            server.BroadcastExcept(sender, notification.Buffer);
        }
    }
}
