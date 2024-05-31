using CommunityToolkit.Mvvm.Messaging.Messages;

namespace Parquet.Floor.Messages {
    public class FileOpenMessage : ValueChangedMessage<string> {
        public FileOpenMessage(string value) : base(value) {
        }
    }
}
