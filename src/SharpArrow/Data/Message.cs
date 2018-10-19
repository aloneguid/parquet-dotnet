using System;
using System.Collections.Generic;
using System.Text;
using FlatBuffers;
using FB = org.apache.arrow.flatbuf;

namespace SharpArrow.Data
{
   public abstract class Message
   {
      protected Message(long bodyLength, FB.MessageHeader messageHeader, FB.MetadataVersion metadataVersion)
      {

      }

      /// <summary>
      /// Creates a message implementation wrapper based on message type.
      /// </summary>
      /// <param name="messageData"></param>
      /// <returns></returns>
      internal static Message CreateFromData(byte[] messageData)
      {
         FB.Message message = FB.Message.GetRootAsMessage(new ByteBuffer(messageData));

         switch (message.HeaderType)
         {
            case FB.MessageHeader.RecordBatch:
               return new RecordBatch(message);
            default:
               throw new NotSupportedException($"header type {message.HeaderType} is not supported");
         }
      }
   }
}
