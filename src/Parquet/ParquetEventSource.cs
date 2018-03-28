using System;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Text;

namespace Parquet
{
   [EventSource(Name = "Parquet.Net")]
   internal sealed class ParquetEventSource : EventSource
   {
      public static readonly ParquetEventSource Current = new ParquetEventSource();

      // Instance constructor is private to enforce singleton semantics
      private ParquetEventSource() : base() { }

      // Event keywords can be used to categorize events. 
      // Each keyword is a bit flag. A single event can be associated with multiple keywords (via EventAttribute.Keywords property).
      // Keywords must be defined as a public class named 'Keywords' inside EventSource that uses them.
      public static class Keywords
      {
         public const EventKeywords Read = (EventKeywords)0x1L;
         public const EventKeywords Write = (EventKeywords)0x2L;
      }

      [Event(1, Level = EventLevel.Informational, Message = "reading field {0}")]
      public void ReadField(string path)
      {
         if (!IsEnabled()) return;

         WriteEvent(1, path);
      }
   }
}
