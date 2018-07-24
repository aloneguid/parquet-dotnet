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
      static class Keywords
      {
         public const EventKeywords Read = (EventKeywords)0x1L;
         public const EventKeywords Write = (EventKeywords)0x2L;
      }

      [Event(1, Level = EventLevel.Informational, Message = "length: {2}, rows: {3}")]
      public void OpenStream(long length, bool leaveOpen, int rowGroupCount, long numRows)
      {
         if (!IsEnabled()) return;

         WriteEvent(1, length, leaveOpen);
      }

      [Event(2, Level = EventLevel.Informational, Message = "path: {0}")]
      public void ReadColumn(string path)
      {
         if (!IsEnabled()) return;

         WriteEvent(2, path);
      }

      [Event(3, Level = EventLevel.Verbose, Message = "seeking to column {0} start @ {1}")]
      public void SeekColumn(string path, long offset)
      {
         if (!IsEnabled()) return;

         WriteEvent(3, path, offset);
      }

      [Event(4, Level = EventLevel.Verbose, Message = "opened data page")]
      public void OpenDataPage(string path, string compressionMethod, long length)
      {
         if (!IsEnabled()) return;

         WriteEvent(4, path, compressionMethod, length);
      }
   }
}
