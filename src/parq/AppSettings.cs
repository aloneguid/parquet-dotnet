using Config.Net;
using System;
using System.Collections.Generic;
using System.Text;

namespace parq
{
   class AppSettings : SettingsContainer
   {
      public readonly Option<string> InputFilePath = new Option<string>();

      public readonly Option<int> DisplayMinWidth = new Option<int>(10);

      public readonly Option<string> Mode = new Option<string>("interactive");

      public readonly Option<bool> Expanded = new Option<bool>(false);

      public readonly Option<bool> DisplayNulls = new Option<bool>(true);

      public readonly Option<string> TruncationIdentifier = new Option<string>("*");

      public readonly Option<bool> ShowVersion = new Option<bool>(false);

      public readonly Option<int> Head = new Option<int>();
      public readonly Option<int> Tail = new Option<int>();


      //singleton
      private static AppSettings instance;
      public static AppSettings Instance => instance ?? (instance = new AppSettings());

      protected override void OnConfigure(IConfigConfiguration configuration)
      {
         configuration.UseCommandLineArgs();
      }
   }
}
