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

      //singleton
      private static AppSettings instance;
      public static AppSettings Instance => instance ?? (instance = new AppSettings());

      protected override void OnConfigure(IConfigConfiguration configuration)
      {
         configuration.UseCommandLineArgs();
      }
   }
}
