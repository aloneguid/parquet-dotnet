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

      //singleton
      private static AppSettings instance;
      public static AppSettings Instance => instance ?? (instance = new AppSettings());

      protected override void OnConfigure(IConfigConfiguration configuration)
      {
         configuration.UseCommandLineArgs();
      }
   }
}
