//see https://github.com/MicrosoftDX/nether/blob/master/RunCodeFormatter.ps1

// https://github.com/MicrosoftDX/nether

$distribUri = "http://i.isolineltd.com/distrib/CodeFormatter.zip"
$downloadLocation = "$env:TEMP\code-formatter.zip"
$extractLocation = "$env:TEMP\code-formatter\"
$exePath = "$extractLocation\CodeFormatter\CodeFormatter.exe"

if(Test-Path $exePath)
{
   Write-Output "code formatter detected on path"
}
else
{
   (New-Object Net.WebClient).DownloadFile($distribUri, $downloadLocation)
   Expand-Archive -Path $downloadLocation -DestinationPath $extractLocation -Force
}