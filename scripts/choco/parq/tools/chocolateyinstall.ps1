Set-Location C:\ProgramData\Chocolatey\lib\parq\tools
Add-Type -assembly "System.IO.Compression.FileSystem"
    
[System.IO.Compression.ZipFile]::ExtractToDirectory([System.IO.Path]::Combine((Get-Location).Path, "parqInstall.zip"), $pwd)

Set-Content -Path %ChocolateyInstall%\bin\parq.cmd -Value "%ChocolateyInstall%\lib\parq\tools\parq.exe" -Encoding Ascii