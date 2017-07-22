# Build
Set-Location ..\..\src\parq
dotnet restore -r win10-x64
dotnet publish -c release -r win10-x64 -o ..\..\scripts\choco\build -f netcoreapp1.1

# Get Version
Set-Location ..\..\scripts\choco\build
$version = .\parq.exe ShowVersion=true

Write-Host The Version of Parq built is $version

# Zip
Add-Type -assembly "System.IO.Compression.FileSystem"
[System.IO.Compression.ZipFile]::CreateFromDirectory((Get-Location).Path, [System.IO.Path]::Combine((Get-Location).Path, "..\parq\tools\parqInstall.zip"))

# Package
Set-Location ..\parq
# Update Version Number
$xml = [xml](Get-Content .\parq.nuspec)
$nsmgr = new-object System.Xml.XmlNamespaceManager($xml.NameTable);
$nsmgr.AddNamespace("nuspec", "http://schemas.microsoft.com/packaging/2015/06/nuspec.xsd")

$xml.SelectNodes("//nuspec:version", $nsmgr) | % { 
    $_."#text" =  $version.ToString()
    }

$xml.Save((Get-Location).Path + "\parq.nuspec")
Write-Host Updated Nuspec with version number and will now begin to package.
choco pack
choco push .\parq.$version.nupkg -k $env:elastacloudKey

# Clean
Set-Location ..
Remove-Item build -Recurse
Remove-Item .\parq\tools\parqInstall.zip