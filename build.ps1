### Build Script

param(
   [switch]
   $Publish,

   [string]
   $NuGetApiKey
)

$VersionPrefix = "1"
$VersionSuffix = "0.0.0"

$SlnPath = "src\Parquet.sln"
$AssemblyVersion = "$VersionPrefix.0.0.0"
$PackageVersion = "$VersionPrefix.$VersionSuffix"
Write-Host "version: $PackageVersion, assembly version: $AssemblyVersion"

function Set-VstsBuildNumber($BuildNumber)
{
   Write-Verbose -Verbose "##vso[build.updatebuildnumber]$BuildNumber"
}

function Update-ProjectVersion([string]$RelPath)
{
   $xml = [xml](Get-Content "$PSScriptRoot\$RelPath")

   if($xml.Project.PropertyGroup.Count -eq $null)
   {
      $pg = $xml.Project.PropertyGroup
   }
   else
   {
      $pg = $xml.Project.PropertyGroup[0]
   }

   $pg.Version = $PackageVersion
   $pg.FileVersion = $PackageVersion
   $pg.AssemblyVersion = $AssemblyVersion

   $xml.Save("$PSScriptRoot\$RelPath")
}

function Exec($Command)
{
   Invoke-Expression $Command
   if($LASTEXITCODE -ne 0)
   {
      Write-Error "command failed (error code: $LASTEXITCODE)"
      exit 1
   }
}

### Start the build

# General validation
if($Publish -and (-not $NuGetApiKey))
{
   Write-Error "Please specify nuget key to publish"
   exit 1
}

# Update version numbers
Set-VstsBuildNumber $PackageVersion
Update-ProjectVersion "src\Parquet\Parquet.csproj"

# Restore packages
Exec "dotnet restore $SlnPath"

# Build solution
Get-ChildItem *.nupkg -Recurse | Remove-Item -ErrorAction Ignore
Exec "dotnet build $SlnPath -c release"

# Run the tests
Exec "dotnet test src\Parquet.Test\Parquet.Test.csproj"

# publish the nugets
if($Publish.IsPresent)
{
   Write-Host "publishing nugets..."

   Get-ChildItem *.nupkg -Recurse | % {
      $path = $_.FullName
      Write-Host "publishing from $path"
      Exec "nuget push $path -Source https://www.nuget.org/api/v2/package -ApiKey $NuGetApiKey"
   }
}

Write-Host "build succeeded."