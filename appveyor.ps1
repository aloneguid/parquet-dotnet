### AppVeyor specific code

#APPVEYOR_BUILD_VERSION=1.0.21

$Copyright = "Copyright (c) 2017 by Elastacloud Ltd."
$PackageIconUrl = "http://i.isolineltd.com/nuget/parquet.png"
$PackageProjectUrl = "https://github.com/elastacloud/parquet-dotnet"
$RepositoryUrl = "https://github.com/elastacloud/parquet-dotnet"
$Authors = "Ivan Gavryliuk (@aloneguid); Richard Conway (@azurecoder)"
$PackageLicenseUrl = "https://github.com/elastacloud/parquet-dotnet/blob/master/LICENSE"
$RepositoryType = "GitHub"

$gv = $env:APPVEYOR_BUILD_VERSION
if($gv -eq $null)
{
   $gv = "1.0.0"
}

function Update-ProjectVersion($File)
{
   $v = $gv

   $xml = [xml](Get-Content $File.FullName)

   if($xml.Project.PropertyGroup.Count -eq $null)
   {
      $pg = $xml.Project.PropertyGroup
   }
   else
   {
      $pg = $xml.Project.PropertyGroup[0]
   }

   $parts = $v -split "\."
   $bv = $parts[2]
   if($bv.Contains("-")) { $bv = $bv.Substring(0, $bv.IndexOf("-"))}
   $fv = "{0}.{1}.{2}.0" -f $parts[0], $parts[1], $bv
   $av = "{0}.0.0.0" -f $parts[0]
   $pv = $v

   $pg.Version = $pv
   $pg.FileVersion = $fv
   $pg.AssemblyVersion = $av

   Write-Host "$($File.Name) => fv: $fv, av: $av, pkg: $pv"

   $pg.Copyright = $Copyright
   $pg.PackageIconUrl = $PackageIconUrl
   $pg.PackageProjectUrl = $PackageProjectUrl
   $pg.RepositoryUrl = $RepositoryUrl
   $pg.Authors = $Authors
   $pg.PackageLicenseUrl = $PackageLicenseUrl
   $pg.RepositoryType = $RepositoryType

   $xml.Save($File.FullName)
}

Invoke-Expression "nuget restore src/Parquet.sln"

# Update versioning information
Get-ChildItem *.csproj -Recurse | Where-Object {-not(($_.Name -like "*test*") -or ($_.Name -like "*parq.csproj*") -or ($_.Name -like "*Runner.csproj*") ) } | % {
   #Write-Host $_
   Update-ProjectVersion $_
}