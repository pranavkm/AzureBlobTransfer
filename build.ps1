if (!(Test-Path nuget.exe)) {
    Invoke-WebRequest https://www.nuget.org/nuget.exe -OutFile nuget.exe
}

.\nuget.exe restore
&"$($env:windir)\Microsoft.NET\Framework\v4.0.30319\MSBuild" -p:Configuration=Release /v:M
if (!(Test-Path artifacts)) {
    md artifacts | Out-Null
}

Push-Location AzureBlobTransfer
..\nuget.exe pack -Build -Prop Configuration=Release AzureBlobTransfer.csproj -Out ..\artifacts
Pop-Location