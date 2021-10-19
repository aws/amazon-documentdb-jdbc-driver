param (
    [Parameter(mandatory=$true)]
    [string]$unsignedFileName
)

if (-not (Test-Path -Path $unsignedFileName)) {
    throw 'The file does not exist '
}

#Authenticate and get the websession
Write-Output "Authenticating"
$Result = Invoke-WebRequest -Uri "https://codesigner.amazon.com/api/v1/authenticate" -SessionVariable websession -UseDefaultCredentials -UseBasicParsing
if (($Result.StatusCode -ne 200 ) -or $Result -eq $null) {
    throw "Authentication error " + $Result.StatusCode + " " + $Result.StatusDescription
}
Write-Output "Authenticated"

Write-Output "Creating signing request"
$Form = @{
    product_name  = 'DocumentDB JDBC Tableau Connector'
    filename   = "$unsignedFileName"
}
#Create Sign in Request
$Result = $null
$Result = Invoke-WebRequest -Uri "https://codesigner.amazon.com/api/v1/requests" -Websession $websession -UseDefaultCredentials -UseBasicParsing   -Method Post -Body $Form
if (($Result.StatusCode -ne 200 ) -or $Result -eq $null) {
    throw "Creating Sign in request error " + $Result.StatusCode + " " + $Result.StatusDescription
}
Write-Output "Sign request created"

#Parsing Sign in request response
Write-Output "Parsing signing request response"
$ContentObj = $Result.Content | ConvertFrom-Json
$RequestId = $ContentObj.request.id
$SecurityToken = $ContentObj.request.security_token

$Headers = @{'X-Codesigner-security-token' = "$SecurityToken"}

#Upload unsigned file
Write-Output "Uploading unsigned file"
$Result = $null
$Result = Invoke-WebRequest  -Uri "https://codesigner.amazon.com/api/v1/requests/$RequestId/unsigned" -Websession $websession -UseDefaultCredentials -UseBasicParsing -Headers $Headers -Method Put -InFile "$unsignedFileName"
if (($Result.StatusCode -ne 202 ) -or $Result -eq $null) {
    throw "Upload file error " + $Result.StatusCode + " " + $Result.StatusDescription
}

#Check if signed file is ready
Write-Output "Waiting https://codesigner.amazon.com/api/v1/requests/$RequestId/signed be ready"
$Result = $null
$timer = [Diagnostics.Stopwatch]::StartNew()
$timeOut = 30
do {
    try {
        $Result = Invoke-WebRequest  -Uri "https://codesigner.amazon.com/api/v1/requests/$RequestId/signed" -Websession $websession -UseBasicParsing
    } catch {
        "https://codesigner.amazon.com/api/v1/requests/$RequestId/signed not ready yet"
    }
    Start-Sleep -Seconds 1
    if ($timer.Elapsed.TotalSeconds -gt $timeOut) {
        $timer.Stop()
        throw "Timeout of $timeOut seconds reached. File is not ready to download or some other error occoured check https://codesigner.amazon.com/requests/new or your emails for a more detailed error."
    }
}
while (($Result.StatusCode -ne 200 ) -or $Result -eq $null)
$timer.Stop()

#Download signed file
Write-Output "Downloading signed file"
Invoke-WebRequest  -Uri "https://codesigner.amazon.com/api/v1/requests/$RequestId/signed" -Websession $websession -UseBasicParsing -OutFile "$unsignedFileName-signed"
Write-Output "Downloaded signed file"
