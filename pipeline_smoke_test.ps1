[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:8000",
    [string]$UserEmail,
    [string]$UserPassword = "SmokePass123!",
    [string]$UserFullName = "Smoke Pipeline User",
    [string]$AdminEmail = $env:SMOKE_ADMIN_EMAIL,
    [string]$AdminPassword = $env:SMOKE_ADMIN_PASSWORD,
    [int]$PipelineDaysRange = 7,
    [int]$GlobalDaysRange = 14,
    [int]$PollIntervalSeconds = 5,
    [int]$PipelineWaitTimeoutSeconds = 180,
    [switch]$FailOnWarnings
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if (-not $PSBoundParameters.ContainsKey("UserEmail")) {
    $UserEmail = "smoke.user.$(Get-Date -Format 'yyyyMMddHHmmss')@example.com"
}

$script:Failures = New-Object System.Collections.Generic.List[string]
$script:Warnings = New-Object System.Collections.Generic.List[string]
$script:StartedAt = Get-Date

function Add-Failure {
    param([string]$Message)
    $script:Failures.Add($Message)
    Write-Host "[FAIL] $Message" -ForegroundColor Red
}

function Add-Warning {
    param([string]$Message)
    $script:Warnings.Add($Message)
    Write-Host "[WARN] $Message" -ForegroundColor Yellow
}

function Add-Ok {
    param([string]$Message)
    Write-Host "[ OK ] $Message" -ForegroundColor Green
}

function Write-Step {
    param([string]$Message)
    Write-Host "`n==> $Message" -ForegroundColor Cyan
}

function Get-ResponseMessage {
    param($Response)

    if ($null -eq $Response) {
        return ""
    }

    if ($Response.Json) {
        if ($Response.Json.PSObject.Properties.Name -contains "detail") {
            return [string]$Response.Json.detail
        }

        if ($Response.Json.PSObject.Properties.Name -contains "message") {
            return [string]$Response.Json.message
        }
    }

    return [string]$Response.RawBody
}

function Invoke-ApiRequest {
    param(
        [Parameter(Mandatory = $true)]
        [ValidateSet("GET", "POST", "PUT", "DELETE")]
        [string]$Method,
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [object]$Body = $null,
        [string]$AuthCookieToken = ""
    )

    $base = $BaseUrl.TrimEnd("/")
    $uri = "$base$Path"

    $headers = @{}
    if ($AuthCookieToken) {
        $headers["Cookie"] = "auth=$AuthCookieToken"
    }

    $jsonBody = $null
    if ($null -ne $Body) {
        $jsonBody = $Body | ConvertTo-Json -Depth 20
    }

    try {
        if ($null -ne $jsonBody) {
            $response = Invoke-WebRequest -Method $Method -Uri $uri -Headers $headers -ContentType "application/json" -Body $jsonBody -UseBasicParsing
        }
        else {
            $response = Invoke-WebRequest -Method $Method -Uri $uri -Headers $headers -UseBasicParsing
        }

        $json = $null
        if ($response.Content) {
            try {
                $json = $response.Content | ConvertFrom-Json -Depth 100
            }
            catch {
                $json = $null
            }
        }

        return [PSCustomObject]@{
            Ok         = $true
            StatusCode = [int]$response.StatusCode
            Json       = $json
            Headers    = $response.Headers
            RawBody    = [string]$response.Content
            Url        = $uri
        }
    }
    catch {
        $statusCode = 0
        $rawBody = ""
        $headersOut = $null

        if ($_.Exception.Response) {
            $errorResponse = $_.Exception.Response
            $headersOut = $errorResponse.Headers

            try {
                $statusCode = [int]$errorResponse.StatusCode
            }
            catch {
                $statusCode = 0
            }

            try {
                $stream = $errorResponse.GetResponseStream()
                if ($stream) {
                    $reader = New-Object System.IO.StreamReader($stream)
                    $rawBody = $reader.ReadToEnd()
                    $reader.Close()
                }
            }
            catch {
                $rawBody = [string]$_.Exception.Message
            }
        }
        else {
            $rawBody = [string]$_.Exception.Message
        }

        $json = $null
        if ($rawBody) {
            try {
                $json = $rawBody | ConvertFrom-Json -Depth 100
            }
            catch {
                $json = $null
            }
        }

        return [PSCustomObject]@{
            Ok         = $false
            StatusCode = $statusCode
            Json       = $json
            Headers    = $headersOut
            RawBody    = $rawBody
            Url        = $uri
        }
    }
}

function Get-AuthCookieToken {
    param($Headers)

    if ($null -eq $Headers) {
        return $null
    }

    $setCookieHeader = $Headers["Set-Cookie"]
    if ($null -eq $setCookieHeader) {
        return $null
    }

    $joined = ""
    if ($setCookieHeader -is [System.Array]) {
        $joined = [string]::Join("; ", $setCookieHeader)
    }
    else {
        $joined = [string]$setCookieHeader
    }

    $match = [regex]::Match($joined, "auth=([^;]+)")
    if ($match.Success) {
        return $match.Groups[1].Value
    }

    return $null
}

function Login-And-GetToken {
    param(
        [string]$Email,
        [string]$Password,
        [string]$Label
    )

    $response = Invoke-ApiRequest -Method "POST" -Path "/auth/login" -Body @{
        email = $Email
        password = $Password
    }

    if (-not $response.Ok) {
        Add-Failure "$Label login failed (HTTP $($response.StatusCode)): $(Get-ResponseMessage $response)"
        return $null
    }

    $token = Get-AuthCookieToken -Headers $response.Headers
    if (-not $token) {
        Add-Failure "$Label login succeeded but auth cookie was not returned"
        return $null
    }

    Add-Ok "$Label login succeeded"
    return $token
}

function Test-UserPipelineFlow {
    param([string]$AuthToken)

    Write-Step "User endpoints smoke"

    $meResponse = Invoke-ApiRequest -Method "GET" -Path "/users/me" -AuthCookieToken $AuthToken
    if (-not $meResponse.Ok) {
        Add-Failure "GET /users/me failed (HTTP $($meResponse.StatusCode)): $(Get-ResponseMessage $meResponse)"
        return
    }

    $userData = $meResponse.Json.data.user
    Add-Ok "Authenticated as userId=$($userData.userId) role=$($userData.role)"

    $profilePayload = @{
        objective = "Buscar vagas backend com Python"
        seniority = "PLENO"
        targetRoles = @("Backend Engineer", "Python Developer")
        preferredLocations = @("Remote", "Sao Paulo")
        preferredWorkModel = "REMOTE"
        mustHaveSkills = @("python", "fastapi", "postgresql")
        niceToHaveSkills = @("redis", "rabbitmq")
    }

    $profileResponse = Invoke-ApiRequest -Method "PUT" -Path "/users/me/profile" -Body $profilePayload -AuthCookieToken $AuthToken
    if (-not $profileResponse.Ok) {
        Add-Warning "PUT /users/me/profile failed (HTTP $($profileResponse.StatusCode)): $(Get-ResponseMessage $profileResponse)"
    }
    else {
        Add-Ok "Profile upsert completed"
    }

    $pipelineResponse = Invoke-ApiRequest -Method "POST" -Path "/pipeline/run" -Body @{
        force = $false
        daysRange = [Math]::Max(1, [Math]::Min(30, $PipelineDaysRange))
        forceRescore = $false
    } -AuthCookieToken $AuthToken

    $runId = $null
    if ($pipelineResponse.Ok) {
        $runId = [string]$pipelineResponse.Json.data.runId
        Add-Ok "POST /pipeline/run queued runId=$runId"
    }
    else {
        $runMessage = Get-ResponseMessage $pipelineResponse
        if ($runMessage -match "already in progress") {
            Add-Warning "Pipeline already in progress; status polling will inspect active run"
        }
        else {
            Add-Failure "POST /pipeline/run failed (HTTP $($pipelineResponse.StatusCode)): $runMessage"
            return
        }
    }

    Write-Step "Polling /pipeline/status"

    $deadline = (Get-Date).AddSeconds([Math]::Max(10, $PipelineWaitTimeoutSeconds))
    $completed = $false

    while ((Get-Date) -lt $deadline) {
        $statusResponse = Invoke-ApiRequest -Method "GET" -Path "/pipeline/status" -AuthCookieToken $AuthToken
        if (-not $statusResponse.Ok) {
            Add-Failure "GET /pipeline/status failed (HTTP $($statusResponse.StatusCode)): $(Get-ResponseMessage $statusResponse)"
            return
        }

        $statusData = $statusResponse.Json.data
        $lastRun = $statusData.lastRun
        $lastRunId = if ($lastRun) { [string]$lastRun.runId } else { "" }
        $lastRunStatus = if ($lastRun) { [string]$lastRun.status } else { "" }
        $activeRunId = [string]$statusData.activeRunId

        Write-Host (
            "status: running={0} activeRunId={1} lastRunId={2} lastRunStatus={3}" -f
            [bool]$statusData.isRunning,
            $activeRunId,
            $lastRunId,
            $lastRunStatus
        )

        if ($statusData.activeRunMetrics) {
            $metrics = $statusData.activeRunMetrics
            Write-Host (
                "metrics: processed={0} failed={1} aiCalls={2} aiHitRate={3}" -f
                $metrics.jobsProcessed,
                $metrics.jobsFailed,
                $metrics.aiCalls,
                $metrics.aiCacheHitRate
            )
        }

        if ($runId -and $lastRunId -eq $runId -and $lastRunStatus -eq "COMPLETED") {
            Add-Ok "Pipeline run completed for runId=$runId"
            $completed = $true
            break
        }

        if (-not $runId -and $lastRunStatus -eq "COMPLETED") {
            Add-Ok "Detected completed pipeline run (runId=$lastRunId)"
            $completed = $true
            break
        }

        Start-Sleep -Seconds ([Math]::Max(1, $PollIntervalSeconds))
    }

    if (-not $completed) {
        Add-Warning "Pipeline run did not reach COMPLETED within timeout; workers may be down or queue is backlogged"
    }

    if ($completed) {
        $safeRankingDays = [Math]::Max(1, [Math]::Min(30, $PipelineDaysRange))
        $rankingPath = "/jobs/ranking/daily?daysRange=$safeRankingDays&limit=10"
        $rankingResponse = Invoke-ApiRequest -Method "GET" -Path $rankingPath -AuthCookieToken $AuthToken

        if (-not $rankingResponse.Ok) {
            Add-Warning "GET $rankingPath failed (HTTP $($rankingResponse.StatusCode)): $(Get-ResponseMessage $rankingResponse)"
        }
        else {
            $rankingItems = $rankingResponse.Json.data.ranking
            $rankingCount = if ($rankingItems) { [int]$rankingItems.Count } else { 0 }
            $window = $rankingResponse.Json.data.window
            Add-Ok "Ranking window read (items=$rankingCount dateFrom=$($window.dateFrom) dateTo=$($window.dateTo))"
        }
    }
}

function Test-AdminPipelineFlow {
    param([string]$AuthToken)

    Write-Step "Admin endpoints smoke"

    $meResponse = Invoke-ApiRequest -Method "GET" -Path "/users/me" -AuthCookieToken $AuthToken
    if (-not $meResponse.Ok) {
        Add-Failure "Admin GET /users/me failed (HTTP $($meResponse.StatusCode)): $(Get-ResponseMessage $meResponse)"
        return
    }

    $role = [string]$meResponse.Json.data.user.role
    if ($role.ToUpperInvariant() -ne "ADMIN") {
        Add-Warning "Provided admin account is role '$role'; skipping admin-only pipeline endpoints"
        return
    }

    Add-Ok "Admin role validated"

    $globalRunResponse = Invoke-ApiRequest -Method "POST" -Path "/pipeline/global/run" -Body @{
        force = $false
        daysRange = [Math]::Max(1, [Math]::Min(30, $GlobalDaysRange))
    } -AuthCookieToken $AuthToken

    if ($globalRunResponse.Ok) {
        Add-Ok "POST /pipeline/global/run queued runId=$($globalRunResponse.Json.data.runId)"
    }
    else {
        $message = Get-ResponseMessage $globalRunResponse
        if ($message -match "already in progress") {
            Add-Warning "Global ingestion already in progress"
        }
        else {
            Add-Failure "POST /pipeline/global/run failed (HTTP $($globalRunResponse.StatusCode)): $message"
        }
    }

    $globalStatusResponse = Invoke-ApiRequest -Method "GET" -Path "/pipeline/global/status" -AuthCookieToken $AuthToken
    if (-not $globalStatusResponse.Ok) {
        Add-Failure "GET /pipeline/global/status failed (HTTP $($globalStatusResponse.StatusCode)): $(Get-ResponseMessage $globalStatusResponse)"
    }
    else {
        $globalStatus = $globalStatusResponse.Json.data
        Add-Ok "Global ingestion status read (isRunning=$($globalStatus.isRunning))"
    }

    $cleanupRunResponse = Invoke-ApiRequest -Method "POST" -Path "/pipeline/global/catalog-cleanup/run" -AuthCookieToken $AuthToken
    if (-not $cleanupRunResponse.Ok) {
        $cleanupMessage = Get-ResponseMessage $cleanupRunResponse
        if ($cleanupMessage -match "already in progress") {
            Add-Warning "Global catalog cleanup already in progress"
        }
        else {
            Add-Failure "POST /pipeline/global/catalog-cleanup/run failed (HTTP $($cleanupRunResponse.StatusCode)): $cleanupMessage"
        }
    }
    else {
        $cleanupData = $cleanupRunResponse.Json.data
        Add-Ok "Cleanup run completed trigger=$($cleanupData.trigger) deletedJobs=$($cleanupData.deletedJobs)"
    }

    $cleanupStatusResponse = Invoke-ApiRequest -Method "GET" -Path "/pipeline/global/catalog-cleanup/status" -AuthCookieToken $AuthToken
    if (-not $cleanupStatusResponse.Ok) {
        Add-Failure "GET /pipeline/global/catalog-cleanup/status failed (HTTP $($cleanupStatusResponse.StatusCode)): $(Get-ResponseMessage $cleanupStatusResponse)"
    }
    else {
        $cleanupStatus = $cleanupStatusResponse.Json.data
        Add-Ok "Cleanup status read (isRunning=$($cleanupStatus.isRunning))"
    }
}

Write-Host "WiredApply complete smoke test" -ForegroundColor Magenta
Write-Host "BaseUrl: $BaseUrl"
Write-Host "UserEmail: $UserEmail"

Write-Step "Connectivity check"
$openapiResponse = Invoke-ApiRequest -Method "GET" -Path "/openapi.json"
if (-not $openapiResponse.Ok) {
    Add-Failure "API is not reachable at $BaseUrl (HTTP $($openapiResponse.StatusCode)): $(Get-ResponseMessage $openapiResponse)"
}
else {
    Add-Ok "API reachable"
}

if ($script:Failures.Count -eq 0) {
    Write-Step "Ensuring smoke user exists"

    $createUserResponse = Invoke-ApiRequest -Method "POST" -Path "/users/" -Body @{
        fullName = $UserFullName
        email = $UserEmail
        password = $UserPassword
    }

    if ($createUserResponse.Ok) {
        Add-Ok "Smoke user created"
    }
    else {
        $createMessage = Get-ResponseMessage $createUserResponse
        if ($createMessage -match "Email already registered") {
            Add-Warning "Smoke user already exists; login will be reused"
        }
        else {
            Add-Failure "Unable to create smoke user (HTTP $($createUserResponse.StatusCode)): $createMessage"
        }
    }
}

$userToken = $null
if ($script:Failures.Count -eq 0) {
    Write-Step "User login"
    $userToken = Login-And-GetToken -Email $UserEmail -Password $UserPassword -Label "Smoke user"
}

if ($script:Failures.Count -eq 0 -and $userToken) {
    Test-UserPipelineFlow -AuthToken $userToken
}

if ([string]::IsNullOrWhiteSpace($AdminEmail) -or [string]::IsNullOrWhiteSpace($AdminPassword)) {
    Add-Warning "Admin credentials were not provided; admin endpoints were skipped"
}
elseif ($script:Failures.Count -eq 0) {
    Write-Step "Admin login"
    $adminToken = Login-And-GetToken -Email $AdminEmail -Password $AdminPassword -Label "Admin"

    if ($adminToken) {
        Test-AdminPipelineFlow -AuthToken $adminToken
    }
}

$elapsed = (Get-Date) - $script:StartedAt
Write-Host "`n================ Smoke Test Summary ================" -ForegroundColor White
Write-Host "Elapsed: $([int]$elapsed.TotalSeconds)s"
Write-Host "Failures: $($script:Failures.Count)"
Write-Host "Warnings: $($script:Warnings.Count)"

if ($script:Failures.Count -gt 0) {
    Write-Host "`nFailure details:" -ForegroundColor Red
    foreach ($failure in $script:Failures) {
        Write-Host " - $failure" -ForegroundColor Red
    }

    exit 1
}

if ($script:Warnings.Count -gt 0) {
    Write-Host "`nWarning details:" -ForegroundColor Yellow
    foreach ($warning in $script:Warnings) {
        Write-Host " - $warning" -ForegroundColor Yellow
    }

    if ($FailOnWarnings) {
        exit 2
    }
}

Write-Host "`nSmoke test finished successfully." -ForegroundColor Green
exit 0
