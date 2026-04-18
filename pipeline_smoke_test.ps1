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
    [int]$RankingSampleLimit = 50,
    [int]$RankingConsistencyProbeCount = 3,
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

function Get-ObjectValue {
    param(
        $Object,
        [string]$PropertyName,
        $Default = $null
    )

    if ($null -eq $Object) {
        return $Default
    }

    if ($Object -is [System.Collections.IDictionary]) {
        if ($Object.Contains($PropertyName)) {
            return $Object[$PropertyName]
        }

        return $Default
    }

    $properties = $Object.PSObject.Properties
    if ($properties -and ($properties.Name -contains $PropertyName)) {
        return $Object.$PropertyName
    }

    return $Default
}

function Convert-ToNullableDouble {
    param($Value)

    if ($null -eq $Value) {
        return $null
    }

    try {
        return [double]$Value
    }
    catch {
        return $null
    }
}

function Convert-ToNullableInt {
    param($Value)

    if ($null -eq $Value) {
        return $null
    }

    try {
        return [int]$Value
    }
    catch {
        return $null
    }
}

function Convert-ToIntOrDefault {
    param(
        $Value,
        [int]$Default = 0
    )

    $parsed = Convert-ToNullableInt $Value
    if ($null -ne $parsed) {
        return $parsed
    }

    return $Default
}

function Convert-ToDateOrNull {
    param($Value)

    if ($null -eq $Value) {
        return $null
    }

    $text = [string]$Value
    if ([string]::IsNullOrWhiteSpace($text)) {
        return $null
    }

    $parsed = [datetime]::MinValue
    if ([datetime]::TryParse($text, [ref]$parsed)) {
        return $parsed.Date
    }

    return $null
}

function Format-Ratio {
    param($Part, $Whole)

    $partValue = Convert-ToNullableDouble $Part
    $wholeValue = Convert-ToNullableDouble $Whole

    if ($null -eq $partValue -or $null -eq $wholeValue -or $wholeValue -le 0) {
        return "n/a"
    }

    return ("{0}%" -f [Math]::Round(($partValue / $wholeValue) * 100.0, 1))
}

function Format-Rate {
    param($Rate)

    $rateValue = Convert-ToNullableDouble $Rate
    if ($null -eq $rateValue) {
        return "n/a"
    }

    return ("{0}%" -f [Math]::Round($rateValue * 100.0, 1))
}

function Get-NumericSummary {
    param([object[]]$Values)

    $numbers = New-Object System.Collections.Generic.List[double]
    foreach ($rawValue in $Values) {
        $numericValue = Convert-ToNullableDouble $rawValue
        if ($null -ne $numericValue) {
            $numbers.Add($numericValue)
        }
    }

    if ($numbers.Count -eq 0) {
        return $null
    }

    $sorted = $numbers.ToArray()
    [array]::Sort($sorted)

    $count = $sorted.Length
    $sum = 0.0
    foreach ($number in $sorted) {
        $sum += $number
    }

    $average = $sum / [double]$count

    $variance = 0.0
    foreach ($number in $sorted) {
        $variance += [Math]::Pow(($number - $average), 2)
    }
    $variance = $variance / [double]$count

    $median = 0.0
    $medianIndex = [int][Math]::Floor(($count - 1) / 2)
    if (($count % 2) -eq 1) {
        $median = $sorted[$medianIndex]
    }
    else {
        $median = ($sorted[$medianIndex] + $sorted[$medianIndex + 1]) / 2.0
    }

    $p90Index = [int][Math]::Floor(($count - 1) * 0.9)

    return [PSCustomObject]@{
        Count  = $count
        Min    = [Math]::Round($sorted[0], 2)
        Max    = [Math]::Round($sorted[$count - 1], 2)
        Avg    = [Math]::Round($average, 2)
        Median = [Math]::Round($median, 2)
        StdDev = [Math]::Round([Math]::Sqrt($variance), 2)
        P90    = [Math]::Round($sorted[$p90Index], 2)
    }
}

function Test-RankingScoreConsistency {
    param(
        [object[]]$RankingItems,
        [string]$AuthToken
    )

    if (-not $RankingItems -or $RankingItems.Count -eq 0) {
        return
    }

    $safeProbeCount = [Math]::Min(
        [Math]::Max(1, $RankingConsistencyProbeCount),
        $RankingItems.Count
    )

    $checked = 0
    $requestFailures = 0
    $mismatches = 0

    for ($index = 0; $index -lt $safeProbeCount; $index++) {
        $item = $RankingItems[$index]
        $jobId = Convert-ToNullableInt (Get-ObjectValue $item "jobId")
        if ($null -eq $jobId) {
            continue
        }

        $response = Invoke-ApiRequest -Method "GET" -Path "/jobs/$jobId/score" -AuthCookieToken $AuthToken
        if (-not $response.Ok) {
            $requestFailures += 1
            continue
        }

        $checked += 1

        $scoreData = Get-ObjectValue (Get-ObjectValue $response.Json "data") "jobScore"
        $rankingScore = Convert-ToNullableDouble (Get-ObjectValue $item "score")
        $detailScore = Convert-ToNullableDouble (Get-ObjectValue $scoreData "score")

        if ($null -eq $rankingScore -or $null -eq $detailScore) {
            continue
        }

        if ([Math]::Abs($rankingScore - $detailScore) -gt 0.01) {
            $mismatches += 1
        }
    }

    if ($checked -eq 0) {
        Add-Warning "Ranking consistency probe could not validate scores (no successful score lookups)"
        return
    }

    if ($requestFailures -gt 0) {
        Add-Warning "Ranking consistency probe had $requestFailures score lookup failures"
    }

    if ($mismatches -gt 0) {
        Add-Warning "Ranking consistency probe detected $mismatches score mismatches across $checked jobs"
    }
    else {
        Add-Ok "Ranking consistency probe passed for $checked jobs"
    }
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
        $headers["Authorization"] = "Bearer $AuthCookieToken"
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
                $json = $response.Content | ConvertFrom-Json
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
                if ($errorResponse.StatusCode -is [int]) {
                    $statusCode = [int]$errorResponse.StatusCode
                }
                elseif ($null -ne $errorResponse.StatusCode) {
                    $statusCode = [int]$errorResponse.StatusCode.value__
                }
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

        if ([string]::IsNullOrWhiteSpace($rawBody) -and $_.ErrorDetails -and $_.ErrorDetails.Message) {
            $rawBody = [string]$_.ErrorDetails.Message
        }

        $json = $null
        if ($rawBody) {
            try {
                $json = $rawBody | ConvertFrom-Json
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

    $userData = Get-ObjectValue (Get-ObjectValue $meResponse.Json "data") "user"
    if ($null -eq $userData) {
        Add-Failure "GET /users/me returned an unexpected payload"
        return
    }

    Add-Ok "Authenticated as userId=$($userData.userId) role=$($userData.role)"

    $baselineJobsCount = $null
    $baselineApplicationsCount = $null
    $baselineStatusResponse = Invoke-ApiRequest -Method "GET" -Path "/pipeline/status" -AuthCookieToken $AuthToken
    if ($baselineStatusResponse.Ok) {
        $baselineData = Get-ObjectValue $baselineStatusResponse.Json "data"
        $baselineJobsCount = Convert-ToNullableInt (Get-ObjectValue $baselineData "jobsCount")
        $baselineApplicationsCount = Convert-ToNullableInt (Get-ObjectValue $baselineData "applicationsCount")

        if ($null -ne $baselineJobsCount -and $null -ne $baselineApplicationsCount) {
            Add-Ok "Baseline inventory jobs=$baselineJobsCount applications=$baselineApplicationsCount"
        }
    }
    else {
        Add-Warning "Could not read baseline /pipeline/status before run (HTTP $($baselineStatusResponse.StatusCode))"
    }

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
    $lastObservedMetrics = $null
    $completedRunId = $runId

    while ((Get-Date) -lt $deadline) {
        $statusResponse = Invoke-ApiRequest -Method "GET" -Path "/pipeline/status" -AuthCookieToken $AuthToken
        if (-not $statusResponse.Ok) {
            Add-Failure "GET /pipeline/status failed (HTTP $($statusResponse.StatusCode)): $(Get-ResponseMessage $statusResponse)"
            return
        }

        $statusData = Get-ObjectValue $statusResponse.Json "data"
        $lastRun = Get-ObjectValue $statusData "lastRun"
        $lastRunId = [string](Get-ObjectValue $lastRun "runId" "")
        $lastRunStatus = [string](Get-ObjectValue $lastRun "status" "")
        $activeRunId = [string](Get-ObjectValue $statusData "activeRunId" "")
        $isRunning = [bool](Get-ObjectValue $statusData "isRunning" $false)

        Write-Host (
            "status: running={0} activeRunId={1} lastRunId={2} lastRunStatus={3}" -f
            $isRunning,
            $activeRunId,
            $lastRunId,
            $lastRunStatus
        )

        $activeRunMetrics = Get-ObjectValue $statusData "activeRunMetrics"
        if ($activeRunMetrics) {
            $metrics = $activeRunMetrics
            $lastObservedMetrics = $activeRunMetrics

            Write-Host (
                "metrics: processed={0} failed={1} aiCalls={2} aiHitRate={3} prefilterRejected={4}" -f
                (Get-ObjectValue $metrics "jobsProcessed"),
                (Get-ObjectValue $metrics "jobsFailed"),
                (Get-ObjectValue $metrics "aiCalls"),
                (Get-ObjectValue $metrics "aiCacheHitRate"),
                (Get-ObjectValue $metrics "aiPrefilterRejected")
            )
        }

        if ($runId -and $lastRunId -eq $runId -and $lastRunStatus -eq "COMPLETED") {
            Add-Ok "Pipeline run completed for runId=$runId"
            $completed = $true
            $completedRunId = $runId
            break
        }

        if (-not $runId -and $lastRunStatus -eq "COMPLETED") {
            Add-Ok "Detected completed pipeline run (runId=$lastRunId)"
            $completed = $true
            $completedRunId = $lastRunId
            break
        }

        Start-Sleep -Seconds ([Math]::Max(1, $PollIntervalSeconds))
    }

    if (-not $completed) {
        Add-Warning "Pipeline run did not reach COMPLETED within timeout; workers may be down or queue is backlogged"
    }

    if ($true) {
        if (-not $completed) {
            Add-Warning "Quantitative analysis is based on a partial snapshot because run did not complete"
        }

        $postRunMetrics = $lastObservedMetrics
        $postRunData = $null
        $postJobsCount = $null
        $postApplicationsCount = $null

        $postStatusResponse = Invoke-ApiRequest -Method "GET" -Path "/pipeline/status" -AuthCookieToken $AuthToken
        if ($postStatusResponse.Ok) {
            $postStatusData = Get-ObjectValue $postStatusResponse.Json "data"
            $postJobsCount = Convert-ToNullableInt (Get-ObjectValue $postStatusData "jobsCount")
            $postApplicationsCount = Convert-ToNullableInt (Get-ObjectValue $postStatusData "applicationsCount")
            $postRunData = Get-ObjectValue $postStatusData "lastRun"

            if ($null -ne $postRunData) {
                $runMetricsCandidate = Get-ObjectValue $postRunData "metrics"
                if ($runMetricsCandidate) {
                    $postRunMetrics = $runMetricsCandidate
                }

                $postRunId = [string](Get-ObjectValue $postRunData "runId" "")
                $postRunStatus = [string](Get-ObjectValue $postRunData "status" "")
                if ($completedRunId -and $postRunId -and $postRunId -ne $completedRunId) {
                    Add-Warning "Last run in status payload differs from completed run (completed=$completedRunId lastRun=$postRunId)"
                }

                Add-Ok "Post-run status runId=$postRunId status=$postRunStatus"
            }

            if ($null -ne $postJobsCount -and $null -ne $postApplicationsCount) {
                Add-Ok "Post-run inventory jobs=$postJobsCount applications=$postApplicationsCount"
            }
        }
        else {
            Add-Warning "Could not read post-run /pipeline/status (HTTP $($postStatusResponse.StatusCode)): $(Get-ResponseMessage $postStatusResponse)"
        }

        if ($null -ne $baselineJobsCount -and $null -ne $postJobsCount) {
            $jobsDelta = $postJobsCount - $baselineJobsCount
            Add-Ok "Inventory delta after run: jobsDelta=$jobsDelta"

            if ($jobsDelta -lt 0) {
                Add-Warning "Jobs inventory decreased after run (delta=$jobsDelta)"
            }
        }

        if ($postRunMetrics) {
            $jobsProcessed = [Math]::Max(0, (Convert-ToIntOrDefault (Get-ObjectValue $postRunMetrics "jobsProcessed") 0))
            $jobsFailed = [Math]::Max(0, (Convert-ToIntOrDefault (Get-ObjectValue $postRunMetrics "jobsFailed") 0))
            $jobsFinishedCandidate = Convert-ToNullableInt (Get-ObjectValue $postRunMetrics "jobsFinished")
            if ($null -ne $jobsFinishedCandidate) {
                $jobsFinished = [Math]::Max(0, [int]$jobsFinishedCandidate)
            }
            else {
                $jobsFinished = [Math]::Max(0, ($jobsProcessed + $jobsFailed))
            }

            $aiCalls = [Math]::Max(0, (Convert-ToIntOrDefault (Get-ObjectValue $postRunMetrics "aiCalls") 0))
            $aiCacheHits = [Math]::Max(0, (Convert-ToIntOrDefault (Get-ObjectValue $postRunMetrics "aiCacheHits") 0))
            $aiCacheMisses = [Math]::Max(0, (Convert-ToIntOrDefault (Get-ObjectValue $postRunMetrics "aiCacheMisses") 0))
            $aiSkipped = [Math]::Max(0, (Convert-ToIntOrDefault (Get-ObjectValue $postRunMetrics "aiSkipped") 0))
            $aiPrefilterRejected = [Math]::Max(0, (Convert-ToIntOrDefault (Get-ObjectValue $postRunMetrics "aiPrefilterRejected") 0))
            $aiCacheHitRate = Convert-ToNullableDouble (Get-ObjectValue $postRunMetrics "aiCacheHitRate")
            $aiEligibleCount = $aiCalls + $aiCacheHits
            $aiEligibleCacheHitRateText = Format-Ratio $aiCacheHits $aiEligibleCount

            Add-Ok (
                "Run metrics: finished={0} processed={1} failed={2} failRate={3} aiCalls={4} aiSkipped={5} cacheHitRate={6} eligibleCacheHitRate={7}" -f
                $jobsFinished,
                $jobsProcessed,
                $jobsFailed,
                (Format-Ratio $jobsFailed $jobsFinished),
                $aiCalls,
                $aiSkipped,
                (Format-Rate $aiCacheHitRate),
                $aiEligibleCacheHitRateText
            )

            $prefilterReasons = Get-ObjectValue $postRunMetrics "aiPrefilterReasons"
            $prefilterReasonParts = New-Object System.Collections.Generic.List[string]
            if ($prefilterReasons -is [System.Collections.IDictionary]) {
                foreach ($key in ($prefilterReasons.Keys | Sort-Object)) {
                    $prefilterReasonParts.Add("$key=$($prefilterReasons[$key])")
                }
            }
            elseif ($prefilterReasons) {
                foreach ($prop in $prefilterReasons.PSObject.Properties) {
                    $prefilterReasonParts.Add("$($prop.Name)=$($prop.Value)")
                }
            }

            if ($prefilterReasonParts.Count -gt 0) {
                Add-Ok "AI prefilter reasons: $($prefilterReasonParts -join ', ')"
            }

            if ($jobsFinished -eq 0) {
                Add-Warning "Run finished with zero processed jobs"
            }

            if ($jobsFailed -gt 0) {
                Add-Warning "Run had failed scoring jobs ($jobsFailed of $jobsFinished)"
            }

            if ($jobsProcessed -gt 0 -and $aiPrefilterRejected -gt 0) {
                $prefilterRejectRate = [double]$aiPrefilterRejected / [double]$jobsProcessed
                if ($prefilterRejectRate -gt 0.9) {
                    Add-Warning "AI prefilter rejected over 90% of processed jobs ($([Math]::Round($prefilterRejectRate * 100.0, 1))%)"
                }
            }

            if ($null -ne $aiCacheHitRate -and $aiCalls -ge 20 -and $aiCacheHitRate -lt 0.05) {
                Add-Warning "AI cache hit rate is low with high call volume (calls=$aiCalls hitRate=$(Format-Rate $aiCacheHitRate))"
            }

            if ($aiCalls -eq 0 -and $jobsProcessed -ge 20 -and $aiCacheHits -eq 0) {
                Add-Warning "No AI calls were executed even with a larger processed set (processed=$jobsProcessed)"
            }
        }
        else {
            Add-Warning "Run metrics were not available on /pipeline/status"
        }

        $safeRankingDays = [Math]::Max(1, [Math]::Min(30, $PipelineDaysRange))
        $safeRankingLimit = [Math]::Max(10, [Math]::Min(100, $RankingSampleLimit))
        $rankingPath = "/jobs/ranking/daily?daysRange=$safeRankingDays&limit=$safeRankingLimit"
        $rankingResponse = Invoke-ApiRequest -Method "GET" -Path $rankingPath -AuthCookieToken $AuthToken

        if (-not $rankingResponse.Ok) {
            Add-Warning "GET $rankingPath failed (HTTP $($rankingResponse.StatusCode)): $(Get-ResponseMessage $rankingResponse)"
        }
        else {
            $rankingData = Get-ObjectValue $rankingResponse.Json "data"
            $rankingItemsRaw = Get-ObjectValue $rankingData "ranking" @()
            $rankingItems = @($rankingItemsRaw | Where-Object { $null -ne $_ })
            $rankingCount = $rankingItems.Count
            $window = Get-ObjectValue $rankingData "window"

            $windowDateFrom = Convert-ToDateOrNull (Get-ObjectValue $window "dateFrom")
            $windowDateTo = Convert-ToDateOrNull (Get-ObjectValue $window "dateTo")
            $windowDaysRange = Convert-ToNullableInt (Get-ObjectValue $window "daysRange")

            Add-Ok "Ranking window read (items=$rankingCount dateFrom=$(Get-ObjectValue $window 'dateFrom' 'n/a') dateTo=$(Get-ObjectValue $window 'dateTo' 'n/a') daysRange=$(Get-ObjectValue $window 'daysRange' 'n/a'))"

            if ($null -ne $windowDaysRange -and $windowDaysRange -ne $safeRankingDays) {
                Add-Warning "Ranking window daysRange ($windowDaysRange) differs from requested daysRange ($safeRankingDays)"
            }

            if ($rankingCount -eq 0) {
                Add-Warning "Ranking returned zero jobs for the analyzed window"
            }
            else {
                $scoreValues = New-Object System.Collections.Generic.List[double]
                $deterministicScoreValues = New-Object System.Collections.Generic.List[double]
                $aiScoreValues = New-Object System.Collections.Generic.List[double]
                $aiConfidenceValues = New-Object System.Collections.Generic.List[double]
                $scoreDeltaValues = New-Object System.Collections.Generic.List[double]
                $effectiveAgeValues = New-Object System.Collections.Generic.List[double]
                $bucketCounts = @{}
                $companiesSeen = @{}

                $sortViolations = 0
                $effectiveDateMissing = 0
                $effectiveDateOutOfWindow = 0
                $previousScore = $null
                $todayDate = (Get-Date).Date

                foreach ($rankingItem in $rankingItems) {
                    $score = Convert-ToNullableDouble (Get-ObjectValue $rankingItem "score")
                    $deterministicScore = Convert-ToNullableDouble (Get-ObjectValue $rankingItem "deterministicScore")
                    $aiScore = Convert-ToNullableDouble (Get-ObjectValue $rankingItem "aiScore")
                    $aiConfidence = Convert-ToNullableDouble (Get-ObjectValue $rankingItem "aiConfidence")

                    if ($null -ne $score) {
                        $scoreValues.Add($score)
                        if ($null -ne $previousScore -and $score -gt ($previousScore + 0.0001)) {
                            $sortViolations += 1
                        }
                        $previousScore = $score
                    }

                    if ($null -ne $deterministicScore) {
                        $deterministicScoreValues.Add($deterministicScore)
                    }

                    if ($null -ne $aiScore) {
                        $aiScoreValues.Add($aiScore)
                    }

                    if ($null -ne $aiConfidence) {
                        $aiConfidenceValues.Add($aiConfidence)
                    }

                    if ($null -ne $score -and $null -ne $deterministicScore) {
                        $scoreDeltaValues.Add($score - $deterministicScore)
                    }

                    $bucket = [string](Get-ObjectValue $rankingItem "bucket" "")
                    if (-not [string]::IsNullOrWhiteSpace($bucket)) {
                        if (-not $bucketCounts.ContainsKey($bucket)) {
                            $bucketCounts[$bucket] = 0
                        }
                        $bucketCounts[$bucket] = [int]$bucketCounts[$bucket] + 1
                    }

                    $company = [string](Get-ObjectValue $rankingItem "company" "")
                    if (-not [string]::IsNullOrWhiteSpace($company)) {
                        $companyKey = $company.ToLowerInvariant()
                        if (-not $companiesSeen.ContainsKey($companyKey)) {
                            $companiesSeen[$companyKey] = $company
                        }
                    }

                    $effectiveDate = Convert-ToDateOrNull (Get-ObjectValue $rankingItem "effectiveDate")
                    if ($null -eq $effectiveDate) {
                        $effectiveDateMissing += 1
                    }
                    else {
                        if ($null -ne $windowDateFrom -and $null -ne $windowDateTo) {
                            if ($effectiveDate -lt $windowDateFrom -or $effectiveDate -gt $windowDateTo) {
                                $effectiveDateOutOfWindow += 1
                            }
                        }

                        $ageDays = [int](($todayDate - $effectiveDate).TotalDays)
                        if ($ageDays -ge 0) {
                            $effectiveAgeValues.Add([double]$ageDays)
                        }
                    }
                }

                $scoreSummary = Get-NumericSummary -Values $scoreValues.ToArray()
                $aiConfidenceSummary = Get-NumericSummary -Values $aiConfidenceValues.ToArray()
                $scoreDeltaSummary = Get-NumericSummary -Values $scoreDeltaValues.ToArray()
                $effectiveAgeSummary = Get-NumericSummary -Values $effectiveAgeValues.ToArray()

                $bucketSummaryParts = New-Object System.Collections.Generic.List[string]
                foreach ($bucketKey in ($bucketCounts.Keys | Sort-Object)) {
                    $bucketSummaryParts.Add("$bucketKey=$($bucketCounts[$bucketKey])")
                }

                $bucketSummary = if ($bucketSummaryParts.Count -gt 0) {
                    $bucketSummaryParts -join ", "
                }
                else {
                    "n/a"
                }

                $uniqueCompaniesCount = $companiesSeen.Count
                $aiCoverageText = Format-Ratio $aiScoreValues.Count $rankingCount
                $aiConfidenceAverageText = "n/a"
                if ($aiConfidenceSummary) {
                    $aiConfidenceAverageText = [string]$aiConfidenceSummary.Avg
                }

                $deterministicToFinalDeltaAvgText = "n/a"
                if ($scoreDeltaSummary) {
                    $deterministicToFinalDeltaAvgText = [string]$scoreDeltaSummary.Avg
                }

                $averageAgeDaysText = "n/a"
                if ($effectiveAgeSummary) {
                    $averageAgeDaysText = [string]$effectiveAgeSummary.Avg
                }

                if ($scoreSummary) {
                    Add-Ok (
                        "Ranking score distribution: min={0} p50={1} avg={2} p90={3} max={4} stdDev={5}" -f
                        $scoreSummary.Min,
                        $scoreSummary.Median,
                        $scoreSummary.Avg,
                        $scoreSummary.P90,
                        $scoreSummary.Max,
                        $scoreSummary.StdDev
                    )
                }

                Add-Ok "Ranking diversity: uniqueCompanies=$uniqueCompaniesCount bucketDistribution=$bucketSummary"

                Add-Ok (
                    "Ranking AI coverage: aiScoreCoverage={0} aiConfidenceAvg={1} deterministicToFinalDeltaAvg={2}" -f
                    $aiCoverageText,
                    $aiConfidenceAverageText,
                    $deterministicToFinalDeltaAvgText
                )

                Add-Ok (
                    "Ranking freshness: missingEffectiveDate={0} outOfWindow={1} averageAgeDays={2}" -f
                    $effectiveDateMissing,
                    $effectiveDateOutOfWindow,
                    $averageAgeDaysText
                )

                if ($sortViolations -gt 0) {
                    Add-Failure "Ranking order has score monotonicity violations ($sortViolations)"
                }

                if ($effectiveDateOutOfWindow -gt 0) {
                    Add-Failure "Ranking returned jobs outside the requested effective-date window (count=$effectiveDateOutOfWindow)"
                }

                if ($rankingCount -lt 10) {
                    Add-Warning "Ranking sample is small (items=$rankingCount); quality metrics may be noisy"
                }

                if ($uniqueCompaniesCount -lt 3 -and $rankingCount -ge 10) {
                    Add-Warning "Ranking has low company diversity for this sample (uniqueCompanies=$uniqueCompaniesCount)"
                }

                if ($scoreSummary -and $scoreSummary.StdDev -lt 4 -and $rankingCount -ge 15) {
                    Add-Warning "Ranking score spread is narrow (stdDev=$($scoreSummary.StdDev)); ordering may be weak"
                }

                if ($effectiveDateMissing -gt 0) {
                    Add-Warning "Some ranking items are missing effectiveDate (count=$effectiveDateMissing)"
                }

                if ($effectiveAgeSummary -and $effectiveAgeSummary.Max -gt ($safeRankingDays + 2)) {
                    Add-Warning "Ranking includes stale jobs relative to requested daysRange (maxAgeDays=$($effectiveAgeSummary.Max) requestedDays=$safeRankingDays)"
                }

                $aiCoverageRate = if ($rankingCount -gt 0) {
                    [double]$aiScoreValues.Count / [double]$rankingCount
                }
                else {
                    0.0
                }

                if ($aiCoverageRate -lt 0.1 -and $rankingCount -ge 15) {
                    Add-Warning "Low AI-score coverage in ranking sample ($([Math]::Round($aiCoverageRate * 100.0, 1))%)"
                }

                Test-RankingScoreConsistency -RankingItems $rankingItems -AuthToken $AuthToken
            }
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
        elseif (($createUserResponse.StatusCode -eq 400) -and [string]::IsNullOrWhiteSpace($createMessage)) {
            Add-Warning "User creation returned HTTP 400 without detail; proceeding to login with provided credentials"
        }
        else {
            Add-Failure "Unable to create smoke user (HTTP $($createUserResponse.StatusCode)): $createMessage"
        }
    }
}

$userToken = $null
$userRole = ""
if ($script:Failures.Count -eq 0) {
    Write-Step "User login"
    $userToken = Login-And-GetToken -Email $UserEmail -Password $UserPassword -Label "Smoke user"
}

if ($script:Failures.Count -eq 0 -and $userToken) {
    Test-UserPipelineFlow -AuthToken $userToken

    $userMeResponse = Invoke-ApiRequest -Method "GET" -Path "/users/me" -AuthCookieToken $userToken
    if ($userMeResponse.Ok) {
        $userData = Get-ObjectValue (Get-ObjectValue $userMeResponse.Json "data") "user"
        $userRole = [string](Get-ObjectValue $userData "role" "")
    }
}

if ([string]::IsNullOrWhiteSpace($AdminEmail) -or [string]::IsNullOrWhiteSpace($AdminPassword)) {
    if ($script:Failures.Count -eq 0 -and $userToken -and $userRole.ToUpperInvariant() -eq "ADMIN") {
        Add-Ok "Admin credentials not provided; using smoke user token because role is ADMIN"
        Test-AdminPipelineFlow -AuthToken $userToken
    }
    else {
        Add-Warning "Admin credentials were not provided; admin endpoints were skipped"
    }
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
