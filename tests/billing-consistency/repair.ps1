param([ValidateRange(1,1000)][int]$Samples=3)
. "$PSScriptRoot/common.ps1"
Add-Type -AssemblyName System.Net.Http
$runId=[guid]::NewGuid().ToString()
$reportDir=Join-Path $RepoRoot "artifacts/billing-consistency/$runId"
New-Item -ItemType Directory -Force $reportDir | Out-Null
$rows=[Collections.Generic.List[object]]::new()
$handler=[Net.Http.HttpClientHandler]::new(); $handler.UseProxy=$false; $client=[Net.Http.HttpClient]::new($handler)
$client.Timeout=[TimeSpan]::FromSeconds(30)
$failure=$null
try {
    foreach ($index in 1..$Samples) {
        $id=[guid]::NewGuid().ToString()
        $mappingId=[guid]::NewGuid().ToString()
        $row=[ordered]@{operationId=$id;scenario='C05 Redis Lua denied after admission';faultTriggered=$false;repaired=$false;ledgerEventEpochMs=$null;detectedAt=$null;repairedAt=$null;eventToObservedRepairMs=$null;error=$null}
        try {
            $mapping=@{id=$mappingId;priority=1;request=@{method='POST';urlPath='/v1/chat/completions';bodyPatterns=@(@{matchesJsonPath=@{expression='$.model';equalTo=$id}})};response=@{status=200;fixedDelayMilliseconds=6000;headers=@{'Content-Type'='application/json'};jsonBody=@{id="billing-$id";choices=@(@{message=@{role='assistant';content='fixed billing fixture'};finish_reason='stop'});usage=@{prompt_tokens=28;completion_tokens=12;total_tokens=40}}}} | ConvertTo-Json -Depth 12
            Invoke-RestMethod -NoProxy 'http://127.0.0.1:28090/__admin/mappings' -Method Post -ContentType 'application/json' -Body $mapping | Out-Null
            $request=[Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Post,'http://127.0.0.1:28080/v1/chat/completions')
            $request.Headers.Add('Authorization','Bearer agentops-dev-key')
            foreach ($header in @('Idempotency-Key','X-AgentOps-Request-Id','X-AgentOps-Correlation-Id')) { $request.Headers.Add($header,$id) }
            $body=@{model=$id;stream=$false;max_tokens=128;messages=@(@{role='user';content='repair fixture'})} | ConvertTo-Json -Depth 5
            $request.Content=[Net.Http.StringContent]::new($body,[Text.Encoding]::UTF8,'application/json')
            $pending=$client.SendAsync($request)
            $deadline=[DateTime]::UtcNow.AddSeconds(4)
            do {
                $ready=Invoke-BillingSql "select count(*) from usage_reservation where request_id='$id' and status='RESERVED' and provider_started=1;"
                if ($ready -eq '1') { break }
                Start-Sleep -Milliseconds 100
            } while ([DateTime]::UtcNow -lt $deadline)
            if ($ready -ne '1') { throw 'Did not reach post-admission fault window.' }
            Invoke-BillingCompose exec -T redis redis-cli ACL SETUSER default -eval -evalsha | Out-Null
            $deadline=[DateTime]::UtcNow.AddSeconds(20)
            do {
                $eventMs=Invoke-BillingSql "select cast(unix_timestamp(l.occurred_at)*1000 as unsigned) from usage_ledger l join usage_reservation r on r.reservation_id=l.reservation_id where r.request_id='$id' and l.ledger_type='USAGE_ACTUAL';"
                if ($eventMs) { break }
                Start-Sleep -Milliseconds 200
            } while ([DateTime]::UtcNow -lt $deadline)
            if (-not $eventMs) { throw 'No committed actual-usage ledger after injected failure.' }
            $ledgerTotal=[long](Invoke-BillingSql 'select coalesce(sum(token_delta),0) from usage_ledger;')
            $consumed=[long](Invoke-BillingCompose exec -T redis redis-cli --raw HGET agentops:quota:project-fund-agent consumed)
            if ($ledgerTotal-$consumed -ne 40) { throw 'Expected exact 40-token Redis discrepancy was not observed.' }
            $row.faultTriggered=$true; $row.ledgerEventEpochMs=[long]$eventMs; $row.detectedAt=[DateTimeOffset]::UtcNow.ToString('o')
            Invoke-BillingCompose exec -T redis redis-cli ACL SETUSER default +eval +evalsha | Out-Null
            $deadline=[DateTime]::UtcNow.AddSeconds(45)
            do {
                $redisValues=@(Invoke-BillingCompose exec -T redis redis-cli --raw HMGET agentops:quota:project-fund-agent consumed reserved active)
                $applied=Invoke-BillingSql "select count(*) from usage_quota_task q join usage_reservation r on r.reservation_id=q.reservation_id where r.request_id='$id' and q.status='APPLIED';"
                if ([long]$redisValues[0] -eq $ledgerTotal -and [long]$redisValues[1] -eq 0 -and [long]$redisValues[2] -eq 0 -and $applied -eq '1') {
                    $now=[DateTimeOffset]::UtcNow
                    $row.repaired=$true; $row.repairedAt=$now.ToString('o'); $row.eventToObservedRepairMs=$now.ToUnixTimeMilliseconds()-[long]$eventMs
                    break
                }
                Start-Sleep -Milliseconds 200
            } while ([DateTime]::UtcNow -lt $deadline)
            if (-not $row.repaired) { throw 'Redis repair exceeded 45-second observation window.' }
            Write-Host "Repair sample $index PASS ($($row.eventToObservedRepairMs) ms, event-to-observation)"
        } catch { $row.error=$_.Exception.Message; throw } finally {
            Invoke-BillingCompose exec -T redis redis-cli ACL SETUSER default +eval +evalsha | Out-Null
            Invoke-RestMethod -NoProxy "http://127.0.0.1:28090/__admin/mappings/$mappingId" -Method Delete | Out-Null
            $rows.Add([pscustomobject]$row)
            if ($pending -and $pending.IsCompleted -and -not $pending.IsFaulted) { $pending.Result.Dispose() }
            if ($request) { $request.Dispose() }
        }
    }
} catch { $failure=$_.Exception.Message; throw } finally {
    $client.Dispose()
    $rows | Export-Csv (Join-Path $reportDir 'repairs.csv') -NoTypeInformation -Encoding utf8
    $valid=@($rows | Where-Object faultTriggered)
    $repaired=@($valid | Where-Object repaired)
    $times=@($repaired | ForEach-Object eventToObservedRepairMs | Sort-Object)
    $p95=if ($times.Count) { $times[[int][Math]::Ceiling(0.95*$times.Count)-1] } else { $null }
    @{runId=$runId;requestedSamples=$Samples;attempted=$rows.Count;validSamples=$valid.Count;repaired=$repaired.Count;invalidSamples=$rows.Count-$valid.Count;unrepaired=$valid.Count-$repaired.Count;observedP95Ms=$p95;error=$failure;timing='Ledger occurred_at (before commit) to observed Redis convergence and APPLIED task. Includes polling and docker exec overhead; not exact commit-to-repair latency.';pollSleepMs=200;recoveryDelayMs=10000;providerDelayMs=6000;claim='Functional fault verification; small sample is not a resume performance benchmark.'} | ConvertTo-Json -Depth 5 | Set-Content (Join-Path $reportDir 'summary.json') -Encoding utf8
    Write-Host "Report: $reportDir"
}



