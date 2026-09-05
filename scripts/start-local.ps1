$ErrorActionPreference = 'Stop'

<#
.SYNOPSIS
Ensures a local Kafka topic exists with enough partitions for configured Worker concurrency.

.DESCRIPTION
Kafka can increase, but not safely decrease, a topic's partition count. This bootstrap step
keeps existing larger topics intact while making fresh local environments use four partitions.
#>
function Ensure-KafkaTopic([string]$Topic, [int]$RequiredPartitions) {
    & docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $Topic --partitions $RequiredPartitions --replication-factor 1
    if ($LASTEXITCODE -ne 0) { throw "Unable to create or inspect Kafka topic $Topic." }

    $description = @(& docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic $Topic)
    if ($LASTEXITCODE -ne 0) { throw "Unable to describe Kafka topic $Topic." }
    $descriptionText = $description -join [Environment]::NewLine
    if ($descriptionText -notmatch 'PartitionCount:\s*(?<count>\d+)') { throw "Unable to determine partition count for Kafka topic $Topic." }

    $actualPartitions = [int]$Matches['count']
    if ($actualPartitions -lt $RequiredPartitions) {
        # Expanding an existing local topic lets Worker consumers use the configured parallelism.
        & docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --alter --topic $Topic --partitions $RequiredPartitions
        if ($LASTEXITCODE -ne 0) { throw "Unable to expand Kafka topic $Topic to $RequiredPartitions partitions." }
    } elseif ($actualPartitions -gt $RequiredPartitions) {
        Write-Host "Kafka topic $Topic already has $actualPartitions partitions; retaining the larger count."
    }
}

# Docker health checks make downstream demos wait for real readiness instead of an arbitrary sleep.
docker compose up -d --wait
Ensure-KafkaTopic 'agentops.usage.ledger.v1' 4
Ensure-KafkaTopic 'agentops.eval.case.v1' 4
docker compose ps
Write-Host 'Infrastructure started. Run server and worker in separate terminals with scripts/run-server.ps1 and scripts/run-worker.ps1.'
