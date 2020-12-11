Param (    
    [string] $TopicName,
    [System.Int32] $PartitionCount = 1,
    [System.Int32] $ReplicationFactor = 1
)

if ($TopicName -eq "")
{
    Write-Error -Message "Topic must have a name" -Category InvalidArgument;
}
else
{
    $createTopicCommand = "docker-compose exec broker kafka-topics --create --topic '$TopicName' --bootstrap-server broker:9092 --replication-factor '$ReplicationFactor' --partitions '$PartitionCount'";
    $runningContainerList = Invoke-Expression -Command "docker ps" -ErrorAction Stop;
    $isKafkaRunning = $runningContainerList | Select-String -Pattern "confluentinc/cp-*" -Quiet;
    
    if (!$isKafkaRunning)
    {
        Write-Host "Kafka is not running...starting Kafka containers";
        Invoke-Expression -Command "docker-compose up -d";
    }
    
    Write-Host "Kafka is running! Creating topic...";
    $topicCreationResult = Invoke-Expression $createTopicCommand;
    Write-Host $topicCreationResult;    
}
