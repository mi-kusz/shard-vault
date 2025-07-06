package org.example.message.warehouse;

import java.util.List;

public record AddShardToWarehouse(String artifactId, Integer shardId, List<Byte> data)
{
}
