package org.example.message.warehouse;

import java.util.List;

public record ShardResponseFromWarehouse(String artifactId, Integer shardId, List<Byte> data)
{
}
