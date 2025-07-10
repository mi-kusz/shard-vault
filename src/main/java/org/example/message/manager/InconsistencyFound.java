package org.example.message.manager;

import java.util.List;

public record InconsistencyFound(int shardId, List<Byte> correctData)
{
}
