package org.example.message.warehouse;

import com.google.common.collect.Multimap;

public record StatusResponseOfWarehouse(Integer warehouseId, Multimap<String, Integer> shards)
{
}
