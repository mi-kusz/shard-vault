package org.example.actor;

import akka.actor.AbstractActor;
import akka.actor.Props;
import org.example.message.warehouse.ArtifactNotFoundInWarehouse;
import org.example.message.warehouse.ShardNotFoundInWarehouse;
import org.example.message.warehouse.AddShardToWarehouse;
import org.example.message.warehouse.DeleteShardFromWarehouse;
import org.example.message.warehouse.GetShardFromWarehouse;
import org.example.message.warehouse.ShardResponseFromWarehouse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WarehouseActor extends AbstractActor
{
    private final int warehouseId;
    private final Map<String, Map<Integer, List<Byte>>> warehouse = new HashMap<>();

    public static Props props(int warehouseId)
    {
        return Props.create(WarehouseActor.class, () -> new WarehouseActor(warehouseId));
    }

    public WarehouseActor(int warehouseId)
    {
        this.warehouseId = warehouseId;
    }

    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match(AddShardToWarehouse.class, this::addShard)
                .match(DeleteShardFromWarehouse.class, this::deleteShard)
                .match(GetShardFromWarehouse.class, this::getShard)
                .build();
    }

    private void addShard(String artifactId, Integer shardId, List<Byte> data)
    {
        if (!warehouse.containsKey(artifactId))
        {
            warehouse.put(artifactId, new HashMap<>());
        }

        warehouse.get(artifactId).put(shardId, data);
    }

    private void addShard(AddShardToWarehouse message)
    {
        addShard(message.artifactId(), message.shardId(), message.data());
    }

    private void deleteShard(String artifactId, Integer shardId)
    {
        if (warehouse.containsKey(artifactId))
        {
            var shards = warehouse.get(artifactId);

            if (shards.containsKey(shardId))
            {
                warehouse.get(artifactId).remove(shardId);
            }
            else
            {
                getSender().tell(new ShardNotFoundInWarehouse(artifactId, shardId), getSelf());
            }

        }
        else
        {
            getSender().tell(new ArtifactNotFoundInWarehouse(artifactId), getSelf());
        }
    }

    private void deleteShard(DeleteShardFromWarehouse message)
    {
        deleteShard(message.artifactId(), message.shardId());
    }

    private void getShard(String artifactId, Integer shardId)
    {
        if (warehouse.containsKey(artifactId))
        {
            var shards = warehouse.get(artifactId);

            if (shards.containsKey(shardId))
            {
                ShardResponseFromWarehouse shard = new ShardResponseFromWarehouse(artifactId, shardId, warehouse.get(artifactId).get(shardId));

                getSender().tell(shard, getSelf());
            }
            else
            {
                getSender().tell(new ShardNotFoundInWarehouse(artifactId, shardId), getSelf());
            }

        }
        else
        {
            getSender().tell(new ArtifactNotFoundInWarehouse(artifactId), getSelf());
        }
    }

    private void getShard(GetShardFromWarehouse message)
    {
        getShard(message.artifactId(), message.shardId());
    }
}
