package org.example.actor;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
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

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props(int warehouseId)
    {
        return Props.create(WarehouseActor.class, () -> new WarehouseActor(warehouseId));
    }

    public WarehouseActor(int warehouseId)
    {
        this.warehouseId = warehouseId;
        log.info("Created warehouse [" + warehouseId + "]");
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
            log.info("Created map for storing shards of [" + artifactId + "]");
        }

        warehouse.get(artifactId).put(shardId, data);
        log.info("Stored shard [" + shardId + "] of artifact [" + artifactId + "]");
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
                log.info("Deleted shard [" + shardId + "] of artifact [" + artifactId + "]");
            }
            else
            {
                getSender().tell(new ShardNotFoundInWarehouse(artifactId, shardId), getSelf());
                log.warning("Warehouse [" + warehouseId + "] doesn't store shard [" + shardId + "] of artifact [" + artifactId + "]");
            }

        }
        else
        {
            getSender().tell(new ArtifactNotFoundInWarehouse(artifactId), getSelf());
            log.warning("Warehouse [" + warehouseId + "] doesn't store artifact [" + artifactId + "]");
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
                log.info("Got shard [" + shardId + "] of artifact [" + artifactId + "]");
            }
            else
            {
                getSender().tell(new ShardNotFoundInWarehouse(artifactId, shardId), getSelf());
                log.warning("Warehouse [" + warehouseId + "] doesn't store shard [" + shardId + "] of artifact [" + artifactId + "]");
            }

        }
        else
        {
            getSender().tell(new ArtifactNotFoundInWarehouse(artifactId), getSelf());
            log.warning("Warehouse [" + warehouseId + "] doesn't store artifact [" + artifactId + "]");
        }
    }

    private void getShard(GetShardFromWarehouse message)
    {
        getShard(message.artifactId(), message.shardId());
    }
}
