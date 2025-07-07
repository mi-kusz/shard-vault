package org.example.actor;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.example.message.warehouse.*;

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
                .match(GetStatusOfWarehouse.class, this::getStatus)
                .build();
    }

    private void addShard(AddShardToWarehouse message)
    {
        String artifactId = message.artifactId();
        int shardId = message.shardId();
        List<Byte> data = message.data();

        if (!warehouse.containsKey(artifactId))
        {
            warehouse.put(artifactId, new HashMap<>());
            log.info("Created a map for storing shards of [" + artifactId + "]");
        }

        warehouse.get(artifactId).put(shardId, data);
        log.info("Stored shard [" + shardId + "] of artifact [" + artifactId + "]");
    }

    private void deleteShard(DeleteShardFromWarehouse message)
    {
        String artifactId = message.artifactId();
        int shardId = message.shardId();

        if (warehouse.containsKey(artifactId))
        {
            var shards = warehouse.get(artifactId);

            if (shards.containsKey(shardId))
            {
                warehouse.get(artifactId).remove(shardId);
                log.info("Deleted shard [" + shardId + "] of artifact [" + artifactId + "]");

                if (warehouse.get(artifactId).isEmpty())
                {
                    warehouse.remove(artifactId);
                    log.info("Removed a map for storing shards of [" + artifactId + "]");
                }
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
        String artifactId = message.artifactId();
        int shardId = message.shardId();

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

    private void getStatus(GetStatusOfWarehouse message)
    {
        Multimap<String, Integer> shards = ArrayListMultimap.create();

        for (var entry : warehouse.entrySet())
        {
            String artifactId = entry.getKey();

            for (int shard : entry.getValue().keySet())
            {
                shards.put(artifactId, shard);
            }
        }

        getSender().tell(new StatusResponseOfWarehouse(warehouseId, shards), getSelf());
    }
}
