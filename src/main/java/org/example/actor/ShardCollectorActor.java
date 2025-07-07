package org.example.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.google.common.collect.Multimap;
import org.example.message.collector.ArtifactResponseFromCollector;
import org.example.message.collector.CollectShardsForCollector;
import org.example.message.warehouse.GetShardFromWarehouse;
import org.example.message.warehouse.ShardResponseFromWarehouse;

import java.util.*;

public class ShardCollectorActor extends AbstractActor
{
    private final String artifactId;
    private final Multimap<Integer, ActorRef> warehouses;
    private final List<List<Byte>> shards;
    private final Set<Integer> shardsLeft;

    private final ActorRef originalSender;

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props(String artifactId, Multimap<Integer, ActorRef> warehouses, ActorRef originalSender)
    {
        return Props.create(ShardCollectorActor.class, () -> new ShardCollectorActor(artifactId, warehouses, originalSender));
    }

    public ShardCollectorActor(String artifactId, Multimap<Integer, ActorRef> warehouses, ActorRef originalSender)
    {
        this.artifactId = artifactId;
        this.warehouses = warehouses;

        int maxShardId = Collections.max(warehouses.keySet());
        this.shards = new ArrayList<>(Collections.nCopies(maxShardId + 1, null));
        this.shardsLeft = new HashSet<>(warehouses.keys());

        this.originalSender = originalSender;

        log.info("Created artifact collector of artifact [" + artifactId + "]");
    }

    @Override
    public void preStart()
    {
        getSelf().tell(new CollectShardsForCollector(), getSelf());
    }

    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match(CollectShardsForCollector.class, this::askForShards)
                .match(ShardResponseFromWarehouse.class, this::buildArtifact)
                .build();
    }

    private void askForShards(CollectShardsForCollector message)
    {
        for (int shardId : warehouses.keys())
        {
            for (ActorRef warehouse : warehouses.get(shardId))
            {
                warehouse.tell(new GetShardFromWarehouse(artifactId, shardId), getSelf());
                log.info("Asked for shard [" + shardId + "]");
            }
        }
    }

    private void buildArtifact(ShardResponseFromWarehouse message)
    {
        int shardId = message.shardId();
        List<Byte> data = message.data();

        List<Byte> currentValue = shards.get(shardId);

        if (currentValue == null)
        {
            shards.set(shardId, data);
            shardsLeft.remove(shardId);
        }
        else if (!currentValue.equals(data))
        {
            throw new IllegalArgumentException("Shards are not the same");
        }

        if (shardsLeft.isEmpty())
        {
            finish();
        }
    }

    private void finish()
    {
        List<Byte> artifact = shards.stream().flatMap(Collection::stream).toList();
        originalSender.tell(new ArtifactResponseFromCollector(artifactId, artifact), getSelf());
        log.info("Sending artifact [" + artifactId + "] to client");
        getContext().stop(getSelf());
    }
}
