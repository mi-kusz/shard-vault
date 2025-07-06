package org.example.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
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

    public static Props props(String artifactId, Multimap<Integer, ActorRef> warehouses)
    {
        return Props.create(ShardCollectorActor.class, () -> new ShardCollectorActor(artifactId, warehouses));
    }

    public ShardCollectorActor(String artifactId, Multimap<Integer, ActorRef> warehouses)
    {
        this.artifactId = artifactId;
        this.warehouses = warehouses;
        this.shards = new ArrayList<>(warehouses.keys().size());

        this.shardsLeft = new HashSet<>(warehouses.keys());

        this.originalSender = getSender();
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
        askForShards();
    }

    private void askForShards()
    {
        for (int shardId : warehouses.keys())
        {
            for (ActorRef warehouse : warehouses.get(shardId))
            {
                warehouse.tell(new GetShardFromWarehouse(artifactId, shardId), getSelf());
            }
        }
    }

    private void buildArtifact(ShardResponseFromWarehouse message)
    {
        buildArtifact(message.shardId(), message.data());
    }

    private void buildArtifact(Integer shardId, List<Byte> data)
    {
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
        getContext().stop(getSelf());
    }
}
