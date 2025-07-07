package org.example.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.google.common.collect.Multimap;
import org.example.message.TimeoutMessage;
import org.example.message.collector.ArtifactResponseFromCollector;
import org.example.message.collector.CannotRecoverArtifact;
import org.example.message.collector.CollectShardsForCollector;
import org.example.message.warehouse.GetShardFromWarehouse;
import org.example.message.warehouse.ShardResponseFromWarehouse;

import java.time.Duration;
import java.util.*;

public class ShardCollectorActor extends AbstractActor
{
    private final String artifactId;
    private final Multimap<Integer, ActorRef> warehouses;
    private final List<List<Byte>> shards;
    private int expectedResponses;
    private int receivedResponses = 0;

    private final ActorRef originalSender;
    private Cancellable timeout;

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

        this.originalSender = originalSender;

        log.info("Created artifact collector of artifact [" + artifactId + "]");
    }

    @Override
    public void preStart()
    {
        getSelf().tell(new CollectShardsForCollector(), getSelf());

        timeout = getContext().getSystem().scheduler().scheduleOnce(
                Duration.ofSeconds(1),
                getSelf(),
                new TimeoutMessage(),
                getContext().getDispatcher(),
                getSelf()
        );
    }

    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match(CollectShardsForCollector.class, this::askForShards)
                .match(ShardResponseFromWarehouse.class, this::buildArtifact)
                .match(TimeoutMessage.class, this::timeout)
                //.match(ArtifactNotFoundInWarehouse.class)
                //.match(ShardNotFoundInWarehouse.class)
                .build();
    }

    private void askForShards(CollectShardsForCollector message)
    {
        int messagesSent = 0;
        for (int shardId : warehouses.keySet())
        {
            for (ActorRef warehouse : warehouses.get(shardId))
            {
                warehouse.tell(new GetShardFromWarehouse(artifactId, shardId), getSelf());
                ++messagesSent;
                log.info("Asked for shard [" + shardId + "]");
            }
        }

        this.expectedResponses = messagesSent;
    }

    private void buildArtifact(ShardResponseFromWarehouse message)
    {
        ++receivedResponses;

        int shardId = message.shardId();
        List<Byte> data = message.data();

        List<Byte> currentValue = shards.get(shardId);

        if (currentValue == null)
        {
            shards.set(shardId, data);
        }
        else if (!currentValue.equals(data))
        {
            // TODO: Quorum of shards
            // TODO: Replicate bad shards again (send message to ArtifactManagerActor)
            throw new IllegalArgumentException("Shards are not the same");
        }

        if (receivedResponses == expectedResponses)
        {
            finish();
        }
    }

    private void timeout(TimeoutMessage message)
    {
        finish();
    }

    private void finish()
    {
        if (!timeout.isCancelled())
        {
            timeout.cancel();
        }

        if (!shards.contains(null))
        {
            List<Byte> artifact = shards.stream().flatMap(Collection::stream).toList();
            originalSender.tell(new ArtifactResponseFromCollector(artifactId, artifact), getSelf());
            log.info("Sending artifact [" + artifactId + "] to client");
        }
        else
        {
            originalSender.tell(new CannotRecoverArtifact(artifactId), originalSender);
            log.error("Cannot rebuild tha artifact [" + artifactId + "]");
        }

        getContext().stop(getSelf());
    }
}
