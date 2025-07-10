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
import org.example.message.collector.CannotCompleteQuorum;
import org.example.message.collector.CannotRecoverArtifact;
import org.example.message.collector.CollectShardsForCollector;
import org.example.message.manager.InconsistencyFound;
import org.example.message.warehouse.ArtifactNotFoundInWarehouse;
import org.example.message.warehouse.GetShardFromWarehouse;
import org.example.message.warehouse.ShardNotFoundInWarehouse;
import org.example.message.warehouse.ShardResponseFromWarehouse;

import java.time.Duration;
import java.util.*;

public class ShardCollectorActor extends AbstractActor
{
    private final String artifactId;
    private final Multimap<Integer, ActorRef> warehouses;
    private final List<Map<List<Byte>, Integer>> shards;
    private final int numberOfShards;
    private int expectedResponses;
    private int receivedResponses = 0;

    private final ActorRef artifactManager;
    private final ActorRef originalSender;
    private Cancellable timeout;

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props(String artifactId, Multimap<Integer, ActorRef> warehouses, ActorRef artifactManager, ActorRef originalSender)
    {
        return Props.create(ShardCollectorActor.class, () -> new ShardCollectorActor(artifactId, warehouses, artifactManager, originalSender));
    }

    public ShardCollectorActor(String artifactId, Multimap<Integer, ActorRef> warehouses, ActorRef artifactManager, ActorRef originalSender)
    {
        this.artifactId = artifactId;
        this.warehouses = warehouses;

        numberOfShards = Collections.max(warehouses.keySet()) + 1;

        this.shards = new ArrayList<>(numberOfShards);

        for (int i = 0; i < numberOfShards; ++i)
        {
            this.shards.add(new HashMap<>());
        }

        this.artifactManager = artifactManager;
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
                .match(ArtifactNotFoundInWarehouse.class, this::artifactNotFound)
                .match(ShardNotFoundInWarehouse.class, this::shardNotFound)
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

        shards.get(shardId).merge(data, 1, Integer::sum);

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

        if (shards.stream().noneMatch(Map::isEmpty))
        {
            List<List<Byte>> result = new LinkedList<>();

            for (int shardId = 0; shardId < numberOfShards; ++shardId)
            {
                Map<List<Byte>, Integer> options = shards.get(shardId);

                int maxVotes = Collections.max(options.values());

                List<List<Byte>> candidates = options.entrySet().stream()
                        .filter(entry -> entry.getValue() == maxVotes)
                        .map(Map.Entry::getKey)
                        .toList();

                if (candidates.size() == 1)
                {
                    List<Byte> correctData = candidates.getFirst();
                    result.add(correctData);

                    if (options.size() > 1)
                    {
                        artifactManager.tell(new InconsistencyFound(shardId, correctData), getSelf());
                        log.info("Detected inconsistency of shard [" + shardId + "] of artifact [" + artifactId + "]");
                    }
                }
                else
                {
                    originalSender.tell(new CannotCompleteQuorum(artifactId), originalSender);
                    log.error("Cannot complete quorum. There are " + candidates.size() + " candidates with " + maxVotes + " votes");
                    getContext().stop(getSelf());
                    return;
                }

            }

            List<Byte> artifact = result.stream().flatMap(Collection::stream).toList();
            originalSender.tell(new ArtifactResponseFromCollector(artifactId, artifact), getSelf());
            log.info("Sending artifact [" + artifactId + "] to client");
        }
        else
        {
            originalSender.tell(new CannotRecoverArtifact(artifactId), originalSender);
            log.error("Cannot rebuild the artifact [" + artifactId + "]");
        }

        getContext().stop(getSelf());
    }

    private void artifactNotFound(ArtifactNotFoundInWarehouse message)
    {

    }

    private void shardNotFound(ShardNotFoundInWarehouse message)
    {

    }
}
