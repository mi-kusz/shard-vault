package org.example.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.example.message.replicator.Replicate;
import org.example.message.warehouse.AddShardToWarehouse;

import java.util.List;

public class ShardReplicatorActor extends AbstractActor
{
    private final String artifactId;
    private final int shardId;
    private final List<Byte> data;

    private final List<ActorRef> warehouses;

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props(String artifactId, int shardId, List<Byte> data, List<ActorRef> warehouses)
    {
        return Props.create(ShardReplicatorActor.class, () -> new ShardReplicatorActor(artifactId, shardId, data, warehouses));
    }

    public ShardReplicatorActor(String artifactId, int shardId, List<Byte> data, List<ActorRef> warehouses)
    {
        this.artifactId = artifactId;
        this.shardId = shardId;
        this.data = data;
        this.warehouses = warehouses;
    }

    @Override
    public void preStart()
    {
        getSelf().tell(new Replicate(), getSelf());
    }

    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match(Replicate.class, this::replicate)
                .build();
    }

    private void replicate(Replicate message)
    {
        for (ActorRef warehouse : warehouses)
        {
            warehouse.tell(new AddShardToWarehouse(artifactId, shardId, data), getSelf());
        }

        log.info("Replicated shard [" + shardId + "] of artifact [" + artifactId + "] to " + warehouses.size() + " warehouses");

        getContext().stop(getSelf());
    }
}
