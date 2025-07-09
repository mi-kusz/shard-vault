package org.example.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.google.common.collect.Multimap;
import org.example.message.manager.DeleteArtifactFromManager;
import org.example.message.manager.GetArtifactFromManager;
import org.example.message.warehouse.AddShardToWarehouse;
import org.example.message.warehouse.DeleteShardFromWarehouse;

import java.util.List;
import java.util.UUID;

public class ArtifactManagerActor extends AbstractActor
{
    private final String artifactId;
    private final Multimap<Integer, ActorRef> dataWarehouses;

    // Variables used only in preStart method (so ArtifactManager doesn't send messages from constructor)
    private List<Byte> _data;
    private final int _numberOfShards;

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props(String artifactId, List<Byte> data, Multimap<Integer, ActorRef> warehouses, int numberOfShards, int replicaCount)
    {
        return Props.create(ArtifactManagerActor.class, () -> new ArtifactManagerActor(artifactId, data, warehouses, numberOfShards, replicaCount));
    }

    public ArtifactManagerActor(String artifactId, List<Byte> data, Multimap<Integer, ActorRef> warehouses, int numberOfShards, int replicaCount)
    {
        this.artifactId = artifactId;

        dataWarehouses = warehouses;

        this._data = data;
        this._numberOfShards = numberOfShards;
    }

    @Override
    public void preStart()
    {
        int shardSize = _data.size() / _numberOfShards;

        for (int shardId = 0; shardId < _numberOfShards; ++shardId)
        {
            int startIndex = shardId * shardSize;
            int endIndex;

            if (shardId == _numberOfShards - 1)
            {
                endIndex = _data.size();
            }
            else
            {
                endIndex = (shardId + 1) * shardSize;
            }

            log.info("Shard [" + shardId + "] range: [" + startIndex + ":" + endIndex + "]");

            List<Byte> shard = _data.subList(startIndex, endIndex);

            for (ActorRef warehouse : dataWarehouses.get(shardId))
            {
                warehouse.tell(new AddShardToWarehouse(artifactId, shardId, shard), getSelf());
            }
        }

        log.info("Created ArtifactManager [" + artifactId + "]. Data length: " + _data.size());

        // Cleanup
        this._data = null;
    }

    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match(GetArtifactFromManager.class, this::getArtifact)
                .match(DeleteArtifactFromManager.class, this::deleteArtifact)
                .build();
    }

    private void getArtifact(GetArtifactFromManager message)
    {
        getContext().actorOf(ShardCollectorActor.props(artifactId, dataWarehouses, getSender()), "ArtifactCollector-" + artifactId + "-" + UUID.randomUUID());
    }

    private void deleteArtifact(DeleteArtifactFromManager message)
    {
        for (var entry : dataWarehouses.entries())
        {
            int shardId = entry.getKey();
            ActorRef warehouse = entry.getValue();

            warehouse.tell(new DeleteShardFromWarehouse(artifactId, shardId), getSelf());
        }
        dataWarehouses.clear();

        getContext().stop(getSelf());
    }
}
