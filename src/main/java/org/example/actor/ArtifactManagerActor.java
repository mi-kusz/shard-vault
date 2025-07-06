package org.example.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.example.message.manager.DeleteArtifactFromManager;
import org.example.message.manager.GetArtifactFromManager;
import org.example.message.warehouse.AddShardToWarehouse;
import org.example.message.warehouse.DeleteShardFromWarehouse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ArtifactManagerActor extends AbstractActor
{
    private final String artifactId;
    private final Multimap<Integer, ActorRef> dataWarehouses = ArrayListMultimap.create();

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props(String artifactId, List<Byte> data, List<ActorRef> warehouses, int numberOfShards, int replicaCount)
    {
        return Props.create(ArtifactManagerActor.class, () -> new ArtifactManagerActor(artifactId, data, warehouses, numberOfShards, replicaCount));
    }

    public ArtifactManagerActor(String artifactId, List<Byte> data, List<ActorRef> warehouses, int numberOfShards, int replicaCount)
    {
        Preconditions.checkArgument(replicaCount % 2 == 1, "Replica count should be odd");
        Preconditions.checkArgument(replicaCount <= warehouses.size(), "Replica count must be lesser or equal to the number of warehouses");

        this.artifactId = artifactId;

        int shardSize = (int) Math.ceil((double) data.size() / numberOfShards);

        warehouses = new ArrayList<>(warehouses);

        log.info("Creating ArtifactManager [" + artifactId + "]. Data length: " + data.size());

        for (int shardId = 0; shardId < numberOfShards; ++shardId)
        {
            int startIndex = shardId * shardSize;
            int endIndex = Math.min((shardId + 1) * shardSize, data.size());

            log.info("Shard [" + shardId + "] range: [" + startIndex + ":" + endIndex + "]");

            List<Byte> shard = data.subList(startIndex, endIndex);

            Collections.shuffle(warehouses);

            for (ActorRef warehouse : warehouses.subList(0, replicaCount))
            {
                dataWarehouses.put(shardId, warehouse);
                warehouse.tell(new AddShardToWarehouse(artifactId, shardId, shard), getSelf());
            }
        }
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
        ActorRef collector = getContext().actorOf(ShardCollectorActor.props(artifactId, dataWarehouses, getSender()), "artifactCollector-" + artifactId);
    }

    private void deleteArtifact(DeleteArtifactFromManager message)
    {
        for (var entry : dataWarehouses.entries())
        {
            int shardId = entry.getKey();
            ActorRef warehouse = entry.getValue();

            warehouse.tell(new DeleteShardFromWarehouse(artifactId, shardId), getSelf());
        }
    }
}
