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
import org.example.message.vault.*;

import java.util.*;

public class VaultManagerActor extends AbstractActor
{
    private final int numberOfShards;
    private final int replicaCount;
    private final int initialWarehouses;

    private int nextWarehouseId = 0;
    private final Map<String, ActorRef> artifactManagers = new HashMap<>();
    private final Map<Integer, ActorRef> warehouses = new HashMap<>();

    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props(int numberOfShards, int replicaCount, int initialWarehouses)
    {
        return Props.create(VaultManagerActor.class, () -> new VaultManagerActor(numberOfShards, replicaCount, initialWarehouses));
    }

    public VaultManagerActor(int numberOfShards, int replicaCount, int initialWarehouses)
    {
        Preconditions.checkArgument(initialWarehouses >= 1, "Warehouses number must be greater or equal 1");
        Preconditions.checkArgument(replicaCount % 2 == 1, "Replica count should be odd");
        Preconditions.checkArgument(replicaCount <= initialWarehouses, "Replica count must be lesser or equal to the number of warehouses");

        this.numberOfShards = numberOfShards;
        this.replicaCount = replicaCount;
        this.initialWarehouses = initialWarehouses;
    }

    @Override
    public void preStart()
    {
        for (int i = 0; i < initialWarehouses; ++i)
        {
            int id = nextWarehouseId++;
            warehouses.put(id, getContext().actorOf(WarehouseActor.props(id), "Warehouse-" + id));
        }

        log.info("Created VaultManager");
    }

    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match(AddArtifactToVault.class, this::addArtifact)
                .match(GetArtifactFromVault.class, this::getArtifact)
                .match(DeleteArtifactFromVault.class, this::deleteArtifact)
                .match(AddWarehouseToVault.class, this::addWarehouseToVault)
                .build();
    }

    private void addArtifact(AddArtifactToVault message)
    {
        String artifactId = message.artifactId();
        List<Byte> data = message.data();

        if (artifactManagers.containsKey(artifactId))
        {
            log.warning("Artifact [" + artifactId + "] already exists");
            getSender().tell(new ArtifactAlreadyExistsInVault(artifactId), getSelf());
        }
        else
        {
            Multimap<Integer, ActorRef> assignedWarehouses = assignWarehouses();

            System.out.println(assignedWarehouses);

            ActorRef artifactManager = getContext().actorOf(ArtifactManagerActor.props(artifactId, data,
                    assignedWarehouses, numberOfShards, replicaCount), "ArtifactManager-" + artifactId + "-" + UUID.randomUUID());
            artifactManagers.put(artifactId, artifactManager);
        }
    }

    private void getArtifact(GetArtifactFromVault message)
    {
        String artifactId = message.artifactId();

        if (artifactManagers.containsKey(artifactId))
        {
            ActorRef artifactManger = artifactManagers.get(artifactId);
            artifactManger.tell(new GetArtifactFromManager(), getSender());
        }
        else
        {
            log.warning("Artifact [" + artifactId + "] not found in the vault");
            getSender().tell(new ArtifactNotFoundInVault(artifactId), getSelf());
        }
    }

    private void deleteArtifact(DeleteArtifactFromVault message)
    {
        String artifactId = message.artifactId();

        if (artifactManagers.containsKey(artifactId))
        {
            ActorRef artifactManager = artifactManagers.get(artifactId);
            artifactManagers.remove(artifactId);
            artifactManager.tell(new DeleteArtifactFromManager(), getSelf());
        }
        else
        {
            log.warning("Artifact [" + artifactId + "] not found in the vault");
            getSender().tell(new ArtifactNotFoundInVault(artifactId), getSelf());
        }
    }

    private void addWarehouseToVault(AddWarehouseToVault message)
    {
        int id = nextWarehouseId++;
        warehouses.put(id, getContext().actorOf(WarehouseActor.props(id), "Warehouse-" + id));

        log.info("Added warehouse [" + id + "] to vault");
    }

    private Multimap<Integer, ActorRef> assignWarehouses()
    {
        ArrayList<ActorRef> warehousesShuffled = new ArrayList<>(warehouses.values());
        Multimap<Integer, ActorRef> warehousesAssignment = ArrayListMultimap.create();

        for (int shardId = 0; shardId < numberOfShards; ++shardId)
        {
            Collections.shuffle(warehousesShuffled);
            for (ActorRef warehouse : warehousesShuffled.subList(0, replicaCount))
            {
                warehousesAssignment.put(shardId, warehouse);
            }
        }

        return warehousesAssignment;
    }
}
