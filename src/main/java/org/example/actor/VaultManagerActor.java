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
import org.example.message.warehouse.NumberOfStoredShards;

import java.util.*;

public class VaultManagerActor extends AbstractActor
{
    private final int numberOfShards;
    private final int replicaCount;
    private final int initialWarehouses;

    private int nextWarehouseId = 0;
    private final Map<String, ActorRef> artifactManagers = new HashMap<>();
    private final Map<Integer, ActorRef> warehouses = new HashMap<>();
    private final Map<Integer, Integer> warehouseSizes = new HashMap<>();

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
            addWarehouse(id);
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
                .match(NumberOfStoredShards.class, this::updateWarehouseSizes)
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

            ActorRef artifactManager = getContext().actorOf(ArtifactManagerActor.props(artifactId, data,
                    assignedWarehouses, numberOfShards), "ArtifactManager-" + artifactId + "-" + UUID.randomUUID());
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
        addWarehouse(id);

        log.info("Added warehouse [" + id + "] to vault");
    }

    private Multimap<Integer, ActorRef> assignWarehouses()
    {
        Multimap<Integer, ActorRef> warehousesAssignment = ArrayListMultimap.create();

        Map<Integer, Integer> expectedSizesOfWarehouses = new HashMap<>(warehouseSizes);

        for (int shardId = 0; shardId < numberOfShards; ++shardId)
        {
            List<Map.Entry<Integer, Integer>> sorted = expectedSizesOfWarehouses.entrySet().stream().sorted(Map.Entry.comparingByValue()).toList();

            for (var entry : sorted.subList(0, replicaCount))
            {
                int warehouseId = entry.getKey();
                warehousesAssignment.put(shardId, warehouses.get(warehouseId));
                expectedSizesOfWarehouses.merge(warehouseId, 1, Integer::sum);
            }
        }

        return warehousesAssignment;
    }

    private void addWarehouse(int id)
    {
        warehouses.put(id, getContext().actorOf(WarehouseActor.props(id, getSelf()), "Warehouse-" + id));
        warehouseSizes.put(id, 0);
    }

    private void updateWarehouseSizes(NumberOfStoredShards message)
    {
        int warehouseId = message.warehouseId();
        int numberOfShards = message.numberOfStoredShards();

        warehouseSizes.put(warehouseId, numberOfShards);

        log.info("Updated warehouse [" + warehouseId + "] size to: " + numberOfShards);
    }
}
