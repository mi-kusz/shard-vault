package org.example.actor;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import org.example.message.manager.DeleteArtifactFromManager;
import org.example.message.manager.GetArtifactFromManager;
import org.example.message.vault.AddArtifactToVault;
import org.example.message.vault.ArtifactNotFoundInVault;
import org.example.message.vault.DeleteArtifactFromVault;
import org.example.message.vault.GetArtifactFromVault;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VaultManagerActor extends AbstractActor
{
    private final int numberOfShards;
    private final int replicaCount;

    private int nextWarehouseId = 0;
    private final Map<String, ActorRef> artifactManagers = new HashMap<>();
    private final Map<Integer, ActorRef> warehouses = new HashMap<>();

    public static Props props(int numberOfShards, int replicaCount, int initialWarehouses)
    {
        return Props.create(VaultManagerActor.class, () -> new VaultManagerActor(numberOfShards, replicaCount, initialWarehouses));
    }

    public VaultManagerActor(int numberOfShards, int replicaCount, int initialWarehouses)
    {
        this.numberOfShards = numberOfShards;
        this.replicaCount = replicaCount;

        for (int i = 0; i < initialWarehouses; ++i)
        {
            int id = nextWarehouseId++;
            warehouses.put(id, getContext().actorOf(WarehouseActor.props(id)));
        }
    }

    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match(AddArtifactToVault.class, this::addArtifact)
                .match(GetArtifactFromVault.class, this::getArtifact)
                .match(DeleteArtifactFromVault.class, this::deleteArtifact)
                .build();
    }

    private void addArtifact(String artifactId, List<Byte> data)
    {
        ActorRef artifactManager = getContext().actorOf(ArtifactManagerActor.props(artifactId, data, warehouses.values().stream().toList(), numberOfShards, replicaCount));
        artifactManagers.put(artifactId, artifactManager);
    }

    private void addArtifact(AddArtifactToVault message)
    {
        addArtifact(message.artifactId(), message.data());
    }

    private void getArtifact(String artifactId)
    {
        if (artifactManagers.containsKey(artifactId))
        {
            ActorRef artifactManger = artifactManagers.get(artifactId);
            artifactManger.tell(new GetArtifactFromManager(), getSender());
        }
        else
        {
            getSender().tell(new ArtifactNotFoundInVault(artifactId), getSelf());
        }
    }

    private void getArtifact(GetArtifactFromVault message)
    {
        getArtifact(message.artifactId());
    }

    private void deleteArtifact(String artifactId)
    {
        if (artifactManagers.containsKey(artifactId))
        {
            ActorRef artifactManger = artifactManagers.get(artifactId);
            artifactManger.tell(new DeleteArtifactFromManager(), getSender());
        }
        else
        {
            getSender().tell(new ArtifactNotFoundInVault(artifactId), getSelf());
        }
    }

    private void deleteArtifact(DeleteArtifactFromVault message)
    {
        deleteArtifact(message.artifactId());
    }
}
