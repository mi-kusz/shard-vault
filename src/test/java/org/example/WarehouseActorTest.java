package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.testkit.TestKit;
import akka.testkit.TestProbe;
import akka.util.Timeout;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.example.actor.WarehouseActor;
import org.example.message.warehouse.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;
import scala.jdk.javaapi.FutureConverters;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class WarehouseActorTest
{
    private ActorSystem system;
    private TestProbe vaultProbe;
    private ActorRef warehouse;
    private final int warehouseId = 0;

    @BeforeEach
    public void setup()
    {
        system = ActorSystem.create("TestSystem");
        vaultProbe = new TestProbe(system);
        warehouse = system.actorOf(WarehouseActor.props(warehouseId, vaultProbe.ref()));
    }

    @AfterEach
    public void cleanup()
    {
        TestKit.shutdownActorSystem(system, Duration.create(5, TimeUnit.SECONDS), false);
    }

    @Test
    public void testAddAndRetrieveShard() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";
        int shardId = 0;
        List<Byte> data = Collections.nCopies(5, (byte) 1);

        warehouse.tell(new AddShardToWarehouse(artifactId, shardId, data), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(warehouse, new GetShardFromWarehouse(artifactId, shardId),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ShardResponseFromWarehouse.class, response);
            ShardResponseFromWarehouse shardResponse = (ShardResponseFromWarehouse) response;
            assertEquals(artifactId, shardResponse.artifactId());
            assertEquals(shardId, shardResponse.shardId());
            assertEquals(data, shardResponse.data());
        }};
    }

    @Test
    public void testRetrieveNonExistingShard() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";
        int shardId = 1;
        List<Byte> data = Collections.nCopies(5, (byte) 1);

        warehouse.tell(new AddShardToWarehouse(artifactId, 0, data), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(warehouse, new GetShardFromWarehouse(artifactId, shardId),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ShardNotFoundInWarehouse.class, response);
            ShardNotFoundInWarehouse shardResponse = (ShardNotFoundInWarehouse) response;
            assertEquals(artifactId, shardResponse.artifactId());
            assertEquals(shardId, shardResponse.shardId());
        }};
    }

    @Test
    public void testRetrieveNonExistingArtifact() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(warehouse, new GetShardFromWarehouse(artifactId, 0),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ArtifactNotFoundInWarehouse.class, response);
            ArtifactNotFoundInWarehouse artifactResponse = (ArtifactNotFoundInWarehouse) response;
            assertEquals(artifactId, artifactResponse.artifactId());
        }};
    }

    @Test
    public void testDeleteNonExistingShard() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";
        int shardId = 1;
        List<Byte> data = Collections.nCopies(5, (byte) 1);

        warehouse.tell(new AddShardToWarehouse(artifactId, 0, data), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(warehouse, new DeleteShardFromWarehouse(artifactId, shardId),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ShardNotFoundInWarehouse.class, response);
            ShardNotFoundInWarehouse shardResponse = (ShardNotFoundInWarehouse) response;
            assertEquals(artifactId, shardResponse.artifactId());
            assertEquals(shardId, shardResponse.shardId());
        }};
    }

    @Test
    public void testDeleteNonExistingArtifact() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(warehouse, new DeleteShardFromWarehouse(artifactId, 0),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ArtifactNotFoundInWarehouse.class, response);
            ArtifactNotFoundInWarehouse artifactResponse = (ArtifactNotFoundInWarehouse) response;
            assertEquals(artifactId, artifactResponse.artifactId());
        }};
    }

    @Test
    public void testWarehouseStatus() throws ExecutionException, InterruptedException
    {
        String artifactId1 = "ArtifactName1";
        int shardId1 = 4;
        List<Byte> data1 = Collections.nCopies(10, (byte) 4);

        String artifactId2 = "ArtifactName2";
        int shardId2 = 101;
        List<Byte> data2 = Collections.nCopies(10, (byte) 101);

        warehouse.tell(new AddShardToWarehouse(artifactId1, shardId1, data1), ActorRef.noSender());
        warehouse.tell(new AddShardToWarehouse(artifactId2, shardId2, data2), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(warehouse, new GetStatusOfWarehouse(),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            Multimap<String, Integer> expectedShards = ArrayListMultimap.create();
            expectedShards.put(artifactId1, shardId1);
            expectedShards.put(artifactId2, shardId2);

            assertInstanceOf(StatusResponseOfWarehouse.class, response);
            StatusResponseOfWarehouse statusResponse = (StatusResponseOfWarehouse) response;
            assertEquals(expectedShards, statusResponse.shards());
        }};
    }

    @Test
    public void testRetrieveDeletedArtifact() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";
        int shardId = 0;
        List<Byte> data = Collections.nCopies(5, (byte) 1);

        warehouse.tell(new AddShardToWarehouse(artifactId, shardId, data), ActorRef.noSender());
        warehouse.tell(new DeleteShardFromWarehouse(artifactId, shardId), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(warehouse, new GetShardFromWarehouse(artifactId, shardId),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ArtifactNotFoundInWarehouse.class, response);
            ArtifactNotFoundInWarehouse artifactResponse = (ArtifactNotFoundInWarehouse) response;
            assertEquals(artifactId, artifactResponse.artifactId());
        }};
    }

    @Test
    public void testRetrieveDeletedShard() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";
        int shardId1 = 0;
        int shardId2 = 5;
        List<Byte> data = Collections.nCopies(5, (byte) 1);

        warehouse.tell(new AddShardToWarehouse(artifactId, shardId1, data), ActorRef.noSender());
        warehouse.tell(new AddShardToWarehouse(artifactId, shardId2, data), ActorRef.noSender());
        warehouse.tell(new DeleteShardFromWarehouse(artifactId, shardId1), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(warehouse, new GetShardFromWarehouse(artifactId, shardId1),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ShardNotFoundInWarehouse.class, response);
            ShardNotFoundInWarehouse shardResponse = (ShardNotFoundInWarehouse) response;
            assertEquals(artifactId, shardResponse.artifactId());
            assertEquals(shardId1, shardResponse.shardId());
        }};
    }

    @Test
    public void testGetNumberOfStoredShards()
    {
        String artifactId = "ArtifactName";
        int shardId = 0;
        List<Byte> data = Collections.nCopies(5, (byte) 1);

        warehouse.tell(new AddShardToWarehouse(artifactId, shardId, data), ActorRef.noSender());

        NumberOfStoredShards numberOfStoredShards = vaultProbe.expectMsgClass(Duration.create(200, TimeUnit.MILLISECONDS), NumberOfStoredShards.class);
        assertEquals(warehouseId, numberOfStoredShards.warehouseId());
        assertEquals(1, numberOfStoredShards.numberOfStoredShards());
    }
}
