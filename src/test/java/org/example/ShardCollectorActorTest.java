package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import akka.testkit.TestProbe;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.example.actor.ShardCollectorActor;
import org.example.message.collector.ArtifactResponseFromCollector;
import org.example.message.collector.CannotRecoverArtifact;
import org.example.message.warehouse.GetShardFromWarehouse;
import org.example.message.warehouse.ShardNotFoundInWarehouse;
import org.example.message.warehouse.ShardResponseFromWarehouse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShardCollectorActorTest
{
    private ActorSystem system;

    Multimap<Integer, TestProbe> testProbes = ArrayListMultimap.create();
    private TestProbe originalSender;

    Multimap<Integer, ActorRef> warehouses = ArrayListMultimap.create();

    private final int numberOfShards = 5;

    private final String artifactId = "ArtifactName";

    @BeforeEach
    public void setup()
    {
        system = ActorSystem.create("TestSystem");

        for (int i = 0; i < numberOfShards; ++i)
        {
            TestProbe testProbe = new TestProbe(system);
            ActorRef warehouse = testProbe.ref();
            testProbes.put(i, testProbe);
            warehouses.put(i, warehouse);
        }

        originalSender = new TestProbe(system);
    }

    @AfterEach
    public void cleanup()
    {
        warehouses.clear();
        TestKit.shutdownActorSystem(system, Duration.create(5, TimeUnit.SECONDS), false);
    }

    @Test
    public void testAskForShards() throws InterruptedException
    {
        system.actorOf(ShardCollectorActor.props(artifactId, warehouses, originalSender.ref()));;

        for (int shardId : testProbes.keySet())
        {
            for (TestProbe testProbe : testProbes.get(shardId))
            {
                GetShardFromWarehouse message = testProbe.expectMsgClass(Duration.create(200, TimeUnit.MILLISECONDS), GetShardFromWarehouse.class);
                assertEquals(artifactId, message.artifactId());
                assertEquals(shardId, message.shardId());
            }
        }
    }

    @Test
    public void testBuildArtifact() throws InterruptedException
    {
        List<Byte> expectedData = new ArrayList<>();
        for (int i = 0; i < numberOfShards; ++i) expectedData.add((byte) 1);

        system.actorOf(ShardCollectorActor.props(artifactId, warehouses, originalSender.ref()));;

        for (int shardId : testProbes.keySet())
        {
            for (TestProbe testProbe : testProbes.get(shardId))
            {
                testProbe.receiveOne(Duration.create(1, TimeUnit.SECONDS));
                testProbe.reply(new ShardResponseFromWarehouse(artifactId, shardId, List.of((byte) 1)));
            }
        }

        ArtifactResponseFromCollector message = originalSender.expectMsgClass(ArtifactResponseFromCollector.class);
        assertEquals(artifactId, message.artifactId());
        assertEquals(expectedData, message.data());
    }

    @Test
    public void testBuildArtifactFailed()
    {
        system.actorOf(ShardCollectorActor.props(artifactId, warehouses, originalSender.ref()));;

        for (int shardId : testProbes.keySet())
        {
            for (TestProbe testProbe : testProbes.get(shardId))
            {
                testProbe.receiveOne(Duration.create(100, TimeUnit.MILLISECONDS));
                testProbe.reply(new ShardNotFoundInWarehouse(artifactId, shardId));
            }
        }

        CannotRecoverArtifact message = originalSender.expectMsgClass(CannotRecoverArtifact.class);
        assertEquals(artifactId, message.artifactId());
    }
}
