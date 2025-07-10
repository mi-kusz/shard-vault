package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import akka.testkit.TestProbe;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.example.actor.ShardCollectorActor;
import org.example.message.collector.ArtifactResponseFromCollector;
import org.example.message.collector.CannotCompleteQuorum;
import org.example.message.collector.CannotRecoverArtifact;
import org.example.message.warehouse.GetShardFromWarehouse;
import org.example.message.warehouse.ShardNotFoundInWarehouse;
import org.example.message.warehouse.ShardResponseFromWarehouse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShardCollectorActorTest
{
    private ActorSystem system;

    Multimap<Integer, TestProbe> testProbes = ArrayListMultimap.create();
    Multimap<Integer, ActorRef> warehouses = ArrayListMultimap.create();

    private TestProbe originalSender;

    private final int numberOfShards = 5;
    private final int numberOfReplicas = 3;

    private final String artifactId = "ArtifactName";

    @BeforeEach
    public void setup()
    {
        system = ActorSystem.create("TestSystem");

        for (int i = 0; i < numberOfShards; ++i)
        {
            for (int replica = 0; replica < numberOfReplicas; ++replica)
            {
                TestProbe testProbe = new TestProbe(system);
                ActorRef warehouse = testProbe.ref();
                testProbes.put(i, testProbe);
                warehouses.put(i, warehouse);
            }
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
    public void testAskForShards()
    {
        system.actorOf(ShardCollectorActor.props(artifactId, warehouses, originalSender.ref()));

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
    public void testBuildArtifact()
    {
        List<Byte> expectedData = new ArrayList<>();
        for (int i = 0; i < numberOfShards; ++i) expectedData.add((byte) 1);

        system.actorOf(ShardCollectorActor.props(artifactId, warehouses, originalSender.ref()));

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
        system.actorOf(ShardCollectorActor.props(artifactId, warehouses, originalSender.ref()));

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

    @Test
    public void testQuorum()
    {
        system.actorOf(ShardCollectorActor.props(artifactId, warehouses, originalSender.ref()));

        List<Byte> expectedData = new ArrayList<>();
        for (int i = 0; i < numberOfShards; ++i) expectedData.add((byte) 1);

        for (int shardId : testProbes.keySet())
        {
            List<TestProbe> probes = testProbes.get(shardId).stream().toList();

            for (int i = 0; i < probes.size(); ++i)
            {
                TestProbe testProbe = probes.get(i);
                testProbe.receiveOne(Duration.create(100, TimeUnit.MILLISECONDS));
                List<Byte> data;

                if (i == 0) // Invalid other data
                {
                    data = Collections.nCopies(10, (byte) 100);
                }
                else // Valid data
                {
                    data = Collections.nCopies(1, (byte) 1);
                }

                testProbe.reply(new ShardResponseFromWarehouse(artifactId, shardId, data));
            }
        }

        ArtifactResponseFromCollector message = originalSender.expectMsgClass(ArtifactResponseFromCollector.class);
        assertEquals(artifactId, message.artifactId());
        assertEquals(expectedData, message.data());
    }

    @Test
    public void testTieInQuorum()
    {
        system.actorOf(ShardCollectorActor.props(artifactId, warehouses, originalSender.ref()));

        for (int shardId : testProbes.keySet())
        {
            List<TestProbe> probes = testProbes.get(shardId).stream().toList();

            TestProbe testProbe1 = probes.getFirst();
            TestProbe testProbe2 = probes.get(1);


            testProbe1.receiveOne(Duration.create(100, TimeUnit.MILLISECONDS));
            testProbe2.receiveOne(Duration.create(100, TimeUnit.MILLISECONDS));

            testProbe1.reply(new ShardResponseFromWarehouse(artifactId, shardId, Collections.nCopies(10, (byte) 100)));
            testProbe2.reply(new ShardResponseFromWarehouse(artifactId, shardId, Collections.nCopies(1, (byte) 1)));
        }

        CannotCompleteQuorum message = originalSender.expectMsgClass(CannotCompleteQuorum.class);
        assertEquals(artifactId, message.artifactId());
    }
}
