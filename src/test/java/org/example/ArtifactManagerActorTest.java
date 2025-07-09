package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import akka.testkit.TestProbe;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.example.actor.ArtifactManagerActor;
import org.example.message.manager.DeleteArtifactFromManager;
import org.example.message.warehouse.DeleteShardFromWarehouse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ArtifactManagerActorTest
{
    private ActorSystem system;
    private final List<TestProbe> testProbes = new ArrayList<>();
    private ActorRef artifactManager;
    private final String artifactId = "ArtifactName";

    @BeforeEach
    public void setup() throws InterruptedException
    {
        system = ActorSystem.create("TestSystem");

        Multimap<Integer, ActorRef> warehouseAssignment = ArrayListMultimap.create();

        int numberOfWarehouses = 5;

        for (int i = 0; i < numberOfWarehouses; ++i)
        {
            TestProbe testProbe = new TestProbe(system);
            testProbes.add(testProbe);

            warehouseAssignment.put(i, testProbe.ref());
        }

        artifactManager = system.actorOf(ArtifactManagerActor.props(artifactId, Collections.nCopies(101, (byte) 1), warehouseAssignment, 5, numberOfWarehouses));

        // Wait for the creation of actors
        Thread.sleep(1000);

        // Clear waiting messages
        for (TestProbe testProbe : testProbes)
        {
            while (testProbe.msgAvailable())
            {
                testProbe.receiveOne(Duration.create(100, TimeUnit.MILLISECONDS));
            }
        }
    }

    @AfterEach
    public void cleanup()
    {
        TestKit.shutdownActorSystem(system, Duration.create(5, TimeUnit.SECONDS), false);
    }

    @Test
    public void testDeleteArtifact() throws ExecutionException, InterruptedException
    {
        artifactManager.tell(new DeleteArtifactFromManager(), ActorRef.noSender());

        new TestKit(system)
        {{
            for (TestProbe testProbe : testProbes)
            {
                DeleteShardFromWarehouse message = testProbe.expectMsgClass(DeleteShardFromWarehouse.class);
                assertEquals(artifactId, message.artifactId());
            }
        }};
    }
}
