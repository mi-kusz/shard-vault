package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.testkit.TestKit;
import akka.util.Timeout;
import org.example.actor.VaultManagerActor;
import org.example.message.collector.ArtifactResponseFromCollector;
import org.example.message.vault.*;
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

public class VaultManagerActorTest
{
    private ActorSystem system;
    private ActorRef vault;

    @BeforeEach
    public void setup()
    {
        system = ActorSystem.create("TestSystem");
        vault = system.actorOf(VaultManagerActor.props(3, 3, 10));
    }

    @AfterEach
    public void cleanup()
    {
        TestKit.shutdownActorSystem(system, Duration.create(5, TimeUnit.SECONDS), false);
    }

    @Test
    public void testAddAndRetrieveArtifact() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";
        List<Byte> data = Collections.nCopies(500, (byte) 100);
        vault.tell(new AddArtifactToVault(artifactId, data), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(vault, new GetArtifactFromVault(artifactId),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ArtifactResponseFromCollector.class, response);
            ArtifactResponseFromCollector artifactResponse = (ArtifactResponseFromCollector) response;
            assertEquals(artifactId, artifactResponse.artifactId());
            assertEquals(data, artifactResponse.data());
        }};
    }

    @Test
    public void testRetrieveNonExistingArtifact() throws ExecutionException, InterruptedException
    {
        String nonExistingArtifactId = "NonExistingArtifact";

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(vault, new GetArtifactFromVault(nonExistingArtifactId),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ArtifactNotFoundInVault.class, response);
            ArtifactNotFoundInVault artifactResponse = (ArtifactNotFoundInVault) response;
            assertEquals(nonExistingArtifactId, artifactResponse.artifactId());
        }};
    }

    @Test
    public void testAddAndRetrieveMultipleArtefacts() throws ExecutionException, InterruptedException
    {
        String artifactId1 = "ArtifactName1";
        List<Byte> data1 = Collections.nCopies(100, (byte) 100);
        vault.tell(new AddArtifactToVault(artifactId1, data1), ActorRef.noSender());

        String artifactId2 = "ArtifactName2";
        List<Byte> data2 = Collections.nCopies(200, (byte) 200);
        vault.tell(new AddArtifactToVault(artifactId2, data2), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future1 = FutureConverters.asJava(Patterns.ask(vault, new GetArtifactFromVault(artifactId1),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            CompletionStage<Object> future2 = FutureConverters.asJava(Patterns.ask(vault, new GetArtifactFromVault(artifactId2),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response1 = future1.toCompletableFuture().get();
            Object response2 = future2.toCompletableFuture().get();

            assertInstanceOf(ArtifactResponseFromCollector.class, response1);
            ArtifactResponseFromCollector artifactResponse1 = (ArtifactResponseFromCollector) response1;
            assertEquals(artifactId1, artifactResponse1.artifactId());
            assertEquals(data1, artifactResponse1.data());

            assertInstanceOf(ArtifactResponseFromCollector.class, response2);
            ArtifactResponseFromCollector artifactResponse2 = (ArtifactResponseFromCollector) response2;
            assertEquals(artifactId2, artifactResponse2.artifactId());
            assertEquals(data2, artifactResponse2.data());
        }};
    }

    @Test
    public void testAddRetrieveDeleteArtifact() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";
        List<Byte> data = Collections.nCopies(500, (byte) 100);
        vault.tell(new AddArtifactToVault(artifactId, data), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(vault, new GetArtifactFromVault(artifactId),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ArtifactResponseFromCollector.class, response);
            ArtifactResponseFromCollector artifactResponse = (ArtifactResponseFromCollector) response;
            assertEquals(artifactId, artifactResponse.artifactId());
            assertEquals(data, artifactResponse.data());


            vault.tell(new DeleteArtifactFromVault(artifactId), ActorRef.noSender());


            future = FutureConverters.asJava(Patterns.ask(vault, new GetArtifactFromVault(artifactId),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            response = future.toCompletableFuture().get();

            assertInstanceOf(ArtifactNotFoundInVault.class, response);
        }};
    }

    @Test
    public void testAddExistingArtifact() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";
        List<Byte> data = Collections.nCopies(100, (byte) 100);
        vault.tell(new AddArtifactToVault(artifactId, data), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(vault, new AddArtifactToVault(artifactId, data),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ArtifactAlreadyExistsInVault.class, response);
            ArtifactAlreadyExistsInVault artifactResponse = (ArtifactAlreadyExistsInVault) response;
            assertEquals(artifactId, artifactResponse.artifactId());
        }};
    }

    @Test
    public void testDeleteNonExistingArtifact() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(vault, new DeleteArtifactFromVault(artifactId),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ArtifactNotFoundInVault.class, response);
            ArtifactNotFoundInVault artifactResponse = (ArtifactNotFoundInVault) response;
            assertEquals(artifactId, artifactResponse.artifactId());
        }};
    }

    @Test
    public void testDeleteAndReAddArtifact() throws ExecutionException, InterruptedException
    {
        String artifactId = "ArtifactName";
        List<Byte> data = Collections.nCopies(500, (byte) 100);
        vault.tell(new AddArtifactToVault(artifactId, data), ActorRef.noSender());
        vault.tell(new DeleteArtifactFromVault(artifactId), ActorRef.noSender());
        vault.tell(new AddArtifactToVault(artifactId, data), ActorRef.noSender());

        new TestKit(system)
        {{
            CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(vault, new GetArtifactFromVault(artifactId),
                    Timeout.create(java.time.Duration.ofSeconds(5))));
            Object response = future.toCompletableFuture().get();

            assertInstanceOf(ArtifactResponseFromCollector.class, response);
            ArtifactResponseFromCollector artifactResponse = (ArtifactResponseFromCollector) response;
            assertEquals(artifactId, artifactResponse.artifactId());
            assertEquals(data, artifactResponse.data());
        }};
    }
}
