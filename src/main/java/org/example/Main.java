package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.example.actor.VaultManagerActor;
import org.example.message.collector.ArtifactResponseFromCollector;
import org.example.message.vault.AddArtifactToVault;
import org.example.message.vault.GetArtifactFromVault;
import scala.jdk.javaapi.FutureConverters;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

public class Main
{
    public static void main(String[] args)
    {
        ActorSystem actorSystem = ActorSystem.create("Vault");
        ActorRef vault = actorSystem.actorOf(VaultManagerActor.props(5, 3, 10));

        vault.tell(new AddArtifactToVault("nazwa", Collections.nCopies(20, (byte) 1)), ActorRef.noSender());

        /*
        CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(vault, new GetArtifactFromVault("nazwa"), Timeout.create(Duration.ofSeconds(5))));

        future.thenAccept(response -> {
            ArtifactResponseFromCollector artifact = (ArtifactResponseFromCollector) response;
            System.out.println(artifact.artifactId());
            System.out.println(artifact.data());
        });

        */
    }
}