package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.example.message.collector.ArtifactResponseFromCollector;
import org.example.message.vault.AddArtifactToVault;
import org.example.message.vault.GetArtifactFromVault;
import scala.jdk.javaapi.FutureConverters;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static akka.http.javadsl.server.PathMatchers.segment;

public class HttpServer extends AllDirectives
{
    private final ActorSystem system;
    private final ActorRef vaultManager;

    public HttpServer(ActorSystem system, ActorRef vaultManager)
    {
        this.system = system;
        this.vaultManager = vaultManager;
    }

    public void start()
    {
        Http http = Http.get(system);

        Route routes = createRoutes();

        CompletionStage<ServerBinding> binding = http
                .newServerAt("localhost", 8080)
                .bind(routes);

        binding
                .thenAccept(b -> System.out.println("OK"))
                .exceptionally(e -> {
                    System.err.println("Error");
                    return null;
                });
    }

    private Route createRoutes()
    {
        return concat(
                handleAddArtifact(),
                handleGetArtifact(),
                handleDeleteArtifact()
        );
    }

    private Route handleAddArtifact()
    {
        return path("artifact", () ->
                put(
                        () -> entity(Jackson.unmarshaller(AddArtifactToVault.class), request ->
                                completeOK(request, Jackson.marshaller()))
                )
        );
    }

    private Route handleGetArtifact() {
        return path(PathMatchers.segment("artifact").slash(PathMatchers.segment()), artifactId ->
                get(() -> {
                    CompletionStage<Object> future = FutureConverters.asJava(Patterns.ask(vaultManager, new GetArtifactFromVault(artifactId), Timeout.create(Duration.ofSeconds(5))));
                    future.thenApply(response -> (ArtifactResponseFromCollector) response);

                    return completeOKWithFuture(future, Jackson.marshaller());
                })
        );
    }

    private Route handleDeleteArtifact()
    {
        return path("artifact", () ->
                pathPrefix(segment(), artifactId ->
                        delete(
                                () -> {
                                    System.out.println("Usunięto " + artifactId);
                                    return null;
                                }
                        )
                )
        );
    }
}
