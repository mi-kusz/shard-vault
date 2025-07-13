package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.marshallers.jackson.Jackson;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.AllDirectives;
import akka.http.javadsl.server.PathMatchers;
import akka.http.javadsl.server.Route;
import akka.http.scaladsl.marshalling.Marshalling;
import akka.http.scaladsl.marshalling.Marshalling$;
import akka.pattern.Patterns;
import akka.util.Timeout;
import org.example.message.collector.ArtifactResponseFromCollector;
import org.example.message.collector.CannotCompleteQuorum;
import org.example.message.collector.CannotRecoverArtifact;
import org.example.message.vault.AddArtifactToVault;
import org.example.message.vault.ArtifactNotFoundInVault;
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
                    CompletionStage<HttpResponse> httpFuture = future.handle((response, throwable) -> {
                        if (throwable != null)
                        {
                            return HttpResponse.create()
                                    .withStatus(StatusCodes.INTERNAL_SERVER_ERROR)
                                    .withEntity("Internal server error");
                        }

                        return switch(response)
                        {
                            case ArtifactResponseFromCollector artifactResponse -> HttpResponse.create()
                                    .withStatus(StatusCodes.OK)
                                    .withEntity(artifactResponse.toString());

                            case CannotRecoverArtifact recoveryError -> HttpResponse.create()
                                    .withStatus(StatusCodes.INTERNAL_SERVER_ERROR)
                                    .withEntity("Cannot recover artifact");

                            case CannotCompleteQuorum quorumError -> HttpResponse.create()
                                    .withStatus(StatusCodes.INTERNAL_SERVER_ERROR)
                                    .withEntity("Cannot complete quorum");

                            case ArtifactNotFoundInVault notFound -> HttpResponse.create()
                                    .withStatus(StatusCodes.NOT_FOUND)
                                    .withEntity("Artifact " + artifactId + " not found");

                            default -> HttpResponse.create()
                                    .withStatus(StatusCodes.INTERNAL_SERVER_ERROR)
                                    .withEntity("Unknown error");
                        };
                    });

                    return completeWithFuture(httpFuture);
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
