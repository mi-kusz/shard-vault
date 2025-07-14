package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.testkit.TestRoute;
import akka.testkit.TestKit;
import org.example.actor.VaultManagerActor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HttpServerTest
{
    private ActorSystem system;
    private ActorRef vault;
    private HttpServer server;

    private final static String BASE_URI = "http://localhost:8080";

    private final static String addContent = """
                        {
                            "artifactId": "testName",
                            "data": [1, 2, 3, 4, 5]
                        }
                        """;

    @BeforeEach
    public void setup()
    {
        system = ActorSystem.create("TestSystem");
        vault = system.actorOf(VaultManagerActor.props(3, 3, 10));

        server = new HttpServer(system, vault);
        server.start();
    }

    @AfterEach
    public void cleanup()
    {
        TestKit.shutdownActorSystem(system, Duration.create(5, TimeUnit.SECONDS), false);
    }

    @Test
    public void testHandleAddArtifact()
    {
        String endpoint = BASE_URI + "/artifact";

        HttpRequest request = HttpRequest.PUT(endpoint)
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, addContent));

        Http.get(system).singleRequest(request).thenApply(response -> {
            assertEquals(StatusCodes.CREATED, response.status());
            return null;
        }).toCompletableFuture().join();
    }

    @Test
    public void testHandleAddExistingArtifact()
    {
        String endpoint = BASE_URI + "/artifact";

        HttpRequest request = HttpRequest.PUT(endpoint)
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, addContent));

        Http.get(system).singleRequest(request).thenApply(response -> {
            assertEquals(StatusCodes.CREATED, response.status());
            return null;
        }).toCompletableFuture().join();

        Http.get(system).singleRequest(request).thenApply(response -> {
            assertEquals(StatusCodes.CONFLICT, response.status());
            return null;
        }).toCompletableFuture().join();
    }

    @Test
    public void testHandleGetArtifact()
    {
        String endpoint = BASE_URI + "/artifact";
        String artifactId = "testName";

        HttpRequest addRequest = HttpRequest.PUT(endpoint)
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, addContent));
        Http.get(system).singleRequest(addRequest).toCompletableFuture().join();

        HttpRequest getRequest = HttpRequest.GET(endpoint + "/" + artifactId);
        Http.get(system).singleRequest(getRequest).thenApply(response -> {
            assertEquals(StatusCodes.OK, response.status());
            return null;
        }).toCompletableFuture().join();
    }

    @Test
    public void testHandleGetNonExistingArtifact()
    {
        String endpoint = BASE_URI + "/artifact";
        String artifactId = "testName";

        HttpRequest getRequest = HttpRequest.GET(endpoint + "/" + artifactId);
        Http.get(system).singleRequest(getRequest).thenApply(response -> {
            assertEquals(StatusCodes.NOT_FOUND, response.status());
            return null;
        }).toCompletableFuture().join();
    }

    @Test
    public void testHandleDeleteArtifact()
    {
        String endpoint = BASE_URI + "/artifact";
        String artifactId = "testName";

        HttpRequest addRequest = HttpRequest.PUT(endpoint)
                .withEntity(HttpEntities.create(ContentTypes.APPLICATION_JSON, addContent));
        Http.get(system).singleRequest(addRequest).toCompletableFuture().join();

        HttpRequest getRequest = HttpRequest.DELETE(endpoint + "/" + artifactId);
        Http.get(system).singleRequest(getRequest).thenApply(response -> {
            assertEquals(StatusCodes.NO_CONTENT, response.status());
            return null;
        }).toCompletableFuture().join();
    }

    @Test
    public void testHandleDeleteNonExistingArtifact()
    {
        String endpoint = BASE_URI + "/artifact";
        String artifactId = "testName";

        HttpRequest getRequest = HttpRequest.DELETE(endpoint + "/" + artifactId);
        Http.get(system).singleRequest(getRequest).thenApply(response -> {
            assertEquals(StatusCodes.NOT_FOUND, response.status());
            return null;
        }).toCompletableFuture().join();
    }
}
