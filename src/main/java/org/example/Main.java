package org.example;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import org.example.actor.VaultManagerActor;
import org.example.message.vault.AddArtifactToVault;

import java.util.Collections;

public class Main
{
    public static void main(String[] args)
    {
        ActorSystem actorSystem = ActorSystem.create("Vault");
        ActorRef vault = actorSystem.actorOf(VaultManagerActor.props(5, 3, 10));

        vault.tell(new AddArtifactToVault("abc", Collections.nCopies(10, (byte) 1)),ActorRef.noSender());

        new HttpServer(actorSystem, vault).start();
    }
}