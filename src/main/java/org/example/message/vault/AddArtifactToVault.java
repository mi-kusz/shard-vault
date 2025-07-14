package org.example.message.vault;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record AddArtifactToVault(@JsonProperty("artifactId") String artifactId, @JsonProperty("data") List<Byte> data)
{
}
