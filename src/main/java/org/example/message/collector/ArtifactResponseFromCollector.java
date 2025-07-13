package org.example.message.collector;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record ArtifactResponseFromCollector(@JsonProperty("artifactId") String artifactId, @JsonProperty("data") List<Byte> data)
{
}
