package org.example.message.collector;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CannotRecoverArtifact(@JsonProperty("artifactId") String artifactId)
{
}
