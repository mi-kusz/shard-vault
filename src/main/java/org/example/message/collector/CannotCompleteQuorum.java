package org.example.message.collector;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CannotCompleteQuorum(@JsonProperty("artifactId") String artifactId)
{
}
