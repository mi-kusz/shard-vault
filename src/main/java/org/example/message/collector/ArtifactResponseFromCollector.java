package org.example.message.collector;

import java.util.List;

public record ArtifactResponseFromCollector(String artifactId, List<Byte> data)
{
}
