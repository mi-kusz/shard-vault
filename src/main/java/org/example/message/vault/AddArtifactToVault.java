package org.example.message.vault;

import java.util.List;

public record AddArtifactToVault(String artifactId, List<Byte> data)
{
}
