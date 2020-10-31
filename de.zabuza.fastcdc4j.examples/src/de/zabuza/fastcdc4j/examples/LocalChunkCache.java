package de.zabuza.fastcdc4j.examples;

import de.zabuza.fastcdc4j.external.chunking.Chunk;
import de.zabuza.fastcdc4j.external.chunking.ChunkMetadata;
import de.zabuza.fastcdc4j.external.chunking.Chunker;
import de.zabuza.fastcdc4j.external.chunking.ChunkerBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Class offering a {@link #main(String[])} method that chunks a given build and populates a local chunk cache.
 *
 * @author Daniel Tischner {@literal <zabuza.dev@gmail.com>}
 */
@SuppressWarnings({"UseOfSystemOutOrSystemErr", "ClassIndependentOfModule", "ClassOnlyUsedInOneModule"})
enum LocalChunkCache {
    ;

    /**
     * Starts the application.
     *
     * @param args Two arguments, the path to the build and the path to the local chunk cache
     * @throws IOException If an IOException occurred
     */
    public static void main(final String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException(
                    "Expected two arguments buildPath and cachePath, where buildPath denotes the path to the build and cachePath the path to the local chunk cache.");
        }
        final Path buildPath = Path.of(args[0]);
        final Path cachePath = Path.of(args[1]);


        List<Path> files = Files.walk(buildPath)
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
        final Chunker chunker = new ChunkerBuilder().nlFiedlerRust().build();

        Consumer<? super Iterable<Chunk>> chunkAction = chunks -> {
            int cachedChunks = 0;
            int uncachedChunks = 0;

            for (final Chunk chunk : chunks) {
                final Path chunkPath = cachePath.resolve(chunk.getHexHash());

                ChunkMetadata chunkMetadata = chunk.toChunkMetadata();
                System.out.printf("hash=%s offset=%d size=%d%n", chunkMetadata.getHexHash(), chunkMetadata.getOffset(), chunkMetadata.getLength());
                if (Files.exists(chunkPath)) {
                    cachedChunks++;
                } else {
                    try {
                        Files.write(chunkPath, chunk.getData());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    uncachedChunks++;
                }
            }
            System.out.printf("%d cached chunks, %d uncached chunks%n", cachedChunks, uncachedChunks);
        };

        files.parallelStream()
                .map(chunker::chunk)
                .forEach(chunkAction);
    }
}
