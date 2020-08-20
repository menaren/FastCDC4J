package de.zabuza.fastcdc4j.examples;

import de.zabuza.fastcdc4j.external.chunking.Chunk;
import de.zabuza.fastcdc4j.external.chunking.ChunkMetadata;
import de.zabuza.fastcdc4j.external.chunking.Chunker;
import de.zabuza.fastcdc4j.external.chunking.ChunkerBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class offering a {@link #main(String[])} method that compares two given paths with each other and prints statistics
 * for patching one file to the other using chunks.
 *
 * @author Daniel Tischner {@literal <zabuza.dev@gmail.com>}
 */
@SuppressWarnings("UseOfSystemOutOrSystemErr")
final class PatchSummary {
	/**
	 * Starts the application.
	 *
	 * @param args Two arguments, the path to the previous build and the path to the current build to compare.
	 */
	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new IllegalArgumentException(
					"Expected two arguments path1 and path2, where path1 denotes the path to the previous version and path2 the path to the current version.");
		}
		final Path previousBuild = Path.of(args[0]);
		final Path currentBuild = Path.of(args[1]);

		final Map<String, Chunker> descriptionToChunker = new LinkedHashMap<>();
		//		descriptionToChunker.put("FSC 8KB", new ChunkerBuilder().setChunkerOption(ChunkerOption.FIXED_SIZE_CHUNKING)
		//				.build());
		//		descriptionToChunker.put("FastCDC 8KB RTPal", new ChunkerBuilder().setChunkerOption(ChunkerOption.FAST_CDC)
		//				.build());
		//		descriptionToChunker.put("NlFiedlerRust 8KB NlFiedlerRust",
		//				new ChunkerBuilder().setChunkerOption(ChunkerOption.NLFIEDLER_RUST)
		//						.setHashTableOption(HashTableOption.NLFIEDLER_RUST)
		//						.build());
		//		descriptionToChunker.put("FastCDC 8KB NlFiedlerRust",
		//				new ChunkerBuilder().setChunkerOption(ChunkerOption.FAST_CDC)
		//						.setHashTableOption(HashTableOption.NLFIEDLER_RUST)
		//						.build());
		//		descriptionToChunker.put("NlFiedlerRust 8KB RTPal",
		//				new ChunkerBuilder().setChunkerOption(ChunkerOption.NLFIEDLER_RUST)
		//						.setHashTableOption(HashTableOption.RTPAL)
		//						.build());
		descriptionToChunker.put("FSC 8KB", new ChunkerBuilder().fsc()
				.setExpectedChunkSize(8 * 1024)
				.build());
		descriptionToChunker.put("FastCDC 8KB", new ChunkerBuilder().fastCdc()
				.setExpectedChunkSize(8 * 1024)
				.build());
		//		descriptionToChunker.put("NlFiedlerRust 2MB",
		//				new ChunkerBuilder().nlFiedlerRust()
		//						.setExpectedChunkSize(2 * 1024 * 1024)
		//						.build());
		//		descriptionToChunker.put("FastCDC 1MB NlFiedlerRust",
		//				new ChunkerBuilder().setChunkerOption(ChunkerOption.FAST_CDC)
		//						.setHashTableOption(HashTableOption.NLFIEDLER_RUST)
		//						.setExpectedChunkSize(1024 * 1024)
		//						.build());
		//		descriptionToChunker.put("NlFiedlerRust 1MB RTPal",
		//				new ChunkerBuilder().setChunkerOption(ChunkerOption.NLFIEDLER_RUST)
		//						.setHashTableOption(HashTableOption.RTPAL)
		//						.setExpectedChunkSize(1024 * 1024)
		//						.build());
		//		descriptionToChunker.put("FastCDC 1MB RTPal seed 1", new ChunkerBuilder().setChunkerOption(ChunkerOption.FAST_CDC)
		//				.setExpectedChunkSize(1024 * 1024)
		//				.setMaskGenerationSeed(1)
		//				.build());
		//		descriptionToChunker.put("FastCDC 1MB RTPal seed 2", new ChunkerBuilder().setChunkerOption(ChunkerOption.FAST_CDC)
		//				.setExpectedChunkSize(1024 * 1024)
		//				.setMaskGenerationSeed(2)
		//				.build());
		//		descriptionToChunker.put("FastCDC 1MB RTPal seed 3", new ChunkerBuilder().setChunkerOption(ChunkerOption.FAST_CDC)
		//				.setExpectedChunkSize(1024 * 1024)
		//				.setMaskGenerationSeed(3)
		//				.build());
		//		descriptionToChunker.put("FastCDC 1MB RTPal seed 4", new ChunkerBuilder().setChunkerOption(ChunkerOption.FAST_CDC)
		//				.setExpectedChunkSize(1024 * 1024)
		//				.setMaskGenerationSeed(4)
		//				.build());

		System.out.printf("Summary for patching from previous (%s) to current (%s):%n", previousBuild,
				currentBuild);
		System.out.println();
		descriptionToChunker.forEach(
				(description, chunker) -> PatchSummary.executePatchSummary(description, chunker, previousBuild,
					currentBuild));
	}

	static PatchSummary computePatchSummary(final Chunker chunker, final Path previousBuild,
			final Path currentBuild) {
		final List<ChunkMetadata> previousChunks = Collections.synchronizedList(new ArrayList<>());
		chunkPath(chunker, previousBuild, chunk -> previousChunks.add(chunk.toChunkMetadata()));
		final BuildSummary previousBuildSummary = new BuildSummary(previousChunks);

		final List<ChunkMetadata> currentChunks = Collections.synchronizedList(new ArrayList<>());
		chunkPath(chunker, currentBuild, chunk -> currentChunks.add(chunk.toChunkMetadata()));
		final BuildSummary currentBuildSummary = new BuildSummary(currentChunks);

		return new PatchSummary(previousBuildSummary, currentBuildSummary);
	}

	private static String bytesToReadable(long bytes) {
		if (bytes < 1_000) {
			return bytes + " B";
		}

		double kiloBytes = bytes / 1_000.0;
		if (kiloBytes < 1_000) {
			return String.format("%.2f", kiloBytes) + " KB";
		}

		double megaBytes = kiloBytes / 1_000.0;
		if (megaBytes < 1_000) {
			return String.format("%.2f", megaBytes) + " MB";
		}

		double gigaBytes = megaBytes / 1_000.0;
		if (gigaBytes < 1_000) {
			return String.format("%.2f", gigaBytes) + " GB";
		}
		return "";
	}

	private static void chunkPath(final Chunker chunker, final Path path, final Consumer<Chunk> chunkAction) {
		try {
			List<Path> files = Files.walk(path)
					.filter(Files::isRegularFile)
					.collect(Collectors.toList());

			long totalBytes = files.stream()
					.mapToLong(file -> {
						try {
							return Files.size(file);
						} catch (IOException e) {
							throw new UncheckedIOException(e);
						}
					})
					.sum();
			AtomicLong processedBytesTotal = new AtomicLong(0);
			AtomicLong processedBytesSincePrint = new AtomicLong(0);
			AtomicLong timeStart = new AtomicLong(System.nanoTime());
			ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
			final long nanosPerSecond = 1_000_000_000L;
			Runnable statPrinter = () -> {
				AtomicLong timeEnd = new AtomicLong(System.nanoTime());
				long timeDiff = timeEnd.get() - timeStart.get();
				if (timeDiff < nanosPerSecond) {
					return;
				}
				timeStart.set(timeEnd.get());
				long bytesPerSecond = processedBytesSincePrint.get() / (timeDiff / nanosPerSecond);
				long bytesLeft = totalBytes - processedBytesTotal.get();
				long secondsLeft = bytesLeft / (bytesPerSecond == 0 ? 1 : bytesPerSecond);

				System.out.printf("\t%12s/s, %12s ETC, %12s processed, %12s total\r", bytesToReadable(bytesPerSecond),
						secondsToReadable(secondsLeft), bytesToReadable(processedBytesTotal.get()),
						bytesToReadable(totalBytes));

				processedBytesSincePrint.set(0);
			};
			var statPrintTask = service.scheduleAtFixedRate(statPrinter, 0, 1, TimeUnit.SECONDS);

			files.parallelStream()
					.filter(Files::isRegularFile)
					.map(chunker::chunk)
					.forEach(chunks -> chunks.forEach(chunk -> {
						processedBytesTotal.addAndGet(chunk.getLength());
						processedBytesSincePrint.addAndGet(chunk.getLength());

						chunkAction.accept(chunk);
					}));
			statPrintTask.cancel(false);
			service.shutdown();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	static void executePatchSummary(final String description, final Chunker chunker, final Path previousBuild,
			final Path currentBuild) {
		final PatchSummary summary = computePatchSummary(chunker, previousBuild, currentBuild);
		System.out.println("==== " + description);
		System.out.printf("%-25s %12s total size, %12d total chunks, %12s unique size, %12d unique chunks%n",
				"Build summary previous:", bytesToReadable(summary.getPreviousBuildSummary()
						.getTotalSize()), summary.getPreviousBuildSummary()
						.getTotalChunksCount(), bytesToReadable(summary.getPreviousBuildSummary()
						.getTotalUniqueSize()), summary.getPreviousBuildSummary()
						.getUniqueChunksCount());
		System.out.printf("%-25s %12s total size, %12d total chunks, %12s unique size, %12d unique chunks%n",
				"Build summary current:", bytesToReadable(summary.getCurrentBuildSummary()
						.getTotalSize()), summary.getCurrentBuildSummary()
						.getTotalChunksCount(), bytesToReadable(summary.getCurrentBuildSummary()
						.getTotalUniqueSize()), summary.getCurrentBuildSummary()
						.getUniqueChunksCount());
		System.out.printf("%-25s %12s average chunk size, %12.2f%% deduplication ratio%n", "Build metrics previous:",
				bytesToReadable(summary.getPreviousBuildSummary()
						.getAverageChunkSize()), summary.getPreviousBuildSummary()
						.getDeduplicationRatio());
		System.out.printf("%-25s %12s average chunk size, %12.2f%% deduplication ratio%n", "Build metrics current:",
				bytesToReadable(summary.getCurrentBuildSummary()
						.getAverageChunkSize()), summary.getCurrentBuildSummary()
						.getDeduplicationRatio());
		System.out.printf("%-25s %12s%n", "Patch size:", bytesToReadable(summary.getPatchSize()));
		System.out.printf("%-25s %12d%n", "Chunks to add:", summary.getChunksToAdd()
				.size());
		System.out.printf("%-25s %12d%n", "Chunks to remove:", summary.getChunksToRemove()
				.size());
		System.out.printf("%-25s %12d%n", "Chunks to move:", summary.getChunksToMove()
				.size());
		System.out.printf("%-25s %12d%n", "Untouched chunks:", summary.getUntouchedChunks()
				.size());
		System.out.println();
	}

	private static String secondsToReadable(long seconds) {
		StringBuilder sb = new StringBuilder();
		boolean entered = false;
		Duration time = Duration.ofSeconds(seconds);

		long days = time.toDays();
		if (days != 0) {
			sb.append(days)
					.append("d ");
			entered = true;
		}

		int hours = time.toHoursPart();
		if (hours != 0 || entered) {
			sb.append(hours)
					.append("h ");
			entered = true;
		}

		int minutes = time.toMinutesPart();
		if (minutes != 0 || entered) {
			sb.append(minutes)
					.append("m ");
		}

		sb.append(time.toSecondsPart())
				.append("s");
		return sb.toString();
	}

	private final List<ChunkMetadata> chunksToAdd = new ArrayList<>();
	private final List<ChunkMetadata> chunksToMove = new ArrayList<>();
	private final List<ChunkMetadata> chunksToRemove = new ArrayList<>();
	private final BuildSummary currentBuildSummary;
	private final BuildSummary previousBuildSummary;
	private final List<ChunkMetadata> untouchedChunks = new ArrayList<>();
	private long patchSize;

	private PatchSummary(final BuildSummary previousBuildSummary, final BuildSummary currentBuildSummary) {
		this.previousBuildSummary = previousBuildSummary;
		this.currentBuildSummary = currentBuildSummary;
		computePatch();
	}

	BuildSummary getCurrentBuildSummary() {
		return currentBuildSummary;
	}

	BuildSummary getPreviousBuildSummary() {
		return previousBuildSummary;
	}

	private void computePatch() {
		// Chunks to remove
		previousBuildSummary.getChunks()
				.filter(Predicate.not(currentBuildSummary::containsChunk))
				.forEach(chunksToRemove::add);
		// Chunks to add
		currentBuildSummary.getChunks()
				.filter(Predicate.not(previousBuildSummary::containsChunk))
				.forEach(chunksToAdd::add);
		// Chunks to move
		currentBuildSummary.getChunks()
				.filter(previousBuildSummary::containsChunk)
				.filter(currentChunk -> previousBuildSummary.getChunk(currentChunk.getHexHash())
						.getOffset() != currentChunk.getOffset())
				.forEach(chunksToMove::add);
		// Untouched chunks
		currentBuildSummary.getChunks()
				.filter(previousBuildSummary::containsChunk)
				.filter(currentChunk -> previousBuildSummary.getChunk(currentChunk.getHexHash())
						.getOffset() == currentChunk.getOffset())
				.forEach(untouchedChunks::add);

		patchSize = chunksToAdd.stream()
				.mapToLong(ChunkMetadata::getLength)
				.sum();
	}

	private List<ChunkMetadata> getChunksToAdd() {
		return Collections.unmodifiableList(chunksToAdd);
	}

	private List<ChunkMetadata> getChunksToMove() {
		return Collections.unmodifiableList(chunksToMove);
	}

	private List<ChunkMetadata> getChunksToRemove() {
		return Collections.unmodifiableList(chunksToRemove);
	}

	long getPatchSize() {
		return patchSize;
	}

	private List<ChunkMetadata> getUntouchedChunks() {
		return Collections.unmodifiableList(untouchedChunks);
	}

	static final class BuildSummary {
		private final Map<String, ChunkMetadata> hashToChunk = new HashMap<>();
		private int totalChunksCount;
		private long totalSize;
		private long totalUniqueSize;
		private int uniqueChunksCount;

		public BuildSummary(final Iterable<? extends ChunkMetadata> chunks) {
			chunks.forEach(chunk -> {
				totalChunksCount++;
				totalSize += chunk.getLength();

				if (hashToChunk.containsKey(chunk.getHexHash())) {
					return;
				}
				hashToChunk.put(chunk.getHexHash(), chunk);
				uniqueChunksCount++;
				totalUniqueSize += chunk.getLength();
			});
		}

		public boolean containsChunk(final ChunkMetadata chunk) {
			return hashToChunk.containsKey(chunk.getHexHash());
		}

		public int getAverageChunkSize() {
			//noinspection NumericCastThatLosesPrecision
			return (int) (totalSize / totalChunksCount);
		}

		public ChunkMetadata getChunk(final String hash) {
			return hashToChunk.get(hash);
		}

		public Stream<ChunkMetadata> getChunks() {
			return hashToChunk.values()
					.stream();
		}

		public double getDeduplicationRatio() {
			return (double) totalUniqueSize / totalSize * 100;
		}

		public int getTotalChunksCount() {
			return totalChunksCount;
		}

		public long getTotalSize() {
			return totalSize;
		}

		public long getTotalUniqueSize() {
			return totalUniqueSize;
		}

		public int getUniqueChunksCount() {
			return uniqueChunksCount;
		}
	}
}
