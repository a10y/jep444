import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.AtomicInteger;

public final class DirectoryHasher {

    private static final HashFunction sha256 = Hashing.sha256();
    private static final int BATCH_BYTES = 32 * 1024 * 1024;

    public static void main(String[] args) {
        Path downloadsDir = Paths.get(System.getProperty("user.home")).resolve("Downloads");
        long start = System.nanoTime();
        System.out.println(new DirectoryHasher(downloadsDir).hashDirectory());
        long duration = System.nanoTime() - start;
        System.err.println("time: " + Math.floorDiv(duration, 1_000_000) + "ms");
    }

    private final Path directory;

    public record FileHash(
            Path file,
            String sha256
    ) {
    }

    public DirectoryHasher(Path directory) {
        if (!directory.toFile().isDirectory()) {
            throw new IllegalArgumentException("Must provide directory");
        }

        this.directory = directory;
    }

    public String hashDirectory() {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            StructuredTaskScope.Subtask<String> result = scope.fork(() -> DirHasher.INSTANCE.calculateHash(directory));
            scope.join();
            scope.throwIfFailed(e -> new RuntimeException("Scope failed", e));

            //  Find total number of bytes hashed as well
            System.err.println("hashed " + counter.get() + " files");

            return result.get();
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed waiting for subtasks", e);
        }
    }

    sealed interface PathHasher {
        String calculateHash(Path path);
    }

    enum FileHasher implements PathHasher {
        INSTANCE;

        @Override
        public String calculateHash(Path file) {
            Hasher hasher = sha256.newHasher();
            try (BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file.toFile()))) {
                byte[] buf = inputStream.readNBytes(BATCH_BYTES);
                hasher.putBytes(buf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String result = hasher.hash().toString();
            System.err.println(file.getFileName() + " => " + result);
            counter.addAndGet(1);
            return result;
        }
    }

    enum DirHasher implements PathHasher {
        INSTANCE;

        @Override
        public String calculateHash(Path directory) {
            try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                // If it's another file, then we recursively attempt to list and split it all.
                List<Path> toHash = Files.list(directory)
                        .sorted(Comparator.comparing(Path::getFileName))
                        .toList();

                List<StructuredTaskScope.Subtask<String>> subtasks = new ArrayList<>();
                for (var path : toHash) {
                    if (Files.isRegularFile(path)) {
                        subtasks.add(scope.fork(() -> FileHasher.INSTANCE.calculateHash(path)));
                    } else if (Files.isDirectory(path)) {
                        subtasks.add(scope.fork(() -> DirHasher.INSTANCE.calculateHash(path)));
                    }
                }

                scope.join();
                scope.throwIfFailed(e -> new RuntimeException("directory stitching failed", e));

                Hasher megaHasher = sha256.newHasher();

                for (var result : subtasks) {
                    megaHasher.putString(result.get(), StandardCharsets.UTF_8);
                }

                return megaHasher.hash().toString();
            } catch (IOException e) {
                throw new RuntimeException("Failed to list directory", e);
            } catch (InterruptedException e) {
                throw new RuntimeException("Failed joining subtasks from task scope", e);
            }

        }
    }

    private static final AtomicInteger counter = new AtomicInteger(0);
}
