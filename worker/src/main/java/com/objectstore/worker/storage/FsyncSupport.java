package com.objectstore.worker.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Durable writes: a file is written to a unique temporary name, forced to the device, then renamed. */
final class FsyncSupport {

    private FsyncSupport() {
    }

    static void writeAndForce(Path file, byte[] data) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
    }

    /** Makes renames and deletions in a directory durable. Best effort where the platform does not allow it. */
    static void fsyncDirectory(Path directory) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException ignored) {
            // Not supported on this file system; the files themselves have been forced.
        }
    }
}
