/*
 * Copyright <2021> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package software.amazon.documentdb.jdbc.sshtunnel;

import org.checkerframework.checker.nullness.qual.NonNull;
import sun.nio.ch.FileChannelImpl;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of a {@link FileChannel}-like class to handle locking of
 * processes and threads.
 */
final class DocumentDbMultiThreadFileChannel implements AutoCloseable {
    private final FileChannel fileChannel;

    /**
     * Constructs a new instance of {@link DocumentDbMultiThreadFileChannel}.
     *
     * @param fileChannel the underlying {@link FileChannel} to use.
     */
    private DocumentDbMultiThreadFileChannel(final @NonNull FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    @Override
    public void close() throws Exception {
        fileChannel.close();
    }

    /**
     * Opens or creates a file, returning a file channel to access the file
     * where options is a set of the options specified in the options array.
     *
     * @param path The path of the file to open or create
     * @param options Options specifying how the file is opened
     * @return A new file channel
     * @throws IOException If an I/O error occurs
     */
    public static DocumentDbMultiThreadFileChannel open(final Path path, final OpenOption... options)
            throws IOException {
        final FileChannel fileChannel = FileChannel.open(path, options);
        return new DocumentDbMultiThreadFileChannel(fileChannel);
    }

    /**
     * Attempts to acquire an exclusive lock on this channel's file.
     * An invocation of this method of the form fc.tryLock() behaves in exactly the same way as the invocation
     * <p>
     *       fc.tryLock(0L, Long.MAX_VALUE, false)
     * <p>
     * Additionally, this implementation treats the {@link OverlappingFileLockException} the same as a
     * concurrent lock on the file. In this case, this method automatically returns null indicated the file is locked.
     *
     * @return A lock object representing the newly-acquired lock, or null if the lock could not be acquired because
     * another program holds an overlapping lock
     * @throws IOException If some other I/O error occurs
     */
    public FileLock tryLock() throws IOException {
        try {
            return fileChannel.tryLock();
        } catch (OverlappingFileLockException | NonWritableChannelException e) {
            return null;
        }
    }

    /**
     * Acquires an exclusive lock on this channel's file.
     * An invocation of this method of the form fc.lock() behaves in exactly the same way as the invocation
     * <p>
     *       fc.lock(0L, Long.MAX_VALUE, false)
     * <p>
     * Additionally, this implementation treats the {@link OverlappingFileLockException} the same as a
     * concurrent lock on the file. It automatically retries until the lock is obtained.
     *
     * @return a {@link FileLock} object.
     * @throws IOException if the file lock fails.
     * @throws InterruptedException if the thread is interrupted while sleeping.
     */
    @NonNull
    public FileLock lock() throws IOException, InterruptedException {
        return lock(10);
    }

    /**
     * Acquires an exclusive lock on this channel's file.
     * An invocation of this method of the form fc.lock() behaves in exactly the same way as the invocation
     * <p>
     *       fc.lock(0L, Long.MAX_VALUE, false)
     * <p>
     * Additionally, this implementation treats the {@link OverlappingFileLockException} the same as a
     * concurrent lock on the file. It automatically retries until the lock is obtained.
     *
     * @param pollTimeMS the amount of time, in milliseconds, to sleep between retries in the case an
     * {@link OverlappingFileLockException} is detected.
     *
     * @return A lock object representing the newly-acquired lock
     * @throws IOException If the file lock fails.
     * @throws InterruptedException if the thread is interrupted while sleeping.
     */
    @NonNull
    public FileLock lock(final int pollTimeMS) throws IOException, InterruptedException {
        FileLock fileLock;
        do {
            try {
                fileLock = fileChannel.lock();
            } catch (OverlappingFileLockException | NonWritableChannelException e) {
                // This is meant to handle multiple threads locking a single file.
                TimeUnit.MILLISECONDS.sleep(pollTimeMS);
                fileLock = null;
            }
        } while (fileLock == null);
        return fileLock;
    }

    public boolean isOpen() {
        return fileChannel.isOpen();
    }

    FileChannel getFileChannel() {
        return fileChannel;
    }
}
