package io.digdag.storage.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.digdag.util.RetryExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.digdag.core.storage.StorageManager.encodeHex;

public class GCSStorage
    implements Storage
{
    private static Logger logger = LoggerFactory.getLogger(GCSStorage.class);

    private final Config config;
    private final Bucket bucketObj;
    private final ExecutorService uploadExecutor;

    public GCSStorage(Config config, com.google.cloud.storage.Storage gcs, String bucket)
    {
        checkArgument(!isNullOrEmpty(bucket), "bucket is null or empty");

        this.config = config;
        this.bucketObj = gcs.get(bucket);
        if (bucketObj == null) {
            throw new ConfigException("Bucket does not exist: " + bucket);
        }

        this.uploadExecutor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                        .setNameFormat("storage-gcs-upload-transfer-%d")
                        .build());
    }

    @Override
    public StorageObject open(String key)
        throws StorageFileNotFoundException
    {
        checkArgument(key != null, "key is null");

        String errorMessage = "opening file bucket " + bucketObj.getName() + " key " + key;
        Blob blob = getWithRetry(errorMessage, () -> bucketObj.get(key));
        final long actualSize = blob.getSize();
        InputStream input = Channels.newInputStream(blob.reader());

        return new StorageObject(input, actualSize);
    }

    @Override
    public String put(String key, long contentLength,
            UploadStreamProvider payload)
        throws IOException
    {
        try {
            return getRetryExecutor()
                .onRetry((exception, retryCount, retryLimit, retryWait) -> {
                    logger.warn("Retrying uploading file bucket "+bucketObj.getName()+" key "+key+" error: "+exception);
                })
                .retryIf((exception) -> {
                    if (exception instanceof IOException || exception instanceof InterruptedException) {
                        return false;
                    }
                    return true;
                })
                .runInterruptible(() -> {
                    try (InputStream in = payload.open()) {
                        Blob blob = bucketObj.create(key, in, "text/plain");
                        byte[] md5 = Base64.getDecoder().decode(blob.getMd5().getBytes());
                        return encodeHex(md5);
                    }
                });
        }
        catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        catch (RetryExecutor.RetryGiveupException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    @Override
    public void list(String keyPrefix, FileListing callback)
    {
        checkArgument(keyPrefix != null, "keyPrefix is null");

        String errorMessage = "listing files on bucket " + bucketObj.getName() + " prefix " + keyPrefix;
        Page<Blob> page = getWithRetry(errorMessage, () -> bucketObj.list(BlobListOption.prefix(keyPrefix)));

        while (true) {
            List<StorageObjectSummary> sos = new ArrayList<>();
            page.getValues().forEach(
                    (blob) -> sos.add(
                        StorageObjectSummary.builder()
                        .key(blob.getName())
                        .contentLength(blob.getSize())
                        .lastModified(Instant.ofEpochMilli(blob.getUpdateTime()))
                        .build()
                        )
                    );

            callback.accept(sos);

            if (!page.hasNextPage()) {
                break;
            }

            page = page.getNextPage();
        }
    }

    @Override
    public Optional<DirectDownloadHandle> getDirectDownloadHandle(String key) {
        final long secondsToExpire = config.get("direct_download_expiration", Long.class, 10L*60);

        String errorMessage = "opening file bucket " + bucketObj.getName() + " key " + key;
        Blob blob = getWithRetry(errorMessage, () -> bucketObj.get(key));
        URL url = blob.signUrl(secondsToExpire, TimeUnit.SECONDS);

        return Optional.of(DirectDownloadHandle.of(url));
    }

    @Override
    public Optional<DirectUploadHandle> getDirectUploadHandle(String key) {
        final long secondsToExpire = config.get("direct_upload_expiration", Long.class, 10L*60);

        String errorMessage = "opening file bucket " + bucketObj.getName() + " key " + key;
        Blob blob = getWithRetry(errorMessage, () -> bucketObj.get(key));
        URL url = blob.signUrl(secondsToExpire, TimeUnit.SECONDS);

        return Optional.of(DirectUploadHandle.of(url));
    }

    private RetryExecutor getRetryExecutor()
    {
        return RetryExecutor.retryExecutor();
    }

    private <T> T getWithRetry(String message, Callable<T> callable)
    {
        try {
            return getRetryExecutor()
                    .onRetry((exception, retryCount, retryLimit, retryWait) -> {
                        logger.warn(String.format("Retrying %s (%d/%d): %s", message, retryCount, retryLimit, exception));
                    })
                    .runInterruptible(() -> callable.call());
        }
        catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        catch (RetryExecutor.RetryGiveupException ex) {
            Exception cause = ex.getCause();
            if(cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }
}
