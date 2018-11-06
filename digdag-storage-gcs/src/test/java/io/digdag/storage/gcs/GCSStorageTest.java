package io.digdag.storage.gcs;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.ByteStreams;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageObjectSummary;
import io.digdag.storage.gcs.GCSStorageFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import static io.digdag.client.DigdagClient.objectMapper;
import static io.digdag.util.Md5CountInputStream.digestMd5;
import static io.digdag.core.storage.StorageManager.encodeHex;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

public class GCSStorageTest
{
    private static final String TEST_GCS_CREDENTIAL = System.getenv("TEST_GCS_CREDENTIAL");
    private static final String TEST_GCS_PROJECT_ID = System.getenv("TEST_GCS_PROJECT_ID");
    private static final String TEST_GCS_BUCKET_PREFIX = "test-digdag-storage-gcs_";

    private Storage storage;

    @Before
    public void setUp()
            throws Exception
    {
        // need to execute "gcloud auth" in advance.
        StorageOptions.Builder builder = StorageOptions.newBuilder();

        if (TEST_GCS_PROJECT_ID != null && !TEST_GCS_PROJECT_ID.isEmpty())  {
            builder.setProjectId(TEST_GCS_PROJECT_ID);
        }

        if (TEST_GCS_CREDENTIAL != null && !TEST_GCS_CREDENTIAL.isEmpty())  {
            try {
                InputStream in = new ByteArrayInputStream(TEST_GCS_CREDENTIAL.getBytes("utf-8"));
                builder.setCredentials(ServiceAccountCredentials.fromStream(in));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        com.google.cloud.storage.Storage gcs= builder.build().getService();

        String bucket_name = TEST_GCS_BUCKET_PREFIX + UUID.randomUUID().toString();
        Bucket bucket = gcs.get(bucket_name);
        if (bucket == null) {
            // create bucket if not exist.
            bucket = gcs.create(BucketInfo.of(bucket_name));
        }

        ConfigFactory cf = new ConfigFactory(objectMapper());
        Config config = cf.create()
                .set("bucket", bucket_name)  // use unique bucket name
                ;

        storage = new GCSStorageFactory().newStorage(config);
    }

    @Test
    public void putReturnsMd5()
        throws Exception
    {
        String checksum1 = storage.put("key1", 10, contents("0123456789"));
        String checksum2 = storage.put("key2", 5, contents("01234"));
        System.out.println("storage checksum: " + checksum1);
        System.out.println("local checksum: " + md5hex("0123456789"));
        assertThat(checksum1, is(md5hex("0123456789")));
        assertThat(checksum2, is(md5hex("01234")));
    }

    @Test
    public void putGet()
        throws Exception
    {
        storage.put("key/file/1", 3, contents("xxx"));
        storage.put("key/file/2", 1, contents("a"));
        storage.put("key/file/3", 4, contents("data"));
        assertThat(readString(storage.open("key/file/1").getContentInputStream()), is("xxx"));
        assertThat(readString(storage.open("key/file/2").getContentInputStream()), is("a"));
        assertThat(readString(storage.open("key/file/3").getContentInputStream()), is("data"));
    }

    @Test
    public void listAll()
        throws Exception
    {
        storage.put("key/file/2", 1, contents("1"));
        storage.put("key/file/1", 3, contents("xxx"));
        storage.put("key/file/3", 2, contents("12"));

        List<StorageObjectSummary> all = new ArrayList<>();
        storage.list("key", (chunk) -> all.addAll(chunk));

        assertThat(all.size(), is(3));
        assertThat(all.get(0).getKey(), is("key/file/1"));
        assertThat(all.get(0).getContentLength(), is(3L));
        assertThat(all.get(1).getKey(), is("key/file/2"));
        assertThat(all.get(1).getContentLength(), is(1L));
        assertThat(all.get(2).getKey(), is("key/file/3"));
        assertThat(all.get(2).getContentLength(), is(2L));
    }

    @Test
    public void listWithPrefix()
        throws Exception
    {
        storage.put("key1", 1, contents("0"));
        storage.put("test/file/1", 1, contents("1"));
        storage.put("test/file/2", 1, contents("1"));

        List<StorageObjectSummary> all = new ArrayList<>();
        storage.list("test", (chunk) -> all.addAll(chunk));
        assertThat(all.size(), is(2));
        assertThat(all.get(0).getKey(), is("test/file/1"));
        assertThat(all.get(1).getKey(), is("test/file/2"));
    }

    private static Storage.UploadStreamProvider contents(String data)
    {
        return () -> new ByteArrayInputStream(data.getBytes(UTF_8));
    }

    private static String md5hex(String data)
    {
        return md5hex(data.getBytes(UTF_8));
    }

    private static String md5hex(byte[] data)
    {
        return encodeHex(digestMd5(data));
    }

    private static String readString(InputStream in)
        throws IOException
    {
        return new String(ByteStreams.toByteArray(in), UTF_8);
    }
}
