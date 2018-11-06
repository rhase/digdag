package io.digdag.storage.gcs;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.StorageOptions;
import io.digdag.client.config.Config;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFactory;

import java.io.*;

public class GCSStorageFactory
        implements StorageFactory
{
    @Override
    public String getType()
    {
        return "gcs";
    }

    public Storage newStorage(Config config)
    {
        // Need to execute "gcloud auth" in advance.
        StorageOptions.Builder builder = StorageOptions.newBuilder();

        if (config.has("credentialfile")) {
            String credentialfile = config.get("credentialfile", String.class);
            try {
                builder.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credentialfile)));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        com.google.cloud.storage.Storage gcs= builder.build().getService();

        String bucket = config.get("bucket", String.class);

        return new GCSStorage(config, gcs, bucket);
    }
}
