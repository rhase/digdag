package io.digdag.storage.gcs;

import java.util.List;
import java.util.Arrays;
import com.google.inject.Module;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import io.digdag.spi.StorageFactory;
import io.digdag.spi.Extension;

public class GCSStorageExtension
        implements Extension
{
    @Override
    public List<Module> getModules()
    {
        return Arrays.asList(new GCSStorageModule());
    }

    public static class GCSStorageModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            Multibinder.newSetBinder(binder, StorageFactory.class)
                    .addBinding().to(GCSStorageFactory.class).in(Scopes.SINGLETON);
        }
    }
}
