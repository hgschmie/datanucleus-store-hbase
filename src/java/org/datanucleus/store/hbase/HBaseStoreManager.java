/**********************************************************************
Copyright (c) 2009 Erik Bengtson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
***********************************************************************/
package org.datanucleus.store.hbase;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.OMFContext;
import org.datanucleus.ObjectManager;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.NucleusConnection;

public class HBaseStoreManager extends AbstractStoreManager
{
    MetaDataListener metadataListener;

    private HBaseConfiguration hbaseConfig; 
    
    private boolean autoCreateTables = false;
    private boolean autoCreateColumns = false;
    
    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param omfContext ObjectManagerFactory context
     */
    public HBaseStoreManager(ClassLoaderResolver clr, OMFContext omfContext)
    {
        super("hbase", clr, omfContext);

        // Handler for metadata
        metadataListener = new HBaseMetaDataListener(this);
        omfContext.getMetaDataManager().registerListener(metadataListener);

        // Handler for persistence process
        persistenceHandler = new HBasePersistenceHandler(this);

        hbaseConfig = new HBaseConfiguration();

        PersistenceConfiguration conf = omfContext.getPersistenceConfiguration();
        boolean autoCreateSchema = conf.getBooleanProperty("datanucleus.autoCreateSchema");
        if (autoCreateSchema)
        {
            autoCreateTables = true;
            autoCreateColumns = true;
        }
        else
        {
            autoCreateTables = conf.getBooleanProperty("datanucleus.autoCreateTables");
            autoCreateColumns = conf.getBooleanProperty("datanucleus.autoCreateColumns");
        }        
        logConfiguration();
    }

    /**
     * Release of resources
     */
    public void close()
    {
        omfContext.getMetaDataManager().deregisterListener(metadataListener);
        super.close();
    }

    public NucleusConnection getNucleusConnection(ObjectManager om)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Accessor for the supported options in string form
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("ApplicationIdentity");
        set.add("TransactionIsolationLevel.read-committed");
        return set;
    }
    
    public HBaseConfiguration getHbaseConfig()
    {
        return hbaseConfig;
    }
    
    public boolean isAutoCreateColumns()
    {
        return autoCreateColumns;
    }
    
    public boolean isAutoCreateTables()
    {
        return autoCreateTables;
    }
}
