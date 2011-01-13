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
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.NucleusContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.NucleusConnection;

public class HBaseStoreManager extends AbstractStoreManager
{
    MetaDataListener metadataListener;

    private HBaseConfiguration hbaseConfig; 
    
    private boolean autoCreateTables = false;
    private boolean autoCreateColumns = false;

    private int poolTimeBetweenEvictionRunsMillis; 
    private int poolMinEvictableIdleTimeMillis;

    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param ctx context
     * @param props Properties for the datastore
     */
    public HBaseStoreManager(ClassLoaderResolver clr, NucleusContext ctx, Map<String, Object> props)
    {
        super("hbase", clr, ctx, props);

        // Handler for metadata
        metadataListener = new HBaseMetaDataListener(this);
        ctx.getMetaDataManager().registerListener(metadataListener);

        persistenceHandler = new HBasePersistenceHandler(this);
        hbaseConfig = new HBaseConfiguration();

        boolean autoCreateSchema = getBooleanProperty("datanucleus.autoCreateSchema");
        if (autoCreateSchema)
        {
            autoCreateTables = true;
            autoCreateColumns = true;
        }
        else
        {
            autoCreateTables = getBooleanProperty("datanucleus.autoCreateTables");
            autoCreateColumns = getBooleanProperty("datanucleus.autoCreateColumns");
        }

        // how often should the evictor run
        poolTimeBetweenEvictionRunsMillis = getIntProperty("datanucleus.connectionPool.timeBetweenEvictionRunsMillis");
        if (poolTimeBetweenEvictionRunsMillis == 0)
        {
            poolTimeBetweenEvictionRunsMillis = 15 * 1000; // default, 15 secs
        }

        // how long may a connection sit idle in the pool before it may be evicted
        poolMinEvictableIdleTimeMillis = getIntProperty("datanucleus.connectionPool.minEvictableIdleTimeMillis");
        if (poolMinEvictableIdleTimeMillis == 0)
        {
            poolMinEvictableIdleTimeMillis = 30 * 1000; // default, 30 secs
        }

        logConfiguration();
    }

    protected void registerConnectionMgr()
    {
        super.registerConnectionMgr();
        this.connectionMgr.disableConnectionPool();
    }

    public void close()
    {
        nucleusContext.getMetaDataManager().deregisterListener(metadataListener);
        super.close();
    }

    public NucleusConnection getNucleusConnection(ExecutionContext om)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Method defining which value-strategy to use when the user specifies "native".
     * @param cmd Class requiring the strategy
     * @param absFieldNumber Field of the class
     * @return Just returns "uuid-hex".
     */
    protected String getStrategyForNative(AbstractClassMetaData cmd, int absFieldNumber)
    {
        return "uuid-hex";
    }

    /**
     * Accessor for the supported options in string form.
     * @return Supported options for this store manager
     */
    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("ApplicationIdentity");
        set.add("TransactionIsolationLevel.read-committed");
        set.add("ORM");
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

    public int getPoolMinEvictableIdleTimeMillis()
    {
        return poolMinEvictableIdleTimeMillis;
    }

    public int getPoolTimeBetweenEvictionRunsMillis()
    {
        return poolTimeBetweenEvictionRunsMillis;
    }
}