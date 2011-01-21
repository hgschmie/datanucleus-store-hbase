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

import java.util.Map;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.util.Localiser;

/**
 * Implementation of a ConnectionFactory for HBase.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER_HBASE = Localiser.getInstance(
        "org.datanucleus.store.hbase.Localisation", HBaseStoreManager.class.getClassLoader());

    private HBaseConnectionPool connectionPool;

    private int poolMinEvictableIdleTimeMillis = 0;

    /**
     * Constructor.
     * @param storeMgr The context
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);

        // how often should the evictor run
        int poolTimeBetweenEvictionRunsMillis = storeMgr.getIntProperty("datanucleus.connectionPool.timeBetweenEvictionRunsMillis");
        if (poolTimeBetweenEvictionRunsMillis == 0)
        {
            poolTimeBetweenEvictionRunsMillis = 15 * 1000; // default, 15 secs
        }

        // how long may a connection sit idle in the pool before it may be evicted
        poolMinEvictableIdleTimeMillis = storeMgr.getIntProperty("datanucleus.connectionPool.minEvictableIdleTimeMillis");
        if (poolMinEvictableIdleTimeMillis == 0)
        {
            poolMinEvictableIdleTimeMillis = 30 * 1000; // default, 30 secs
        }

        connectionPool = new HBaseConnectionPool();
        connectionPool.setTimeBetweenEvictionRunsMillis(poolTimeBetweenEvictionRunsMillis);

        String url = storeMgr.getConnectionURL();
        if (!url.startsWith("hbase"))
        {
            throw new NucleusException(LOCALISER_HBASE.msg("HBase.URLInvalid", url));
        }

        // Split the URL into local, or "host:port"
        String hbaseStr = url.substring(5); // Omit the hbase prefix, and any colon
        if (hbaseStr.startsWith(":"))
        {
            hbaseStr = hbaseStr.substring(1);
        }

        if (hbaseStr.length() > 0)
        {
            // Remote, so specify server
            String serverName = hbaseStr;
            String portName = "60000";
            if (hbaseStr.indexOf(':') > 0)
            {
                serverName = hbaseStr.substring(0, hbaseStr.indexOf(':'));
                portName = hbaseStr.substring(hbaseStr.indexOf(':')+1);
            }
            HBaseStoreManager hbaseStoreMgr = (HBaseStoreManager)storeMgr;
            hbaseStoreMgr.getHbaseConfig().set("hbase.zookeeper.quorum", serverName);
            hbaseStoreMgr.getHbaseConfig().set("hbase.master", serverName + ":" + portName);
        }
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the {@link org.datanucleus.Transaction} 
     * associated to the <code>poolKey</code> if "enlist" is set to true.
     * @param poolKey the pool that is bound the connection during its lifecycle (or null)
     * @param options Any options for then creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(Object poolKey, Map transactionOptions)
    {
        HBaseStoreManager storeManager = (HBaseStoreManager) storeMgr;
        HBaseManagedConnection managedConnection = connectionPool.getPooledConnection();
        if (managedConnection == null) 
        {
            managedConnection = new HBaseManagedConnection(storeManager.getHbaseConfig());
            managedConnection.setIdleTimeoutMills(poolMinEvictableIdleTimeMillis);
            connectionPool.registerConnection(managedConnection);
        }
        managedConnection.incrementReferenceCount();
        return managedConnection;
    }
}