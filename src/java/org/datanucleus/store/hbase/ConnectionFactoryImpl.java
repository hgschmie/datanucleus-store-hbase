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

    /**
     * Constructor.
     * @param storeMgr The context
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);
        connectionPool = new HBaseConnectionPool();
        connectionPool.setTimeBetweenEvictionRunsMillis(((HBaseStoreManager)storeMgr).getPoolTimeBetweenEvictionRunsMillis());

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
            HBaseStoreManager hbaseStoreMgr = (HBaseStoreManager)storeMgr;
            hbaseStoreMgr.getHbaseConfig().set("hbase.zookeeper.quorum", hbaseStr);
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
            managedConnection.setIdleTimeoutMills(storeManager.getPoolMinEvictableIdleTimeMillis());
            connectionPool.registerConnection(managedConnection);
        }
        managedConnection.incrementReferenceCount();
        return managedConnection;
    }
}