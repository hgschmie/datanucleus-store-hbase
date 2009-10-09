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

import org.datanucleus.OMFContext;
import org.datanucleus.ObjectManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;

/**
 * Implementation of a ConnectionFactory for HBase.
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    private HBaseConnectionPool connectionPool;

    /**
     * Constructor.
     * @param omfContext The OMF context
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(OMFContext omfContext, String resourceType)
    {
        super(omfContext, resourceType);
        HBaseStoreManager storeManager = (HBaseStoreManager) omfContext.getStoreManager();
        connectionPool = new HBaseConnectionPool();
        connectionPool.setTimeBetweenEvictionRunsMillis(storeManager.getPoolTimeBetweenEvictionRunsMillis());
    }

    /**
     * Method to create a new managed connection.
     * @param om ObjectManager
     * @param transactionOptions Transaction options
     * @return The connection
     */
    public ManagedConnection createManagedConnection(ObjectManager om, Map transactionOptions)
    {
        HBaseStoreManager storeManager = (HBaseStoreManager) om.getStoreManager();
               
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