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

import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.XAResource;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.datanucleus.OMFContext;
import org.datanucleus.ObjectManager;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;

/**
 * Implementation of a ConnectionFactory for HBase.
 */
public class ConnectionFactoryImpl implements ConnectionFactory
{
    /** The underlying ObjectManagerFactory context. */
    OMFContext omfContext;

    /**
     * Constructor.
     * @param omfContext The OMF context
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(OMFContext omfContext, String resourceType)
    {
        this.omfContext = omfContext;
    }

    /**
     * Method to get a managed connection enlisting it in the transaction.
     * @param om ObjectManager (or null)
     * @param options Any options for when creating the connection
     * @return The connection
     */
    public ManagedConnection getConnection(ObjectManager om, Map options)
    {
        Map addedOptions = new HashMap();
        if (options != null)
        {
            addedOptions.putAll(options);
        }
        return omfContext.getStoreManager().getConnectionManager().allocateConnection(this, om, addedOptions);
    }

    /**
     * Method to create a new managed connection.
     * @param om ObjectManager
     * @param transactionOptions Transaction options
     * @return The connection
     */
    public ManagedConnection createManagedConnection(ObjectManager om, Map transactionOptions)
    {
        return new ManagedConnectionImpl();
    }

    /**
     * Implementation of a ManagedConnection.
     */
    public static class ManagedConnectionImpl extends AbstractManagedConnection
    {
        public ManagedConnectionImpl()
        {
        }

        public void close()
        {
            if (conn == null)
            {
                return;
            }
            for (int i=0; i<listeners.size(); i++)
            {
                ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPreClose();
            }
            try
            {
                //close something?
            }
            finally
            {
                conn = null;
                for (int i=0; i<listeners.size(); i++)
                {
                    ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPostClose();
                }
            }
        }

        public Object getConnection()
        {
            if (conn == null)
            {
                conn = new HBaseConfiguration();
            }
            return conn;
        }

        public XAResource getXAResource()
        {
            return null;
        }

        public void release()
        {
            //ignore
        }

        public void setManagedResource()
        {
            //ignore
        }
    }
}