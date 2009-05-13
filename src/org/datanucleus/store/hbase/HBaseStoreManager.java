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
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.OMFContext;
import org.datanucleus.ObjectManager;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.DefaultCandidateExtent;
import org.datanucleus.store.Extent;
import org.datanucleus.store.NucleusConnection;
import org.datanucleus.store.exceptions.NoExtentException;

public class HBaseStoreManager extends AbstractStoreManager
{
    /**
     * Constructor.
     * @param clr ClassLoader resolver
     * @param omfContext ObjectManagerFactory context
     */
    public HBaseStoreManager(ClassLoaderResolver clr, OMFContext omfContext)
    {
        super("hbase", clr, omfContext);

        // Handler for persistence process
        persistenceHandler = new HBasePersistenceHandler(this);

        logConfiguration();
    }

    /**
     * Get the date/time of the datastore.
     * @return Date/time of the datastore
     */
    public Date getDatastoreDate()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Accessor for an Extent for a class.
     * @param om The Object Manager
     * @param c The class requiring the Extent
     * @param subclasses Whether to include subclasses of 'c'
     * @return The Extent.
     */
    public Extent getExtent(final ObjectManager om, final Class c, final boolean subclasses)
    {
        AbstractClassMetaData cmd = getMetaDataManager().getMetaDataForClass(c, om.getClassLoaderResolver());
        if (!cmd.isRequiresExtent())
        {
            throw new NoExtentException(c.getName());
        }

        return new DefaultCandidateExtent(om, c, subclasses, cmd);
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
}
