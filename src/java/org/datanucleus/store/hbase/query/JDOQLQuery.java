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
package org.datanucleus.store.hbase.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.util.NucleusLogger;

/**
 * Implementation of JDOQL for HBase datastores.
 */
public class JDOQLQuery extends AbstractJDOQLQuery
{
    /**
     * Constructs a new query instance that uses the given persistence manager.
     * @param om the associated ObjectManager for this query.
     */
    public JDOQLQuery(ObjectManager om)
    {
        this(om, (JDOQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param om The ObjectManager
     * @param q The query from which to copy criteria.
     */
    public JDOQLQuery(ObjectManager om, JDOQLQuery q)
    {
        super(om, q);
    }

    /**
     * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
     * @param om The persistence manager
     * @param query The query string
     */
    public JDOQLQuery(ObjectManager om, String query)
    {
        super(om, query);
    }

    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = om.getStoreManager().getConnection(om);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL", getSingleStringQuery(), null));
            }
            List candidates = null;
            if (candidateCollection != null)
            {
                candidates = new ArrayList(candidateCollection);
            }
            else if (candidateExtent != null)
            {
            	candidates = new ArrayList();
            	Iterator iter = candidateExtent.iterator();
            	while (iter.hasNext())
            	{
            		candidates.add(iter.next());
            	}
            }
            else
            {
                candidates = HBaseUtils.getObjectsOfCandidateType(om, mconn, candidateClass, subclasses,
                    ignoreCache);
            }

            // Apply any result restrictions to the results
            JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this, candidates, compilation,
                parameters, om.getClassLoaderResolver());
            Collection results = resultMapper.execute(true, true, true, true, true);

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL", 
                    "" + (System.currentTimeMillis() - startTime)));
            }

            return results;
        }
        finally
        {
            mconn.release();
        }
    }
}