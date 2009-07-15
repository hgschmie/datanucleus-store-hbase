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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.io.RowResult;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.FetchPlan;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.connection.ManagedConnection;

public class HBaseUtils
{
    public static String getTableName(AbstractClassMetaData acmd)
    {
        if(acmd.getTable()!=null)
        {
            return acmd.getTable();
        }
        return acmd.getName();
    }
    
    public static String getColumnName(AbstractClassMetaData acmd, int fieldNumber)
    {
        AbstractMemberMetaData ammd = acmd.getMetaDataForManagedMemberAtPosition(fieldNumber);
        String columnName = ammd.getColumn();
        if (columnName==null)
        {
            columnName = ammd.getName();
        }
        return HBaseUtils.getTableName(acmd)+":" + columnName;
    }
    
    /**
     * Convenience method to get all objects of the candidate type (and optional subclasses) from the 
     * specified XML connection.
     * @param om ObjectManager
     * @param mconn Managed Connection
     * @param candidateClass Candidate
     * @param subclasses Include subclasses?
     * @param ignoreCache Whether to ignore the cache
     * @return List of objects of the candidate type (or subclass)
     */
    public static List getObjectsOfCandidateType(ObjectManager om, ManagedConnection mconn, Class candidateClass,
            boolean subclasses, boolean ignoreCache)
    {
        List results = new ArrayList();
        HBaseConfiguration config = (HBaseConfiguration)mconn.getConnection();
        try
        {
            final ClassLoaderResolver clr = om.getClassLoaderResolver();
            final AbstractClassMetaData acmd = om.getMetaDataManager().getMetaDataForClass(candidateClass,clr);
            HTable table = new HTable(config,HBaseUtils.getTableName(acmd));

            byte[][] columnNames = new byte[acmd.getMemberCount()][];
            for(int i=0; i<acmd.getMemberCount(); i++)
            {
                columnNames[i] = HBaseUtils.getColumnName(acmd, acmd.getManagedMembers()[i].getAbsoluteFieldNumber()).getBytes();
            }
            Scanner scanner = table.getScanner(columnNames);
            Iterator<RowResult> it = scanner.iterator();
            while(it.hasNext())
            {
                final RowResult result = it.next();
                results.add(om.findObjectUsingAID(clr.classForName(acmd.getFullClassName()), new FieldValues()
                {
                    // StateManager calls the fetchFields method
                    public void fetchFields(StateManager sm)
                    {
                        sm.replaceFields(acmd.getPKMemberPositions(), new HBaseFetchFieldManager(sm, result));
                        sm.replaceFields(acmd.getBasicMemberPositions(clr), new HBaseFetchFieldManager(sm, result));
                    }

                    public void fetchNonLoadedFields(StateManager sm)
                    {
                        sm.replaceNonLoadedFields(acmd.getAllMemberPositions(), new HBaseFetchFieldManager(sm, result));
                    }

                    public FetchPlan getFetchPlanForLoading()
                    {
                        return null;
                    }
                }, ignoreCache, true));

            }
                // we want to get back only "myColumnFamily:columnQualifier1" when we iterate
                table.getScanner(new String[]{"myColumnFamily:columnQualifier1"});
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        return results;
    }
}
