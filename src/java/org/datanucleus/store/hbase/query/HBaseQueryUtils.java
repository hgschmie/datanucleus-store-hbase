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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OID;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.FieldValues2;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.hbase.HBaseManagedConnection;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.hbase.fieldmanager.FetchFieldManager;

class HBaseQueryUtils
{
    /**
     * Convenience method to get all objects of the candidate type (and optional subclasses) from the 
     * specified connection.
     * @param ec Execution Context
     * @param mconn Managed Connection
     * @param candidateClass Candidate
     * @param subclasses Include subclasses?
     * @param ignoreCache Whether to ignore the cache
     * @param fetchPlan Fetch Plan
     * @return List of objects of the candidate type (or subclass)
     */
    static List getObjectsOfCandidateType(final ExecutionContext ec, final HBaseManagedConnection mconn,
            Class candidateClass, boolean subclasses, boolean ignoreCache, FetchPlan fetchPlan)
    {
        List results = new ArrayList();
        try
        {
            final ClassLoaderResolver clr = ec.getClassLoaderResolver();
            final AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass,clr);

            Iterator<Result> it = (Iterator<Result>) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));

                    // Set up to retrieve all fields, plus optional version and datastore identity columns
                    // TODO Respect FetchPlan
                    int[] fieldNumbers =  acmd.getAllMemberPositions();
                    Scan scan = new Scan();
                    for (int i=0; i<fieldNumbers.length; i++)
                    {
                        byte[] familyName = HBaseUtils.getFamilyName(acmd,fieldNumbers[i]).getBytes();
                        byte[] columnName = HBaseUtils.getQualifierName(acmd, fieldNumbers[i]).getBytes();
                        scan.addColumn(familyName, columnName);
                    }
                    if (acmd.hasVersionStrategy() && acmd.getVersionMetaData().getFieldName() == null)
                    {
                        // Add version column
                        byte[] familyName = HBaseUtils.getFamilyName(acmd.getVersionMetaData()).getBytes();
                        byte[] columnName = HBaseUtils.getQualifierName(acmd.getVersionMetaData()).getBytes();
                        scan.addColumn(familyName, columnName);
                    }
                    if (acmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        // Add datastore identity column
                        byte[] familyName = HBaseUtils.getFamilyName(acmd.getIdentityMetaData()).getBytes();
                        byte[] columnName = HBaseUtils.getQualifierName(acmd.getIdentityMetaData()).getBytes();
                        scan.addColumn(familyName, columnName);
                    }
                    ResultScanner scanner = table.getScanner(scan);
                    Iterator<Result> it = scanner.iterator();
                    return it;
                }
            });

            if (acmd.getIdentityType() == IdentityType.APPLICATION)
            {
                while(it.hasNext())
                {
                    final Result result = it.next();
                    Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, acmd, null, 
                        false, new FetchFieldManager(ec, acmd, result));
                    results.add(ec.findObject(id, 
                        new FieldValues2()
                        {
                            public void fetchFields(ObjectProvider sm)
                            {
                                sm.replaceFields(acmd.getAllMemberPositions(), new FetchFieldManager(ec, acmd, result));
                            }
                            public void fetchNonLoadedFields(ObjectProvider sm)
                            {
                                sm.replaceNonLoadedFields(acmd.getAllMemberPositions(), new FetchFieldManager(ec, acmd, result));
                            }
                            public FetchPlan getFetchPlanForLoading()
                            {
                                return null;
                            }
                        }, null, ignoreCache));
                }
            }
            else if (acmd.getIdentityType() == IdentityType.DATASTORE)
            {
                String dsidFamilyName = HBaseUtils.getFamilyName(acmd.getIdentityMetaData());
                String dsidColumnName = HBaseUtils.getQualifierName(acmd.getIdentityMetaData());
                while (it.hasNext())
                {
                    final Result result = it.next();
                    OID id = null;
                    try
                    {
                        byte[] bytes = result.getValue(dsidFamilyName.getBytes(), dsidColumnName.getBytes());
                        if (bytes != null)
                        {
                            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                            ObjectInputStream ois = new ObjectInputStream(bis);
                            Object key = ois.readObject();
                            id = OIDFactory.getInstance(ec.getNucleusContext(), acmd.getFullClassName(), key);
                            ois.close();
                            bis.close();
                        }
                        else
                        {
                            throw new NucleusException("Retrieved identity for family=" + dsidFamilyName + " column=" + dsidColumnName + " IS NULL");
                        }
                    }
                    catch (Exception e)
                    {
                        throw new NucleusException(e.getMessage(), e);
                    }
                    results.add(ec.findObject(id, 
                        new FieldValues2()
                        {
                            // StateManager calls the fetchFields method
                            public void fetchFields(ObjectProvider sm)
                            {
                                sm.replaceFields(acmd.getAllMemberPositions(), 
                                    new FetchFieldManager(ec, acmd, result));
                            }
                            public void fetchNonLoadedFields(ObjectProvider sm)
                            {
                                sm.replaceNonLoadedFields(acmd.getAllMemberPositions(), 
                                    new FetchFieldManager(ec, acmd, result));
                            }
                            public FetchPlan getFetchPlanForLoading()
                            {
                                return null;
                            }
                        }, null, ignoreCache));
                }
            }
        }
        catch (PrivilegedActionException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e.getCause());
        }
        return results;
    }
}