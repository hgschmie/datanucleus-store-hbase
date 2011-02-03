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
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.Relation;
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

        final ClassLoaderResolver clr = ec.getClassLoaderResolver();

        final AbstractClassMetaData acmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, clr);
        results.addAll(getObjectsOfType(ec, mconn, acmd, ignoreCache, fetchPlan));

        if (subclasses)
        {
            // TODO Cater for inheritance here - maybe they persist all into the same table, with discriminator?
            String[] subclassNames = ec.getMetaDataManager().getSubclassesForClass(candidateClass.getName(), true);
            if (subclassNames != null && subclassNames.length > 0)
            {
                for (int i=0;i<subclassNames.length;i++)
                {
                    AbstractClassMetaData subcmd = ec.getMetaDataManager().getMetaDataForClass(subclassNames[i], clr);
                    results.addAll(getObjectsOfType(ec, mconn, subcmd, ignoreCache, fetchPlan));
                }
            }
        }

        return results;
    }

    /**
     * Convenience method to get all objects of the specified type.
     * @param ec Execution Context
     * @param mconn Managed Connection
     * @param cmd Metadata for the type to return
     * @param ignoreCache Whether to ignore the cache
     * @param fetchPlan Fetch Plan
     * @return List of objects of the candidate type
     */
    static private List getObjectsOfType(final ExecutionContext ec, final HBaseManagedConnection mconn,
            final AbstractClassMetaData cmd, boolean ignoreCache, FetchPlan fetchPlan)
    {
        List results = new ArrayList();
        try
        {
            final ClassLoaderResolver clr = ec.getClassLoaderResolver();

            Iterator<Result> it = (Iterator<Result>) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    HTable table = mconn.getHTable(HBaseUtils.getTableName(cmd));

                    // Set up to retrieve all fields, plus optional version and datastore identity columns
                    // TODO Respect FetchPlan
                    int[] fieldNumbers =  cmd.getAllMemberPositions();
                    Scan scan = new Scan();
                    for (int i=0; i<fieldNumbers.length; i++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                        int relationType = mmd.getRelationType(clr);
                        if ((relationType == Relation.ONE_TO_ONE_UNI || relationType == Relation.ONE_TO_ONE_BI) && mmd.isEmbedded())
                        {
                            addColumnsToScanForEmbeddedMember(scan, mmd, HBaseUtils.getTableName(cmd), ec);
                        }
                        else
                        {
                            byte[] familyName = HBaseUtils.getFamilyName(cmd, fieldNumbers[i]).getBytes();
                            byte[] columnName = HBaseUtils.getQualifierName(cmd, fieldNumbers[i]).getBytes();
                            scan.addColumn(familyName, columnName);
                        }
                    }
                    if (cmd.hasVersionStrategy() && cmd.getVersionMetaData().getFieldName() == null)
                    {
                        // Add version column
                        byte[] familyName = HBaseUtils.getFamilyName(cmd.getVersionMetaData()).getBytes();
                        byte[] columnName = HBaseUtils.getQualifierName(cmd.getVersionMetaData()).getBytes();
                        scan.addColumn(familyName, columnName);
                    }
                    if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        // Add datastore identity column
                        byte[] familyName = HBaseUtils.getFamilyName(cmd.getIdentityMetaData()).getBytes();
                        byte[] columnName = HBaseUtils.getQualifierName(cmd.getIdentityMetaData()).getBytes();
                        scan.addColumn(familyName, columnName);
                    }
                    ResultScanner scanner = table.getScanner(scan);
                    Iterator<Result> it = scanner.iterator();
                    return it;
                }
            });

            if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                while (it.hasNext())
                {
                    final Result result = it.next();
                    results.add(getObjectUsingApplicationIdForResult(result, cmd, ec, ignoreCache));
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                while (it.hasNext())
                {
                    final Result result = it.next();
                    results.add(getObjectUsingDatastoreIdForResult(result, cmd, ec, ignoreCache));
                }
            }
        }
        catch (PrivilegedActionException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e.getCause());
        }
        return results;
    }

    protected static Object getObjectUsingApplicationIdForResult(final Result result, final AbstractClassMetaData cmd,
            final ExecutionContext ec, boolean ignoreCache)
    {
        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, 
            new FetchFieldManager(ec, cmd, result));

        Object pc = ec.findObject(id, 
            new FieldValues2()
            {
                public void fetchFields(ObjectProvider sm)
                {
                    sm.replaceFields(cmd.getAllMemberPositions(), 
                        new FetchFieldManager(ec, cmd, result));
                }
                public void fetchNonLoadedFields(ObjectProvider sm)
                {
                    sm.replaceNonLoadedFields(cmd.getAllMemberPositions(), 
                        new FetchFieldManager(ec, cmd, result));
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, null, ignoreCache);

        if (cmd.hasVersionStrategy())
        {
            // Set the version on the object
            ObjectProvider sm = ec.findObjectProvider(pc);
            Object version = null;
            if (cmd.getVersionMetaData().getFieldName() != null)
            {
                // Set the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(cmd.getVersionMetaData().getFieldName());
                version = sm.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = HBaseUtils.getSurrogateVersionForObject(cmd, result);
            }
            sm.setVersion(version);
        }

        return pc;
    }

    protected static Object getObjectUsingDatastoreIdForResult(final Result result, final AbstractClassMetaData cmd,
            final ExecutionContext ec, boolean ignoreCache)
    {
        String dsidFamilyName = HBaseUtils.getFamilyName(cmd.getIdentityMetaData());
        String dsidColumnName = HBaseUtils.getQualifierName(cmd.getIdentityMetaData());
        OID id = null;
        try
        {
            byte[] bytes = result.getValue(dsidFamilyName.getBytes(), dsidColumnName.getBytes());
            if (bytes != null)
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                Object key = ois.readObject();
                id = OIDFactory.getInstance(ec.getNucleusContext(), cmd.getFullClassName(), key);
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

        Object pc = ec.findObject(id, 
            new FieldValues2()
            {
                // StateManager calls the fetchFields method
                public void fetchFields(ObjectProvider sm)
                {
                    sm.replaceFields(cmd.getAllMemberPositions(), new FetchFieldManager(ec, cmd, result));
                }
                public void fetchNonLoadedFields(ObjectProvider sm)
                {
                    sm.replaceNonLoadedFields(cmd.getAllMemberPositions(), new FetchFieldManager(ec, cmd, result));
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            }, null, ignoreCache);

        if (cmd.hasVersionStrategy())
        {
            // Set the version on the object
            ObjectProvider sm = ec.findObjectProvider(pc);
            Object version = null;
            if (cmd.getVersionMetaData().getFieldName() != null)
            {
                // Set the version from the field value
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(cmd.getVersionMetaData().getFieldName());
                version = sm.provideField(verMmd.getAbsoluteFieldNumber());
            }
            else
            {
                // Get the surrogate version from the datastore
                version = HBaseUtils.getSurrogateVersionForObject(cmd, result);
            }
            sm.setVersion(version);
        }

        return pc;
    }

    private static void addColumnsToScanForEmbeddedMember(Scan scan, AbstractMemberMetaData mmd, String tableName, ExecutionContext ec)
    {
        EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
        for (int i=0;i<embmmds.length;i++)
        {
            AbstractMemberMetaData embMmd = embmmds[i];
            int relationType = embMmd.getRelationType(clr);
            if ((relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI) && embMmd.isEmbedded())
            {
                addColumnsToScanForEmbeddedMember(scan, embMmd, tableName, ec);
            }
            else
            {
                byte[] familyName = HBaseUtils.getFamilyName(mmd, i, tableName).getBytes();
                byte[] columnName = HBaseUtils.getQualifierName(mmd, i).getBytes();
                scan.addColumn(familyName, columnName);
            }
        }
    }
}