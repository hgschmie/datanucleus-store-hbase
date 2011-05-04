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
import org.apache.hadoop.hbase.filter.Filter;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.OID;
import org.datanucleus.identity.OIDFactory;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.FieldValues;
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
     * @param filter Optional filter for the candidates
     * @return List of objects of the candidate type (or subclass)
     */
    static List getObjectsOfCandidateType(final ExecutionContext ec, final HBaseManagedConnection mconn,
            Class candidateClass, boolean subclasses, boolean ignoreCache, FetchPlan fetchPlan, Filter filter)
    {
        List<AbstractClassMetaData> cmds = 
            MetaDataUtils.getMetaDataForCandidates(candidateClass, subclasses, ec);

        Iterator<AbstractClassMetaData> cmdIter = cmds.iterator();
        List results = new ArrayList();
        while (cmdIter.hasNext())
        {
            AbstractClassMetaData acmd = cmdIter.next();
            results.addAll(getObjectsOfType(ec, mconn, acmd, ignoreCache, fetchPlan, filter));
        }

        return results;
    }

    /**
     * Convenience method to get all objects of the specified type.
     * @param ec Execution Context
     * @param mconn Managed Connection
     * @param cmd Metadata for the type to return
     * @param ignoreCache Whether to ignore the cache
     * @param fp Fetch Plan
     * @param filter Optional filter for the candidates
     * @return List of objects of the candidate type
     */
    static private List getObjectsOfType(final ExecutionContext ec, final HBaseManagedConnection mconn,
            final AbstractClassMetaData cmd, boolean ignoreCache, FetchPlan fp, final Filter filter)
    {
        List results = new ArrayList();

        fp.manageFetchPlanForClass(cmd);
        final int[] fpMembers = fp.getFetchPlanForClass(cmd).getMemberNumbers();
        try
        {
            final ClassLoaderResolver clr = ec.getClassLoaderResolver();

            Iterator<Result> it = (Iterator<Result>) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    Scan scan = new Scan();
                    if (filter != null)
                    {
                        scan.setFilter(filter);
                    }

                    // Retrieve all fetch-plan fields
                    for (int i=0; i<fpMembers.length; i++)
                    {
                        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fpMembers[i]);
                        int relationType = mmd.getRelationType(clr);
                        if (Relation.isRelationSingleValued(relationType) && mmd.isEmbedded())
                        {
                            addColumnsToScanForEmbeddedMember(scan, mmd, HBaseUtils.getTableName(cmd), ec);
                        }
                        else
                        {
                            byte[] familyName = HBaseUtils.getFamilyName(cmd, fpMembers[i]).getBytes();
                            byte[] columnName = HBaseUtils.getQualifierName(cmd, fpMembers[i]).getBytes();
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
                    if (cmd.hasDiscriminatorStrategy())
                    {
                        // Add discriminator column
                        byte[] familyName = HBaseUtils.getFamilyName(cmd.getDiscriminatorMetaData()).getBytes();
                        byte[] columnName = HBaseUtils.getQualifierName(cmd.getDiscriminatorMetaData()).getBytes();
                        scan.addColumn(familyName, columnName);
                    }
                    if (cmd.getIdentityType() == IdentityType.DATASTORE)
                    {
                        // Add datastore identity column
                        byte[] familyName = HBaseUtils.getFamilyName(cmd.getIdentityMetaData()).getBytes();
                        byte[] columnName = HBaseUtils.getQualifierName(cmd.getIdentityMetaData()).getBytes();
                        scan.addColumn(familyName, columnName);
                    }

                    HTable table = mconn.getHTable(HBaseUtils.getTableName(cmd));
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
                    Object obj = getObjectUsingApplicationIdForResult(result, cmd, ec, ignoreCache, fpMembers);
                    if (obj != null)
                    {
                        results.add(obj);
                    }
                }
            }
            else if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                while (it.hasNext())
                {
                    final Result result = it.next();
                    Object obj = getObjectUsingDatastoreIdForResult(result, cmd, ec, ignoreCache, fpMembers);
                    if (obj != null)
                    {
                        results.add(obj);
                    }
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
            final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Check the class for this discriminator value
            Object discValue = HBaseUtils.getDiscriminatorForObject(cmd, result);
            if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
            {
                if (!cmd.getFullClassName().equals(discValue))
                {
                    return null;
                }
            }
            else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP)
            {
                if (!cmd.getDiscriminatorValue().equals(discValue))
                {
                    return null;
                }
            }
        }

        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, 
            new FetchFieldManager(ec, cmd, result));

        Object pc = ec.findObject(id, 
            new FieldValues()
            {
                public void fetchFields(ObjectProvider sm)
                {
                    sm.replaceFields(fpMembers, new FetchFieldManager(ec, cmd, result));
                }
                public void fetchNonLoadedFields(ObjectProvider sm)
                {
                    sm.replaceNonLoadedFields(fpMembers, new FetchFieldManager(ec, cmd, result));
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
            final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers)
    {
        if (cmd.hasDiscriminatorStrategy())
        {
            // Check the class for this discriminator value
            Object discValue = HBaseUtils.getDiscriminatorForObject(cmd, result);
            if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
            {
                if (!cmd.getFullClassName().equals(discValue))
                {
                    return null;
                }
            }
            else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP)
            {
                if (!cmd.getDiscriminatorValue().equals(discValue))
                {
                    return null;
                }
            }
        }

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
            new FieldValues()
            {
                // StateManager calls the fetchFields method
                public void fetchFields(ObjectProvider sm)
                {
                    sm.replaceFields(fpMembers, new FetchFieldManager(ec, cmd, result));
                }
                public void fetchNonLoadedFields(ObjectProvider sm)
                {
                    sm.replaceNonLoadedFields(fpMembers, new FetchFieldManager(ec, cmd, result));
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