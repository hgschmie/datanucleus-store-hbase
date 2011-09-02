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
2011 Andy Jefferson - support for datastore identity, versions, discriminators,
                      localisation
    ...
***********************************************************************/
package org.datanucleus.store.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.VersionHelper;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.hbase.fieldmanager.FetchFieldManager;
import org.datanucleus.store.hbase.fieldmanager.StoreFieldManager;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

/**
 * Persistence handler for HBase, providing insert, update, delete, and find handling.
 */
public class HBasePersistenceHandler extends AbstractPersistenceHandler
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.hbase.Localisation", HBaseStoreManager.class.getClassLoader());

    protected final HBaseStoreManager storeMgr;

    public HBasePersistenceHandler(StoreManager storeMgr)
    {
        this.storeMgr = (HBaseStoreManager) storeMgr;
    }

    public void close()
    {
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#insertObjects(org.datanucleus.store.ObjectProvider[])
     */
    @Override
    public void insertObjects(ObjectProvider... ops)
    {
        if (ops.length == 0)
        {
            insertObject(ops[0]);
            return;
        }

        // TODO Auto-generated method stub
        super.insertObjects(ops);
    }

    public void insertObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(op);

        if (!storeMgr.managesClass(op.getClassMetaData().getFullClassName()))
        {
            storeMgr.addClass(op.getClassMetaData().getFullClassName(),op.getExecutionContext().getClassLoaderResolver());
        }

        AbstractClassMetaData cmd = op.getClassMetaData();
        boolean enforceUniquenessInApp = storeMgr.getBooleanProperty("datanucleus.hbase.enforceUniquenessInApplication", false);
        if (enforceUniquenessInApp)
        {
            NucleusLogger.DATASTORE_PERSIST.info("User requesting to enforce uniqueness of object identity in their application, so not checking for existence");
        }
        else
        {
            if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                // Enforce uniqueness of datastore rows in this plugin
                try
                {
                    locateObject(op);
                    throw new NucleusUserException(LOCALISER.msg("HBase.Insert.ObjectWithIdAlreadyExists", 
                        op.toPrintableID(), op.getInternalObjectId()));
                }
                catch (NucleusObjectNotFoundException onfe)
                {
                    // Do nothing since object with this id doesn't exist
                }
            }
        }

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(op.getExecutionContext());
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("HBase.Insert.Start", 
                    op.toPrintableID(), op.getInternalObjectId()));
            }

            HTable table = mconn.getHTable(HBaseUtils.getTableName(cmd));
            Put put = HBaseUtils.getPutForObject(op);
            Delete delete = HBaseUtils.getDeleteForObject(op);

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                String familyName = HBaseUtils.getFamilyName(cmd.getIdentityMetaData());
                String columnName = HBaseUtils.getQualifierName(cmd.getIdentityMetaData());
                Object key = ((OID)op.getInternalObjectId()).getKeyValue();
                try
                {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(key);
                    oos.flush();
                    put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                    oos.close();
                    bos.close();
                }
                catch (IOException e)
                {
                    throw new NucleusException(e.getMessage(), e);
                }
            }

            if (cmd.hasDiscriminatorStrategy())
            {
                // Add discriminator field
                DiscriminatorMetaData discmd = cmd.getDiscriminatorMetaData();
                String discVal = null;
                if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
                {
                    discVal = cmd.getFullClassName();
                }
                else
                {
                    discVal = discmd.getValue();
                }
                String familyName = HBaseUtils.getFamilyName(discmd);
                String columnName = HBaseUtils.getQualifierName(discmd);

                put.add(familyName.getBytes(), columnName.getBytes(), discVal.getBytes());
            }

            if (cmd.isVersioned())
            {
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                String familyName = HBaseUtils.getFamilyName(vermd);
                String columnName = HBaseUtils.getQualifierName(vermd);
                if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    long versionNumber = 1;
                    op.setTransactionalVersion(Long.valueOf(versionNumber));
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER.msg("HBase.Insert.ObjectPersistedWithVersion",
                            op.toPrintableID(), op.getInternalObjectId(), "" + versionNumber));
                    }
                    if (vermd.getFieldName() != null)
                    {
                        AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                        Object verFieldValue = Long.valueOf(versionNumber);
                        if (verMmd.getType() == int.class || verMmd.getType() == Integer.class)
                        {
                            verFieldValue = Integer.valueOf((int)versionNumber);
                        }
                        op.replaceField(verMmd.getAbsoluteFieldNumber(), verFieldValue);
                    }
                    else
                    {
                        put.add(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes(versionNumber));
                    }
                }
                else if (vermd.getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    Date date = new Date();
                    Timestamp ts = new Timestamp(date.getTime());
                    op.setTransactionalVersion(ts);
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER.msg("HBase.Insert.ObjectPersistedWithVersion",
                            op.toPrintableID(), op.getInternalObjectId(), "" + ts));
                    }
                    if (vermd.getFieldName() != null)
                    {
                        AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                        op.replaceField(verMmd.getAbsoluteFieldNumber(), ts);
                    }
                    else
                    {
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(ts);
                            put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                }
            }

            StoreFieldManager fm = new StoreFieldManager(op, put, delete, true);
            op.provideFields(cmd.getAllMemberPositions(), fm);

            table.put(put);
            table.close();

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("HBase.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (storeMgr.getRuntimeManager() != null)
            {
                storeMgr.getRuntimeManager().incrementInsertCount();
            }
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void updateObject(ObjectProvider op, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(op);

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(op.getExecutionContext());
        try
        {
            long startTime = System.currentTimeMillis();
            AbstractClassMetaData cmd = op.getClassMetaData();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                StringBuffer fieldStr = new StringBuffer();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("HBase.Update.Start", 
                    op.toPrintableID(), op.getInternalObjectId(), fieldStr.toString()));
            }

            HTable table = mconn.getHTable(HBaseUtils.getTableName(cmd));
            if (cmd.isVersioned())
            {
                // Optimistic checking of version
                Result result = HBaseUtils.getResultForObject(op, table);
                Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, op.getExecutionContext());
                VersionHelper.performVersionCheck(op, datastoreVersion, cmd.getVersionMetaDataForClass());
            }

            Put put = HBaseUtils.getPutForObject(op);
            Delete delete = HBaseUtils.getDeleteForObject(op);
            if (cmd.isVersioned())
            {
                // Version object so calculate version to store with
                Object currentVersion = op.getTransactionalVersion();
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                Object nextVersion = VersionHelper.getNextVersion(vermd.getVersionStrategy(), currentVersion);
                op.setTransactionalVersion(nextVersion);

                if (vermd.getFieldName() != null)
                {
                    // Update the field version value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    op.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);
                }
                else
                {
                    // Update the stored surrogate value
                    String familyName = HBaseUtils.getFamilyName(vermd);
                    String columnName = HBaseUtils.getQualifierName(vermd);
                    if (nextVersion instanceof Long)
                    {
                        put.add(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes((Long)nextVersion));
                    }
                    else if (nextVersion instanceof Integer)
                    {
                        put.add(familyName.getBytes(), columnName.getBytes(), Bytes.toBytes((Integer)nextVersion));
                    }
                    else
                    {
                        put.add(familyName.getBytes(), columnName.getBytes(), ((String)nextVersion).getBytes());
                    }
                }
            }

            StoreFieldManager fm = new StoreFieldManager(op, put, delete, false);
            op.provideFields(fieldNumbers, fm);
            if (!put.isEmpty())
            {
                table.put(put);
            }
            if (!delete.isEmpty())
            {
                // only delete if there are columns to delete. Otherwise an empty delete would cause the
                // entire row to be deleted
                table.delete(delete);            
            }
            table.close();

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("HBase.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (storeMgr.getRuntimeManager() != null)
            {
                storeMgr.getRuntimeManager().incrementUpdateCount();
            }
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractPersistenceHandler#deleteObjects(org.datanucleus.store.ObjectProvider[])
     */
    @Override
    public void deleteObjects(ObjectProvider... ops)
    {
        if (ops.length == 1)
        {
            deleteObject(ops[0]);
            return;
        }

        // Separate the objects to be persisted into groups, for the "table" in question
        Map<String, Set<ObjectProvider>> opsByTable = new HashMap();
        for (int i=0;i<ops.length;i++)
        {
            AbstractClassMetaData cmd = ops[i].getClassMetaData();
            String tableName = HBaseUtils.getTableName(cmd);
            Set<ObjectProvider> opsForTable = opsByTable.get(tableName);
            if (opsForTable == null)
            {
                opsForTable = new HashSet<ObjectProvider>();
                opsByTable.put(tableName, opsForTable);
            }
            opsForTable.add(ops[i]);
        }

        Set<NucleusOptimisticException> optimisticExcps = null;
        for (String tableName : opsByTable.keySet())
        {
            Set<ObjectProvider> opsForTable = opsByTable.get(tableName);
            ExecutionContext ec = ops[0].getExecutionContext();
            HBaseManagedConnection mconn = (HBaseManagedConnection)storeMgr.getConnection(ec);
            try
            {
                long startTime = System.currentTimeMillis();
                HTable table = mconn.getHTable(HBaseUtils.getTableName(opsForTable.iterator().next().getClassMetaData()));

                List<Delete> deletes = new ArrayList(opsForTable.size());
                for (ObjectProvider op : opsForTable)
                {
                    // Check if read-only so update not permitted
                    storeMgr.assertReadOnlyForUpdateOfObject(op);

                    if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("HBase.Delete.Start", 
                            op.toPrintableID(), op.getInternalObjectId()));
                    }
                    boolean deletable = true;
                    AbstractClassMetaData cmd = op.getClassMetaData();
                    if (cmd.isVersioned())
                    {
                        // Optimistic checking of version
                        Object currentVersion = op.getTransactionalVersion();
                        Result result = HBaseUtils.getResultForObject(op, table);
                        Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, ec);
                        if (!datastoreVersion.equals(currentVersion)) // TODO Use VersionHelper.performVersionCheck
                        {
                            if (optimisticExcps == null)
                            {
                                optimisticExcps = new HashSet<NucleusOptimisticException>();
                            }
                            optimisticExcps.add(new NucleusOptimisticException("Cannot delete object with id " + 
                                op.getObjectId() + " since has version=" + currentVersion + 
                                " while datastore has version=" + datastoreVersion));
                            deletable = false;
                        }
                    }

                    if (deletable)
                    {
                        // Invoke any cascade deletion
                        op.loadUnloadedFields();
                        op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op));

                        // Delete the object
                        deletes.add(HBaseUtils.getDeleteForObject(op));
                    }
                }

                // Delete all rows from this table
                table.delete(deletes);

                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("HBase.ExecutionTime", 
                        (System.currentTimeMillis() - startTime)));
                }
                if (storeMgr.getRuntimeManager() != null)
                {
                    for (int i=0;i<opsForTable.size();i++)
                    {
                        storeMgr.getRuntimeManager().incrementDeleteCount();
                    }
                }
            }
            catch (IOException ioe)
            {
                throw new NucleusDataStoreException(ioe.getMessage(), ioe);
            }
            finally
            {
                mconn.release();
            }
        }

        if (optimisticExcps != null)
        {
            if (optimisticExcps.size() == 1)
            {
                throw optimisticExcps.iterator().next();
            }
            throw new NucleusOptimisticException("Optimistic exceptions thrown during delete of objects",
                optimisticExcps.toArray(new NucleusOptimisticException[optimisticExcps.size()]));
        }
    }

    public void deleteObject(ObjectProvider op)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(op);
        
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(op.getExecutionContext());
        try
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("HBase.Delete.Start", 
                    op.toPrintableID(), op.getInternalObjectId()));
            }

            HTable table = mconn.getHTable(HBaseUtils.getTableName(cmd));
            if (cmd.isVersioned())
            {
                // Optimistic checking of version
                Result result = HBaseUtils.getResultForObject(op, table);
                Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result, op.getExecutionContext());
                VersionHelper.performVersionCheck(op, datastoreVersion, cmd.getVersionMetaDataForClass());
            }

            // Invoke any cascade deletion
            op.loadUnloadedFields();
            op.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(op));

            // Delete the object
            table.delete(HBaseUtils.getDeleteForObject(op));

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("HBase.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
            if (storeMgr.getRuntimeManager() != null)
            {
                storeMgr.getRuntimeManager().incrementDeleteCount();
            }
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public void fetchObject(ObjectProvider op, int[] fieldNumbers)
    {
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(op.getExecutionContext());
        try
        {
            AbstractClassMetaData cmd = op.getClassMetaData();
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuffer str = new StringBuffer("Fetching object \"");
                str.append(op.toPrintableID()).append("\" (id=");
                str.append(op.getExecutionContext().getApiAdapter().getObjectId(op)).append(")").append(" fields [");
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        str.append(",");
                    }
                    str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                str.append("]");
                NucleusLogger.PERSISTENCE.debug(str);
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER.msg("HBase.Fetch.Start", 
                    op.toPrintableID(), op.getInternalObjectId()));
            }

            HTable table = mconn.getHTable(HBaseUtils.getTableName(cmd));
            Result result = HBaseUtils.getResultForObject(op, table);
            if (result.getRow() == null)
            {
                throw new NucleusObjectNotFoundException();
            }
            else if (cmd.hasDiscriminatorStrategy())
            {
                Object discValue = HBaseUtils.getDiscriminatorForObject(cmd, result);
                if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.CLASS_NAME)
                {
                    if (!cmd.getFullClassName().equals(discValue))
                    {
                        throw new NucleusObjectNotFoundException();
                    }
                }
                else if (cmd.getDiscriminatorStrategy() == DiscriminatorStrategy.VALUE_MAP)
                {
                    if (!cmd.getDiscriminatorValue().equals(discValue))
                    {
                        throw new NucleusObjectNotFoundException();
                    }
                }
            }

            FetchFieldManager fm = new FetchFieldManager(op, result);
            op.replaceFields(cmd.getAllMemberPositions(), fm);

            if (cmd.isVersioned() && op.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Version stored in a field
                    Object datastoreVersion =
                        op.provideField(cmd.getAbsolutePositionOfMember(vermd.getFieldName()));
                    op.setVersion(datastoreVersion);
                }
                else
                {
                    // Surrogate version
                    op.setVersion(HBaseUtils.getSurrogateVersionForObject(cmd, result));
                }
            }

            table.close();

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(LOCALISER.msg("HBase.ExecutionTime",
                    (System.currentTimeMillis() - startTime)));
            }
            if (storeMgr.getRuntimeManager() != null)
            {
                storeMgr.getRuntimeManager().incrementFetchCount();
            }
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
        finally
        {
            mconn.release();
        }
    }

    public Object findObject(ExecutionContext ectx, Object id)
    {
        return null;
    }

    public void locateObject(ObjectProvider op)
    {
        final AbstractClassMetaData cmd = op.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION || 
            cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(op.getExecutionContext());
            try
            {
                AbstractClassMetaData acmd = op.getClassMetaData();
                HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
                if (!HBaseUtils.objectExistsInTable(op, table))
                {
                    throw new NucleusObjectNotFoundException();
                }
                table.close();
            }
            catch (IOException e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
            finally
            {
                mconn.release();
            }
        }
    }
}