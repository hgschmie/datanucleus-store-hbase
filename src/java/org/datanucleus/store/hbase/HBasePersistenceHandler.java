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
import java.util.Date;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
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

    public void insertObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);

        if (!storeMgr.managesClass(sm.getClassMetaData().getFullClassName()))
        {
            storeMgr.addClass(sm.getClassMetaData().getFullClassName(),sm.getExecutionContext().getClassLoaderResolver());
        }

        AbstractClassMetaData cmd = sm.getClassMetaData();
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
                    locateObject(sm);
                    throw new NucleusUserException(LOCALISER.msg("HBase.Insert.ObjectWithIdAlreadyExists", 
                        sm.toPrintableID(), sm.getInternalObjectId()));
                }
                catch (NucleusObjectNotFoundException onfe)
                {
                    // Do nothing since object with this id doesn't exist
                }
            }
        }

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("HBase.Insert.Start", 
                    sm.toPrintableID(), sm.getInternalObjectId()));
            }

            HTable table = mconn.getHTable(HBaseUtils.getTableName(cmd));
            Put put = HBaseUtils.getPutForObject(sm);
            Delete delete = HBaseUtils.getDeleteForObject(sm);

            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                String familyName = HBaseUtils.getFamilyName(cmd.getIdentityMetaData());
                String columnName = HBaseUtils.getQualifierName(cmd.getIdentityMetaData());
                Object key = ((OID)sm.getInternalObjectId()).getKeyValue();
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
                Object discVal = null;
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

                try
                {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(discVal);
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

            if (cmd.hasVersionStrategy())
            {
                String familyName = HBaseUtils.getFamilyName(cmd.getVersionMetaData());
                String columnName = HBaseUtils.getQualifierName(cmd.getVersionMetaData());
                if (cmd.getVersionMetaData().getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    long versionNumber = 1;
                    sm.setTransactionalVersion(Long.valueOf(versionNumber));
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER.msg("HBase.Insert.ObjectPersistedWithVersion",
                            sm.toPrintableID(), sm.getInternalObjectId(), "" + versionNumber));
                    }
                    if (cmd.getVersionMetaData().getFieldName() != null)
                    {
                        AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(cmd.getVersionMetaData().getFieldName());
                        Object verFieldValue = Long.valueOf(versionNumber);
                        if (verMmd.getType() == int.class || verMmd.getType() == Integer.class)
                        {
                            verFieldValue = Integer.valueOf((int)versionNumber);
                        }
                        sm.replaceField(verMmd.getAbsoluteFieldNumber(), verFieldValue);
                    }
                    else
                    {
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeLong(versionNumber);
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
                }
                else if (cmd.getVersionMetaData().getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    Date date = new Date();
                    Timestamp ts = new Timestamp(date.getTime());
                    sm.setTransactionalVersion(ts);
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER.msg("HBase.Insert.ObjectPersistedWithVersion",
                            sm.toPrintableID(), sm.getInternalObjectId(), "" + ts));
                    }
                    if (cmd.getVersionMetaData().getFieldName() != null)
                    {
                        AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(cmd.getVersionMetaData().getFieldName());
                        sm.replaceField(verMmd.getAbsoluteFieldNumber(), ts);
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

            StoreFieldManager fm = new StoreFieldManager(sm, put, delete);
            sm.provideFields(cmd.getAllMemberPositions(), fm);

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

    public void updateObject(ObjectProvider sm, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            long startTime = System.currentTimeMillis();
            AbstractClassMetaData cmd = sm.getClassMetaData();
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
                    sm.toPrintableID(), sm.getInternalObjectId(), fieldStr.toString()));
            }

            HTable table = mconn.getHTable(HBaseUtils.getTableName(cmd));
            if (cmd.hasVersionStrategy())
            {
                // Optimistic checking of version
                Object currentVersion = sm.getTransactionalVersion();
                Result result = HBaseUtils.getResultForObject(sm, table);
                Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result);
                if (!datastoreVersion.equals(currentVersion))
                {
                    throw new NucleusOptimisticException("Cannot update object with id " + sm.getObjectId() +
                        " since has version=" + currentVersion + " while datastore has version=" + datastoreVersion);
                }
            }

            Put put = HBaseUtils.getPutForObject(sm);
            Delete delete = HBaseUtils.getDeleteForObject(sm);
            if (cmd.hasVersionStrategy())
            {
                // Version object so calculate version to store with
                Object currentVersion = sm.getTransactionalVersion();
                VersionMetaData vermd = cmd.getVersionMetaData();
                Object nextVersion = vermd.getNextVersion(currentVersion);
                sm.setTransactionalVersion(nextVersion);

                if (cmd.getVersionMetaData().getFieldName() != null)
                {
                    // Update the field version value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(cmd.getVersionMetaData().getFieldName());
                    sm.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);
                }
                else
                {
                    // Update the stored surrogate value
                    String familyName = HBaseUtils.getFamilyName(vermd);
                    String columnName = HBaseUtils.getQualifierName(vermd);
                    try
                    {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        if (nextVersion instanceof Long)
                        {
                            oos.writeLong(((Long)nextVersion).longValue());
                        }
                        else if (nextVersion instanceof Integer)
                        {
                            oos.writeInt(((Integer)nextVersion).intValue());
                        }
                        else
                        {
                            oos.writeObject(nextVersion);
                        }
                        oos.flush();
                        put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                        oos.close();
                        bos.close();
                    }
                    catch (IOException ioe)
                    {
                        throw new NucleusException(ioe.getMessage(), ioe);
                    }
                }
            }

            StoreFieldManager fm = new StoreFieldManager(sm, put, delete);
            sm.provideFields(fieldNumbers, fm);
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

    public void deleteObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);
        
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData cmd = sm.getClassMetaData();
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(LOCALISER.msg("HBase.Delete.Start", 
                    sm.toPrintableID(), sm.getInternalObjectId()));
            }

            HTable table = mconn.getHTable(HBaseUtils.getTableName(cmd));
            if (cmd.hasVersionStrategy())
            {
                // Optimistic checking of version
                Object currentVersion = sm.getTransactionalVersion();
                Result result = HBaseUtils.getResultForObject(sm, table);
                Object datastoreVersion = HBaseUtils.getVersionForObject(cmd, result);
                if (!datastoreVersion.equals(currentVersion))
                {
                    throw new NucleusOptimisticException("Cannot delete object with id " + sm.getObjectId() +
                        " since has version=" + currentVersion + " while datastore has version=" + datastoreVersion);
                }
            }

            // Invoke any cascade deletion
            sm.loadUnloadedFields();
            sm.provideFields(cmd.getAllMemberPositions(), new DeleteFieldManager(sm));

            // Delete the object
            table.delete(HBaseUtils.getDeleteForObject(sm));

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

    public void fetchObject(ObjectProvider sm, int[] fieldNumbers)
    {
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData cmd = sm.getClassMetaData();
            if (NucleusLogger.PERSISTENCE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuffer str = new StringBuffer("Fetching object \"");
                str.append(sm.toPrintableID()).append("\" (id=");
                str.append(sm.getExecutionContext().getApiAdapter().getObjectId(sm)).append(")").append(" fields [");
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
                    sm.toPrintableID(), sm.getInternalObjectId()));
            }

            HTable table = mconn.getHTable(HBaseUtils.getTableName(cmd));
            Result result = HBaseUtils.getResultForObject(sm, table);
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

            FetchFieldManager fm = new FetchFieldManager(sm, result);
            sm.replaceFields(cmd.getAllMemberPositions(), fm);

            if (cmd.hasVersionStrategy() && sm.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it
                if (cmd.getVersionMetaData().getFieldName() != null)
                {
                    // Version stored in a field
                    Object datastoreVersion =
                        sm.provideField(cmd.getAbsolutePositionOfMember(cmd.getVersionMetaData().getFieldName()));
                    sm.setVersion(datastoreVersion);
                }
                else
                {
                    // Surrogate version
                    sm.setVersion(HBaseUtils.getSurrogateVersionForObject(cmd, result));
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

    public void locateObject(ObjectProvider sm)
    {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION || 
            cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
            try
            {
                AbstractClassMetaData acmd = sm.getClassMetaData();
                HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
                if (!HBaseUtils.objectExistsInTable(sm, table))
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