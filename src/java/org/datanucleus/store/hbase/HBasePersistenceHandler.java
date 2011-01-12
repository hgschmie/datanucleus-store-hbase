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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.hbase.fieldmanager.FetchFieldManager;
import org.datanucleus.store.hbase.fieldmanager.InsertFieldManager;
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

        try
        {
            // Check existence of the object since HBase doesn't enforce application identity
            locateObject(sm);
            throw new NucleusUserException(LOCALISER.msg("HBase.Insert.ObjectWithIdAlreadyExists", 
                sm.toPrintableID(), sm.getInternalObjectId()));
        }
        catch (NucleusObjectNotFoundException onfe)
        {
            // Do nothing since object with this id doesn't exist
        }

        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = sm.getClassMetaData();
            HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            Put put = newPut(sm);
            Delete delete = newDelete(sm);
            if (acmd.hasVersionStrategy())
            {
                String familyName = HBaseUtils.getFamilyName(acmd.getVersionMetaData());
                String columnName = HBaseUtils.getQualifierName(acmd.getVersionMetaData());
                if (acmd.getVersionMetaData().getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    long versionNumber = 1;
                    sm.setTransactionalVersion(Long.valueOf(versionNumber));
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER.msg("HBase.Insert.ObjectPersistedWithVersion",
                            sm.toPrintableID(), sm.getInternalObjectId(), "" + versionNumber));
                    }
                    if (acmd.getVersionMetaData().getFieldName() != null)
                    {
                        AbstractMemberMetaData verMmd = acmd.getMetaDataForMember(acmd.getVersionMetaData().getFieldName());
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
                            NucleusLogger.GENERAL.info(">> insertObject setting VERSION");
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
                else if (acmd.getVersionMetaData().getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    Date date = new Date();
                    Timestamp ts = new Timestamp(date.getTime());
                    sm.setTransactionalVersion(ts);
                    if (NucleusLogger.DATASTORE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE.debug(LOCALISER.msg("HBase.Insert.ObjectPersistedWithVersion",
                            sm.toPrintableID(), sm.getInternalObjectId(), "" + ts));
                    }
                    if (acmd.getVersionMetaData().getFieldName() != null)
                    {
                        AbstractMemberMetaData verMmd = acmd.getMetaDataForMember(acmd.getVersionMetaData().getFieldName());
                        sm.replaceField(verMmd.getAbsoluteFieldNumber(), ts);
                    }
                    else
                    {
                        try
                        {
                            NucleusLogger.GENERAL.info(">> insertObject setting VERSION");
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

            InsertFieldManager fm = new InsertFieldManager(acmd, put, delete);
            sm.provideFields(acmd.getAllMemberPositions(), fm);

            table.put(put);
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

    public void updateObject(ObjectProvider sm, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);
        
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = sm.getClassMetaData();
            HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            Put put = newPut(sm);
            Delete delete = newDelete(sm); // we will ignore the delete object
            InsertFieldManager fm = new InsertFieldManager(acmd, put, delete);
            sm.provideFields(fieldNumbers, fm);

            if (acmd.hasVersionStrategy())
            {
                // Version object so calculate version to store with
                Object currentVersion = sm.getTransactionalVersion();
                Object nextVersion = null;
                VersionMetaData vermd = acmd.getVersionMetaData();
                if (acmd.getVersionMetaData().getFieldName() != null)
                {
                    // Version field
                    AbstractMemberMetaData verfmd = acmd.getMetaDataForMember(vermd.getFieldName());
                    if (currentVersion instanceof Integer)
                    {
                        // Cater for Integer-based versions TODO Generalise this
                        currentVersion = Long.valueOf(((Integer)currentVersion).longValue());
                    }

                    nextVersion = acmd.getVersionMetaData().getNextVersion(currentVersion);
                    if (verfmd.getType() == Integer.class || verfmd.getType() == int.class)
                    {
                        // Cater for Integer-based versions TODO Generalise this
                        nextVersion = Integer.valueOf(((Long)nextVersion).intValue());
                    }
                }
                else
                {
                    // Surrogate version column
                    nextVersion = vermd.getNextVersion(currentVersion);
                }

                String familyName = HBaseUtils.getFamilyName(acmd.getVersionMetaData());
                String columnName = HBaseUtils.getQualifierName(acmd.getVersionMetaData());
                if (acmd.getVersionMetaData().getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                {
                    try
                    {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeLong(((Long)nextVersion).longValue());
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
                else if (acmd.getVersionMetaData().getVersionStrategy() == VersionStrategy.DATE_TIME)
                {
                    try
                    {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(nextVersion);
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

            if (!put.isEmpty())
            {
                table.put(put);
            }
            if (!delete.isEmpty())
            {
                //only delete if there are columns to delete. Otherwise an empty delete would cause the
                //entire row to be deleted
                table.delete(delete);            
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

    public void deleteObject(ObjectProvider sm)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);
        
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = sm.getClassMetaData();
            HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            
            table.delete(newDelete(sm));
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
            AbstractClassMetaData acmd = sm.getClassMetaData();
            HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            Result result = getResult(sm, table);
            if (result.getRow()==null)
            {
                throw new NucleusObjectNotFoundException();
            }
            FetchFieldManager fm = new FetchFieldManager(acmd, result);
            sm.replaceFields(acmd.getAllMemberPositions(), fm);

            if (acmd.hasVersionStrategy() && sm.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it
                if (acmd.getVersionMetaData().getFieldName() != null)
                {
                    // Version stored in a field
                    Object datastoreVersion =
                        sm.provideField(acmd.getAbsolutePositionOfMember(acmd.getVersionMetaData().getFieldName()));
                    sm.setVersion(datastoreVersion);
                }
                else
                {
                    // Surrogate version
                    String familyName = HBaseUtils.getFamilyName(acmd.getVersionMetaData());
                    String columnName = HBaseUtils.getQualifierName(acmd.getVersionMetaData());
                    try
                    {
                        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
                        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                        ObjectInputStream ois = new ObjectInputStream(bis);
                        if (acmd.getVersionMetaData().getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                        {
                            sm.setVersion(Long.valueOf(ois.readLong()));
                        }
                        else
                        {
                            sm.setVersion(ois.readObject());
                        }
                        ois.close();
                        bis.close();
                    }
                    catch (Exception e)
                    {
                        throw new NucleusException(e.getMessage(), e);
                    }
                }
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

    public Object findObject(ExecutionContext ectx, Object id)
    {
        return null;
    }

    public void locateObject(ObjectProvider sm)
    {
        HBaseManagedConnection mconn = (HBaseManagedConnection) storeMgr.getConnection(sm.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = sm.getClassMetaData();
            HTable table = mconn.getHTable(HBaseUtils.getTableName(acmd));
            if (!exists(sm, table))
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

    private Put newPut(ObjectProvider sm) throws IOException
    {
        // TODO Support composite PKs
        // TODO Support datastore id
        Object pkValue = sm.provideField(sm.getClassMetaData().getPKMemberPositions()[0]);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        Put batch = new Put(bos.toByteArray());
        oos.close();
        bos.close();
        return batch;
    }

    private Delete newDelete(ObjectProvider sm) throws IOException
    {
        // TODO Support composite PKs
        // TODO Support datastore id
        Object pkValue = sm.provideField(sm.getClassMetaData().getPKMemberPositions()[0]);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        Delete batch = new Delete(bos.toByteArray());
        oos.close();
        bos.close();
        return batch;
    }

    private Result getResult(ObjectProvider sm, HTable table) throws IOException
    {
        // TODO Support composite PKs
        // TODO Support datastore id
        Object pkValue = sm.provideField(sm.getClassMetaData().getPKMemberPositions()[0]);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        Get get = new Get(bos.toByteArray());
        Result result = table.get(get);
        oos.close();
        bos.close();
        return result;
    }  

    private boolean exists(ObjectProvider sm, HTable table) throws IOException
    {
        // TODO Support composite PKs
        // TODO Support datastore id
        Object pkValue = sm.provideField(sm.getClassMetaData().getPKMemberPositions()[0]);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        Get get = new Get(bos.toByteArray());
        boolean result = table.exists(get);
        oos.close();
        bos.close();
        return result;
    }
}