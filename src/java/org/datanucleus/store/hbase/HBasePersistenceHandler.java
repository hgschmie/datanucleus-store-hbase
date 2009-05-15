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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;
import org.datanucleus.ManagedConnection;
import org.datanucleus.ObjectManager;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.StorePersistenceHandler;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.StringUtils;

public class HBasePersistenceHandler implements StorePersistenceHandler
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.hbase.Localisation", HBaseStoreManager.class.getClassLoader());

    protected final HBaseStoreManager storeMgr;

    HBasePersistenceHandler(StoreManager storeMgr)
    {
        this.storeMgr = (HBaseStoreManager) storeMgr;
    }
    
    public void close()
    {
        // TODO Auto-generated method stub
        
    }

    public void deleteObject(StateManager sm)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);
        
        ManagedConnection mconn = storeMgr.getConnection(sm.getObjectManager());
        try
        {
            HBaseConfiguration config = (HBaseConfiguration)mconn.getConnection();
            AbstractClassMetaData acmd = sm.getClassMetaData();
            createSchema(config, acmd);
            HTable table = new HTable(config, HBaseUtils.getTableName(acmd));
            table.deleteAll(getRowBytes(sm));
            table.close();
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(),e);
        }
        finally
        {
            mconn.release();
        }    
    }

    public void fetchObject(StateManager sm, int[] fieldNumbers)
    {
        ManagedConnection mconn = storeMgr.getConnection(sm.getObjectManager());
        try
        {
            HBaseConfiguration config = (HBaseConfiguration)mconn.getConnection();
            AbstractClassMetaData acmd = sm.getClassMetaData();
            createSchema(config, acmd);
            HTable table = new HTable(config, HBaseUtils.getTableName(acmd));
            RowResult result = getRowResult(sm,table);
            if(result==null)
            {
                throw new NucleusObjectNotFoundException();
            }
            HBaseFetchFieldManager fm = new HBaseFetchFieldManager(sm, result);
            sm.replaceFields(acmd.getAllMemberPositions(), fm);
            table.close();
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(),e);
        }
        finally
        {
            mconn.release();
        }    
    }

    public Object findObject(ObjectManager om, Object id)
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void insertObject(StateManager sm)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);

        // Check existence of the object since HBase doesn't enforce application identity
        try
        {
            locateObject(sm);
            throw new NucleusUserException(LOCALISER.msg("HBase.Insert.ObjectWithIdAlreadyExists",
                StringUtils.toJVMIDString(sm.getObject()), sm.getInternalObjectId()));
        }
        catch (NucleusObjectNotFoundException onfe)
        {
            // Do nothing since object with this id doesn't exist
        }
        
        storeMgr.addClass(sm.getClassMetaData().getFullClassName(),sm.getObjectManager().getClassLoaderResolver());
        ManagedConnection mconn = storeMgr.getConnection(sm.getObjectManager());
        try
        {
            HBaseConfiguration config = (HBaseConfiguration)mconn.getConnection();
            AbstractClassMetaData acmd = sm.getClassMetaData();
            createSchema(config, acmd);
            HTable table = new HTable(config, HBaseUtils.getTableName(acmd));
            BatchUpdate batch = newBatchUpdate(sm);
            HBaseInsertFieldManager fm = new HBaseInsertFieldManager(sm, batch);
            sm.provideFields(acmd.getAllMemberPositions(), fm);
            table.commit(batch);
            table.close();
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(),e);
        }
        finally
        {
            mconn.release();
        }
    }

    private BatchUpdate newBatchUpdate(StateManager sm) throws IOException
    {
        Object pkValue = sm.provideField(sm.getClassMetaData().getPKMemberPositions()[0]);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        BatchUpdate batch = new BatchUpdate(bos.toByteArray());
        oos.close();
        bos.close();
        return batch;
    }
    
    private RowResult getRowResult(StateManager sm, HTable table) throws IOException
    {
        Object pkValue = sm.provideField(sm.getClassMetaData().getPKMemberPositions()[0]);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        RowResult result = table.getRow(bos.toByteArray());
        oos.close();
        bos.close();
        return result;
    }  
    
    private byte[] getRowBytes(StateManager sm) throws IOException
    {
        Object pkValue = sm.provideField(sm.getClassMetaData().getPKMemberPositions()[0]);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkValue);
        byte[] bytes = bos.toByteArray();
        oos.close();
        bos.close();
        return bytes;
    }     
    
    private void createSchema(HBaseConfiguration config, AbstractClassMetaData acmd) throws IOException
    {
        HBaseAdmin hBaseAdmin = new HBaseAdmin(config);
        HTableDescriptor hTable;
        String tableName = HBaseUtils.getTableName(acmd);
        boolean isNew = false;
        try
        {
            hTable = hBaseAdmin.getTableDescriptor(tableName);
        }
        catch(TableNotFoundException ex)
        {
            isNew = true;
            hTable = new HTableDescriptor(tableName);
            hBaseAdmin.createTable(hTable);
        }

        if (isNew)
        {
            // TODO Do something or ditch this variable
        }
        HColumnDescriptor hColumn;
        hColumn = hTable.getFamily(HBaseUtils.getTableName(acmd).getBytes());
        if( hColumn==null)
        {
            hColumn = new HColumnDescriptor(HBaseUtils.getTableName(acmd)+":");
            hTable.addFamily(hColumn);
            hBaseAdmin.disableTable(hTable.getName());
            hBaseAdmin.modifyTable(hTable.getName(), hTable);
            hBaseAdmin.enableTable(hTable.getName());
        }
    }
    
    public void locateObject(StateManager sm)
    {
        fetchObject(sm, sm.getClassMetaData().getAllMemberPositions());
    }

    public void updateObject(StateManager sm, int[] fieldNumbers)
    {
        // Check if read-only so update not permitted
        storeMgr.assertReadOnlyForUpdateOfObject(sm);
        
        ManagedConnection mconn = storeMgr.getConnection(sm.getObjectManager());
        try
        {
            HBaseConfiguration config = (HBaseConfiguration)mconn.getConnection();
            AbstractClassMetaData acmd = sm.getClassMetaData();
            createSchema(config, acmd);
            HTable table = new HTable(config, HBaseUtils.getTableName(acmd));
            BatchUpdate batch = newBatchUpdate(sm);
            HBaseInsertFieldManager fm = new HBaseInsertFieldManager(sm, batch);
            sm.provideFields(fieldNumbers, fm);
            table.commit(batch);
            table.close();
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(),e);
        }
        finally
        {
            mconn.release();
        }
    }

}
