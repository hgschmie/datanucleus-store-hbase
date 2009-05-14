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

import org.apache.hadoop.hbase.io.BatchUpdate;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;

public class HBaseInsertFieldManager extends AbstractFieldManager
{
    BatchUpdate batch;
    StateManager sm;

    public HBaseInsertFieldManager(StateManager sm, BatchUpdate batch)
    {
        this.sm = sm;
        this.batch = batch;
    }
    
    public void storeBooleanField(int fieldNumber, boolean value)
    {
        String columnName = HBaseUtils.getColumnName(sm.getClassMetaData(),fieldNumber);

        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeBoolean(value);
            oos.flush();
            batch.put(columnName, bos.toByteArray());
            oos.close();
            bos.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }
    
    public void storeByteField(int fieldNumber, byte value)
    {
        String columnName = HBaseUtils.getColumnName(sm.getClassMetaData(),fieldNumber);
        batch.put(columnName, new byte[]{value});
    }

    public void storeCharField(int fieldNumber, char value)
    {
        String columnName = HBaseUtils.getColumnName(sm.getClassMetaData(),fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeChar(value);
            oos.flush();
            batch.put(columnName, bos.toByteArray());
            oos.close();
            bos.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }
    
    public void storeDoubleField(int fieldNumber, double value)
    {
        String columnName = HBaseUtils.getColumnName(sm.getClassMetaData(),fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeDouble(value);
            oos.flush();
            batch.put(columnName, bos.toByteArray());
            oos.close();
            bos.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }
    
    public void storeFloatField(int fieldNumber, float value)
    {
        String columnName = HBaseUtils.getColumnName(sm.getClassMetaData(),fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeFloat(value);
            oos.flush();
            batch.put(columnName, bos.toByteArray());
            oos.close();
            bos.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }
    
    public void storeIntField(int fieldNumber, int value)
    {
        String columnName = HBaseUtils.getColumnName(sm.getClassMetaData(),fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeInt(value);
            oos.flush();
            batch.put(columnName, bos.toByteArray());
            oos.close();
            bos.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }
    
    public void storeLongField(int fieldNumber, long value)
    {
        String columnName = HBaseUtils.getColumnName(sm.getClassMetaData(),fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeLong(value);
            oos.flush();
            batch.put(columnName, bos.toByteArray());
            oos.close();
            bos.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }
    
    public void storeObjectField(int fieldNumber, Object value)
    {
        String columnName = HBaseUtils.getColumnName(sm.getClassMetaData(),fieldNumber);
        if(value==null)
        {
            batch.delete(columnName);
        }
        else
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(value);
                batch.put(columnName, bos.toByteArray());
                oos.close();
                bos.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
    }
    
    public void storeShortField(int fieldNumber, short value)
    {
        String columnName = HBaseUtils.getColumnName(sm.getClassMetaData(),fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeShort(value);
            oos.flush();
            batch.put(columnName, bos.toByteArray());
            oos.close();
            bos.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
    }
    
    public void storeStringField(int fieldNumber, String value)
    {
        String columnName = HBaseUtils.getColumnName(sm.getClassMetaData(),fieldNumber);
        if(value==null)
        {
            batch.delete(columnName);
        }
        else
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(value);
                batch.put(columnName, bos.toByteArray());
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
