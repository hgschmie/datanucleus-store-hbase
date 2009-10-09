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
import java.io.IOException;
import java.io.ObjectInputStream;

import org.apache.hadoop.hbase.client.Result;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;

public class HBaseFetchFieldManager extends AbstractFieldManager
{
    Result result;
    StateManager sm;

    public HBaseFetchFieldManager(StateManager sm, Result result)
    {
        this.sm = sm;
        this.result = result;
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(sm.getClassMetaData(),fieldNumber);
        String columnName = HBaseUtils.getQualifierName(sm.getClassMetaData(),fieldNumber);
        boolean value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readBoolean();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }
    
    public byte fetchByteField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(sm.getClassMetaData(),fieldNumber);
        String columnName = HBaseUtils.getQualifierName(sm.getClassMetaData(),fieldNumber);
        byte value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readByte();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }
    
    public char fetchCharField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(sm.getClassMetaData(),fieldNumber);
        String columnName = HBaseUtils.getQualifierName(sm.getClassMetaData(),fieldNumber);
        char value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readChar();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;    
    }
    
    public double fetchDoubleField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(sm.getClassMetaData(),fieldNumber);
        String columnName = HBaseUtils.getQualifierName(sm.getClassMetaData(),fieldNumber);
        double value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readDouble();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value; 
    }
    
    public float fetchFloatField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(sm.getClassMetaData(),fieldNumber);
        String columnName = HBaseUtils.getQualifierName(sm.getClassMetaData(),fieldNumber);
        float value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readFloat();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }
    
    public int fetchIntField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(sm.getClassMetaData(),fieldNumber);
        String columnName = HBaseUtils.getQualifierName(sm.getClassMetaData(),fieldNumber);
        int value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readInt();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }
    
    public long fetchLongField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(sm.getClassMetaData(),fieldNumber);
        String columnName = HBaseUtils.getQualifierName(sm.getClassMetaData(),fieldNumber);
        long value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readLong();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }
    
    public Object fetchObjectField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(sm.getClassMetaData(),fieldNumber);
        String columnName = HBaseUtils.getQualifierName(sm.getClassMetaData(),fieldNumber);
        Object value;
        try
        {
            try
            {
                byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readObject();
                ois.close();
                bis.close();
            }
            catch(NullPointerException ex)
            {
                return null;
            }
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        catch (ClassNotFoundException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }
    
    public short fetchShortField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(sm.getClassMetaData(),fieldNumber);
        String columnName = HBaseUtils.getQualifierName(sm.getClassMetaData(),fieldNumber);
        short value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bis);
            value = ois.readShort();
            ois.close();
            bis.close();
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }
    
    public String fetchStringField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(sm.getClassMetaData(),fieldNumber);
        String columnName = HBaseUtils.getQualifierName(sm.getClassMetaData(),fieldNumber);
        String value;
        try
        {
            try
            {
                byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = (String) ois.readObject();
                ois.close();
                bis.close();
            }
            catch(NullPointerException ex)
            {
                return null;
            }
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        catch (ClassNotFoundException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;
    }
}
