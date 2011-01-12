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
2011 Andy Jefferson - clean up of NPE code
2011 Andy Jefferson - rewritten to support relationships
    ...
***********************************************************************/
package org.datanucleus.store.hbase.fieldmanager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.Result;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.types.sco.SCOUtils;

public class FetchFieldManager extends AbstractFieldManager
{
    Result result;

    ExecutionContext ec;
    ObjectProvider sm;
    AbstractClassMetaData acmd;

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData acmd, Result result)
    {
        this.ec = ec;
        this.acmd = acmd;
        this.result = result;
    }

    public FetchFieldManager(ObjectProvider sm, Result result)
    {
        this.ec = sm.getExecutionContext();
        this.sm = sm;
        this.acmd = sm.getClassMetaData();
        this.result = result;
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
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
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        return bytes[0];
    }

    public char fetchCharField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        AbstractMemberMetaData mmd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        char value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            if (mmd.isSerialized())
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readChar();
                ois.close();
                bis.close();
            }
            else
            {
                String strValue = new String(bytes);
                value = strValue.charAt(0);
            }
        }
        catch (IOException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return value;    
    }

    public double fetchDoubleField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
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
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
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
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
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
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
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
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        Object value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            if (bytes != null)
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = ois.readObject();
                ois.close();
                bis.close();
            }
            else
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

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        int relationType = mmd.getRelationType(clr);
        if (relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI ||
                relationType == Relation.MANY_TO_ONE_BI || relationType == Relation.MANY_TO_ONE_UNI)
        {
            if (mmd.isSerialized())
            {
                return value;
            }
            else
            {
                // The stored value was the identity
                return ec.findObject(value, true, true, null);
            }
        }
        else if (relationType == Relation.ONE_TO_MANY_UNI || relationType == Relation.ONE_TO_MANY_BI ||
                relationType == Relation.MANY_TO_MANY_BI)
        {
            // TODO Replace with SCO
            if (mmd.hasCollection())
            {
                if (mmd.isSerialized())
                {
                    return value;
                }

                Collection<Object> coll;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                    coll = (Collection<Object>) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                Collection collIds = (Collection)value;
                Iterator idIter = collIds.iterator();
                while (idIter.hasNext())
                {
                    Object elementId = idIter.next();
                    coll.add(ec.findObject(elementId, true, true, null));
                }
                return coll;
            }
            else if (mmd.hasMap())
            {
                if (mmd.isSerialized())
                {
                    return value;
                }
            }
            else if (mmd.hasArray())
            {
                if (mmd.isSerialized())
                {
                    return value;
                }

                Collection arrIds = (Collection)value;
                Object array = Array.newInstance(mmd.getType().getComponentType(), arrIds.size());
                Iterator idIter = arrIds.iterator();
                int i=0;
                while (idIter.hasNext())
                {
                    Object elementId = idIter.next();
                    Array.set(array, i, ec.findObject(elementId, true, true, null));
                }
                return array;
            }
        }
        // TODO Replace with SCO
        return value;
    }

    public short fetchShortField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
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
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        AbstractMemberMetaData mmd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        String value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            if (bytes != null)
            {
                if (mmd.isSerialized())
                {
                    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                    ObjectInputStream ois = new ObjectInputStream(bis);
                    value = (String) ois.readObject();
                    ois.close();
                    bis.close();
                }
                else
                {
                    value = new String(bytes);
                }
            }
            else
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