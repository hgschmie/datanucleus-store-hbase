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
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.types.ObjectLongConverter;
import org.datanucleus.store.types.ObjectStringConverter;
import org.datanucleus.store.types.sco.SCOUtils;

public class FetchFieldManager extends AbstractFieldManager
{
    Result result;
    ExecutionContext ec;
    ObjectProvider sm;
    AbstractClassMetaData cmd;

    public FetchFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, Result result)
    {
        this.ec = ec;
        this.cmd = cmd;
        this.result = result;
    }

    public FetchFieldManager(ObjectProvider sm, Result result)
    {
        this.ec = sm.getExecutionContext();
        this.sm = sm;
        this.cmd = sm.getClassMetaData();
        this.result = result;
    }

    protected String getFamilyName(int fieldNumber)
    {
        return HBaseUtils.getFamilyName(cmd, fieldNumber);
    }

    protected String getQualifierName(int fieldNumber)
    {
        return HBaseUtils.getQualifierName(cmd, fieldNumber);
    }

    protected AbstractMemberMetaData getMemberMetaData(int fieldNumber)
    {
        return cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
    }

    public boolean fetchBooleanField(int fieldNumber)
    {
        boolean value;
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());

        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (mmd.isSerialized())
        {
            try
            {
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
        }
        else
        {
            value = Bytes.toBoolean(bytes);
        }
        return value;
    }

    public byte fetchByteField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        return bytes[0];
    }

    public char fetchCharField(int fieldNumber)
    {
        char value;
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());

        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (mmd.isSerialized())
        {
            try
            {
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
        }
        else
        {
            String strValue = new String(bytes);
            value = strValue.charAt(0);
        }
        return value;
    }

    public double fetchDoubleField(int fieldNumber)
    {
        double value;
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());

        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (mmd.isSerialized())
        {
            try
            {
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
        }
        else
        {
            value = Bytes.toDouble(bytes);
        }
        return value;
    }

    public float fetchFloatField(int fieldNumber)
    {
        float value;
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());

        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (mmd.isSerialized())
        {
            try
            {
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
        }
        else
        {
            value = Bytes.toFloat(bytes);
        }
        return value;
    }

    public int fetchIntField(int fieldNumber)
    {
        int value;
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());

        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (mmd.isSerialized())
        {
            try
            {
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
        }
        else
        {
            value = Bytes.toInt(bytes);
        }
        return value;
    }

    public long fetchLongField(int fieldNumber)
    {
        long value;
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());

        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (mmd.isSerialized())
        {
            try
            {
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
        }
        else
        {
            value = Bytes.toLong(bytes);
        }
        return value;
    }

    public short fetchShortField(int fieldNumber)
    {
        short value;
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());

        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (mmd.isSerialized())
        {
            try
            {
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
        }
        else
        {
            value = Bytes.toShort(bytes);
        }
        return value;
    }

    public String fetchStringField(int fieldNumber)
    {
        String value;
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        if (bytes == null)
        {
            return null;
        }

        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (mmd.isSerialized())
        {
            try
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                value = (String) ois.readObject();
                ois.close();
                bis.close();
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
            catch (ClassNotFoundException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
        }
        else
        {
            value = new String(bytes);
        }
        return value;
    }

    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        int relationType = mmd.getRelationType(clr);
        if (mmd.isEmbedded() && Relation.isRelationSingleValued(relationType))
        {
            // Persistable object embedded into table of this object
            Class embcls = mmd.getType();
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
            if (embcmd != null)
            {
                String tableName = HBaseUtils.getTableName(cmd);

                // Check for null value (currently need all columns to return null)
                // TODO Cater for null (use embmd.getNullIndicatorColumn/Value)
                EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
                AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
                boolean isNull = true;
                for (int i=0;i<embmmds.length;i++)
                {
                    String familyName = HBaseUtils.getFamilyName(mmd, i, tableName);
                    String columnName = HBaseUtils.getQualifierName(mmd, i);
                    if (result.getValue(familyName.getBytes(), columnName.getBytes()) != null)
                    {
                        isNull = false;
                        break;
                    }
                }
                if (isNull)
                {
                    return null;
                }

                ObjectProvider embSM = ec.newObjectProviderForEmbedded(mmd, embcmd, sm, fieldNumber);
                FieldManager ffm = new FetchEmbeddedFieldManager(embSM, result, mmd, tableName);
                embSM.replaceFields(embcmd.getAllMemberPositions(), ffm);
                return embSM.getObject();
            }
            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " marked as embedded but no such metadata");
        }

        String familyName = HBaseUtils.getFamilyName(cmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(cmd, fieldNumber);
        Object value = readObjectField(familyName, columnName, result);
        if (value == null)
        {
            return null;
        }

        if (Relation.isRelationSingleValued(relationType))
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
        else if (Relation.isRelationMultiValued(relationType))
        {
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
                if (sm != null)
                {
                    return sm.wrapSCOField(fieldNumber, coll, false, false, true);
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
            throw new NucleusUserException("No container that isnt collection/map/array");
        }
        else
        {
            ObjectStringConverter strConv = 
                ec.getNucleusContext().getTypeManager().getStringConverter(value.getClass());
            ObjectLongConverter longConv = 
                ec.getNucleusContext().getTypeManager().getLongConverter(value.getClass());
            Object returnValue = value;
            if (!mmd.isSerialized())
            {
                if (strConv != null)
                {
                    // Persisted as a String, so convert back
                    String strValue = (String)value;
                    returnValue = strConv.toObject(strValue);
                }
                else if (longConv != null)
                {
                    // Persisted as a Long, so convert back
                    Long longValue = (Long)value;
                    returnValue = longConv.toObject(longValue);
                }
            }
            if (sm != null)
            {
                return sm.wrapSCOField(fieldNumber, returnValue, false, false, true);
            }
            return returnValue;
        }
    }

    protected Object readObjectField(String familyName, String columnName, Result result)
    {
        Object value = null;
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