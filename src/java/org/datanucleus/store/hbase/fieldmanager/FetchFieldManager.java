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
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
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
import org.datanucleus.store.types.ObjectStringConverter;
import org.datanucleus.store.types.sco.SCOUtils;

import com.google.common.base.Charsets;

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
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchBooleanInternal(mmd, bytes);
    }

    public byte fetchByteField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchByteInternal(mmd, bytes);
    }

    public char fetchCharField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchCharInternal(mmd, bytes);
    }

    public double fetchDoubleField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchDoubleInternal(mmd, bytes);
    }

    public float fetchFloatField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchFloatInternal(mmd, bytes);
    }

    public int fetchIntField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchIntInternal(mmd, bytes);
    }

    public long fetchLongField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchLongInternal(mmd, bytes);
    }

    public short fetchShortField(int fieldNumber)
    {
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        return fetchShortInternal(mmd, bytes);
    }

    public String fetchStringField(int fieldNumber)
    {
        String value;
        String familyName = getFamilyName(fieldNumber);
        String columnName = getQualifierName(fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        AbstractMemberMetaData mmd = getMemberMetaData(fieldNumber);
        if (bytes == null)
        {
            // Handle missing field
            return HBaseUtils.getDefaultValueForMember(mmd);
        }

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
            value = new String(bytes, Charsets.UTF_8);
        }
        return value;
    }

    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);

        if (mmd.getType() == UUID.class) {
            String familyName = HBaseUtils.getFamilyName(cmd, fieldNumber);
            String columnName = HBaseUtils.getQualifierName(cmd, fieldNumber);
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            return fetchUUIDInternal(mmd, bytes);
        }

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
        Object value = readObjectField(familyName, columnName, result, fieldNumber, mmd);
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

                Map map;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), false);
                    map = (Map) instanceType.newInstance();
                }
                catch (Exception e)
                {
                    throw new NucleusDataStoreException(e.getMessage(), e);
                }

                Map mapIds = (Map)value;
                Iterator<Map.Entry> mapIdIter = mapIds.entrySet().iterator();
                while (mapIdIter.hasNext())
                {
                    Map.Entry entry = mapIdIter.next();
                    Object mapKey = entry.getKey();
                    Object mapValue = entry.getValue();
                    if (mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager()) != null)
                    {
                        // Map key must be an "id"
                        mapKey = ec.findObject(mapKey, true, true, null);
                    }
                    if (mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager()) != null)
                    {
                        // Map value must be an "id"
                        mapValue = ec.findObject(mapValue, true, true, null);
                    }
                    map.put(mapKey, mapValue);
                }
                if (sm != null)
                {
                    return sm.wrapSCOField(fieldNumber, map, false, false, true);
                }
                return map;
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
            Object returnValue = value;
            if (!mmd.isSerialized())
            {
                if (Enum.class.isAssignableFrom(mmd.getType()))
                {
                    // Persisted as a String, so convert back
                    // TODO Retrieve as number when requested
                    return Enum.valueOf(mmd.getType(), (String)value);
                }

                ObjectStringConverter strConv =
                    ec.getNucleusContext().getTypeManager().getStringConverter(mmd.getType());
                if (strConv != null)
                {
                    // Persisted as a String, so convert back
                    String strValue = (String)value;
                    returnValue = strConv.toObject(strValue);
                }
            }
            if (sm != null)
            {
                return sm.wrapSCOField(fieldNumber, returnValue, false, false, true);
            }
            return returnValue;
        }
    }

    protected Object readObjectField(String familyName, String columnName, Result result, int fieldNumber,
            AbstractMemberMetaData mmd)
    {
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        if (bytes == null)
        {
            return null;
        }

        if (mmd.getType() == Boolean.class)
        {
            return fetchBooleanInternal(mmd, bytes);
        }
        else if (mmd.getType() == Byte.class)
        {
            return fetchByteInternal(mmd, bytes);
        }
        else if (mmd.getType() == Character.class)
        {
            return fetchCharInternal(mmd, bytes);
        }
        else if (mmd.getType() == Double.class)
        {
            return fetchDoubleInternal(mmd, bytes);
        }
        else if (mmd.getType() == Float.class)
        {
            return fetchFloatInternal(mmd, bytes);
        }
        else if (mmd.getType() == Integer.class)
        {
            return fetchIntInternal(mmd, bytes);
        }
        else if (mmd.getType() == Long.class)
        {
            return fetchLongInternal(mmd, bytes);
        }
        else if (mmd.getType() == Short.class)
        {
            return fetchShortInternal(mmd, bytes);
        }
        else if (mmd.getType() == UUID.class)
        {
            return fetchUUIDInternal(mmd, bytes);
        }

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = null;
        try
        {
            ois = new ObjectInputStream(bis);
            return ois.readObject();
        }
        catch (IOException e)
        {
            // Failure in deserialisation, so must be persisted as String
            // Return as a String TODO Allow persist using ObjectLongConverter as non-serialised
            return new String(bytes, Charsets.UTF_8);
        }
        catch (ClassNotFoundException e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        finally
        {
            IOUtils.closeStream(ois);
            IOUtils.closeStream(bis);
        }
    }

    private boolean fetchBooleanInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        boolean value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Boolean.valueOf(dflt).booleanValue();
            }
            return false;
        }

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

    private byte fetchByteInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return dflt.getBytes()[0];
            }
            return 0;
        }

        return bytes[0];
    }

    private char fetchCharInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        char value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null && dflt.length() > 0)
            {
                return dflt.charAt(0);
            }
            return 0;
        }

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
            String strValue = new String(bytes, Charsets.UTF_8);
            value = strValue.charAt(0);
        }
        return value;
    }

    private double fetchDoubleInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        double value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Double.valueOf(dflt).doubleValue();
            }
            return 0;
        }

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

    private float fetchFloatInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        float value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Float.valueOf(dflt).floatValue();
            }
            return 0;
        }

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

    private int fetchIntInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        int value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Integer.valueOf(dflt).intValue();
            }
            return 0;
        }

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

    private long fetchLongInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        long value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Long.valueOf(dflt).longValue();
            }
            return 0;
        }

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

    private short fetchShortInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        short value;
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            if (dflt != null)
            {
                return Short.valueOf(dflt).shortValue();
            }
            return 0;
        }

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

    private UUID fetchUUIDInternal(AbstractMemberMetaData mmd, byte[] bytes)
    {
        if (bytes == null)
        {
            // Handle missing field
            String dflt = HBaseUtils.getDefaultValueForMember(mmd);
            return dflt != null ? UUID.fromString(dflt) : null;
        }

        if (mmd.isSerialized())
        {
            ByteArrayInputStream bis = null;
            ObjectInputStream ois = null;
            try
            {
                bis = new ByteArrayInputStream(bytes);
                ois = new ObjectInputStream(bis);
                return UUID.class.cast(ois.readObject());
            }
            catch (ClassNotFoundException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
            catch (IOException e)
            {
                throw new NucleusException(e.getMessage(), e);
            }
            finally
            {
                IOUtils.closeStream(ois);
                IOUtils.closeStream(bis);
            }
        }
        else
        {
            if (bytes.length ==16) {
                // serialized as bytes
                long upper = Bytes.toLong(bytes);
                long lower = Bytes.toLong(ArrayUtils.subarray(bytes, 8, 16));
                return new UUID(upper, lower);
            }
            else {
                final String value = new String(bytes, Charsets.UTF_8);
                return UUID.fromString(value);
            }
        }
    }
}
