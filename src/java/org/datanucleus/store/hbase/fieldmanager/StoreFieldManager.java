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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.datanucleus.ClassLoaderResolver;
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

public class StoreFieldManager extends AbstractFieldManager
{
    ObjectProvider sm;
    Put put;
    Delete delete;
    AbstractClassMetaData acmd;

    public StoreFieldManager(ObjectProvider sm, Put put, Delete delete)
    {
        this.sm = sm;
        this.acmd = sm.getClassMetaData();
        this.put = put;
        this.delete = delete;
    }

    public void storeBooleanField(int fieldNumber, boolean value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeBoolean(value);
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

    public void storeByteField(int fieldNumber, byte value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        put.add(familyName.getBytes(), columnName.getBytes(), new byte[]{value});
    }

    public void storeCharField(int fieldNumber, char value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        AbstractMemberMetaData mmd = 
            acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd.isSerialized())
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeChar(value);
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
        else
        {
            put.add(familyName.getBytes(), columnName.getBytes(), ("" + value).getBytes());
        }
    }

    public void storeDoubleField(int fieldNumber, double value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeDouble(value);
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

    public void storeFloatField(int fieldNumber, float value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeFloat(value);
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

    public void storeIntField(int fieldNumber, int value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeInt(value);
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

    public void storeLongField(int fieldNumber, long value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeLong(value);
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

    public void storeObjectField(int fieldNumber, Object value)
    {
        ExecutionContext ec = sm.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = 
            acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        int relationType = mmd.getRelationType(clr);
        if ((relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI) && mmd.isEmbedded())
        {
            // Embedded PC object
            Class embcls = mmd.getType();
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
            if (embcmd != null) 
            {
                if (value == null)
                {
                    deleteColumnsForEmbeddedMember(mmd, clr, ec);
                    return;
                }

                ObjectProvider embSM = ec.findObjectProviderForEmbedded(value, sm, mmd);
                FieldManager ffm = new StoreEmbeddedFieldManager(embSM, put, delete, mmd, HBaseUtils.getTableName(acmd));
                embSM.provideFields(embcmd.getAllMemberPositions(), ffm);
                return;
            }
            else
            {
                throw new NucleusUserException("Field " + mmd.getFullFieldName() +
                    " specified as embedded but metadata not found for the class of type " + mmd.getTypeName());
            }
        }

        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        if (value == null)
        {
            // TODO What about delete-orphans?
            delete.deleteColumn(familyName.getBytes(), columnName.getBytes());
        }
        else
        {
            if (relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI ||
                relationType == Relation.MANY_TO_ONE_BI || relationType == Relation.MANY_TO_ONE_UNI)
            {
                // PC object, so make sure it is persisted
                Object valuePC = sm.getExecutionContext().persistObjectInternal(value, sm, fieldNumber, -1);
                if (mmd.isSerialized())
                {
                    // Persist as serialised into the column of this object
                    try
                    {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(value);
                        put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                        oos.close();
                        bos.close();
                    }
                    catch (IOException e)
                    {
                        throw new NucleusException(e.getMessage(), e);
                    }
                }
                else
                {
                    // Persist identity in the column of this object
                    Object valueId = ec.getApiAdapter().getIdForObject(valuePC);
                    try
                    {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(valueId);
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
            else if (relationType == Relation.ONE_TO_MANY_UNI || relationType == Relation.ONE_TO_MANY_BI ||
                relationType == Relation.MANY_TO_MANY_BI)
            {
                // Collection/Map/Array
                if (mmd.hasCollection())
                {
                    Collection collIds = new ArrayList();
                    Collection coll = (Collection)value;
                    Iterator collIter = coll.iterator();
                    while (collIter.hasNext())
                    {
                        Object element = collIter.next();
                        Object elementPC = sm.getExecutionContext().persistObjectInternal(element, sm, fieldNumber, -1);
                        Object elementID = sm.getExecutionContext().getApiAdapter().getIdForObject(elementPC);
                        collIds.add(elementID);
                    }

                    if (mmd.isSerialized())
                    {
                        // Persist as serialised into the column of this object
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(value);
                            put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                    else
                    {
                        // Persist comma-separated element key list into the column of this object
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(collIds);
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
                else if (mmd.hasMap())
                {
                    Map map = (Map)value;
                    Iterator<Map.Entry> mapIter = map.entrySet().iterator();
                    while (mapIter.hasNext())
                    {
                        Map.Entry entry = mapIter.next();
                        Object mapKey = entry.getKey();
                        Object mapValue = entry.getValue();
                        if (ec.getApiAdapter().isPersistable(mapKey))
                        {
                            ec.persistObjectInternal(mapKey, sm, fieldNumber, -1);
                        }
                        if (ec.getApiAdapter().isPersistable(mapValue))
                        {
                            ec.persistObjectInternal(mapValue, sm, fieldNumber, -1);
                        }
                    }
                    if (mmd.isSerialized())
                    {
                        // Persist as serialised into the column of this object
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(value);
                            put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                    else
                    {
                        // TODO Implement map persistence non-serialised
                        throw new NucleusException("Only currently support maps serialised with HBase. Mark the field as serialized");
                    }
                }
                else if (mmd.hasArray())
                {
                    Collection arrIds = new ArrayList();
                    for (int i=0;i<Array.getLength(value);i++)
                    {
                        Object element = Array.get(value, i);
                        Object elementPC = ec.persistObjectInternal(element, sm, fieldNumber, -1);
                        Object elementID = ec.getApiAdapter().getIdForObject(elementPC);
                        arrIds.add(elementID);
                    }

                    if (mmd.isSerialized())
                    {
                        // Persist as serialised into the column of this object
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(value);
                            put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                            oos.close();
                            bos.close();
                        }
                        catch (IOException e)
                        {
                            throw new NucleusException(e.getMessage(), e);
                        }
                    }
                    else
                    {
                        // Persist list of array element ids into the column of this object
                        try
                        {
                            ByteArrayOutputStream bos = new ByteArrayOutputStream();
                            ObjectOutputStream oos = new ObjectOutputStream(bos);
                            oos.writeObject(arrIds);
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
            else
            {
                ObjectStringConverter strConv = 
                    ec.getNucleusContext().getTypeManager().getStringConverter(value.getClass());
                if (!mmd.isSerialized() && strConv != null)
                {
                    // Persist as a String
                    String strValue = strConv.toString(value);
                    put.add(familyName.getBytes(), columnName.getBytes(), strValue.getBytes());
                }
                else
                {
                    try
                    {
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        oos.writeObject(value);
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
    }

    protected void deleteColumnsForEmbeddedMember(AbstractMemberMetaData mmd, ClassLoaderResolver clr,
            ExecutionContext ec)
    {
        Class embcls = mmd.getType();
        AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
        if (embcmd != null)
        {
            EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
            AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
            String tableName = HBaseUtils.getTableName(acmd);
            for (int i=0;i<embmmds.length;i++)
            {
                int relationType = embmmds[i].getRelationType(clr);
                if ((relationType == Relation.ONE_TO_ONE_BI || relationType == Relation.ONE_TO_ONE_UNI) && 
                    embmmds[i].isEmbedded())
                {
                    deleteColumnsForEmbeddedMember(embmmds[i], clr, ec);
                }
                else
                {
                    String familyName = HBaseUtils.getFamilyName(mmd, i, tableName);
                    String columnName = HBaseUtils.getQualifierName(mmd, i);
                    delete.deleteColumn(familyName.getBytes(), columnName.getBytes());
                }
            }
            return;
        }
    }

    public void storeShortField(int fieldNumber, short value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        try
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeShort(value);
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

    public void storeStringField(int fieldNumber, String value)
    {
        String familyName = HBaseUtils.getFamilyName(acmd, fieldNumber);
        String columnName = HBaseUtils.getQualifierName(acmd, fieldNumber);
        if (value == null)
        {
            delete.deleteColumn(familyName.getBytes(), columnName.getBytes());
        }
        else
        {
            AbstractMemberMetaData mmd = 
                acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
            if (mmd.isSerialized())
            {
                try
                {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(value);
                    put.add(familyName.getBytes(), columnName.getBytes(), bos.toByteArray());
                    oos.close();
                    bos.close();
                }
                catch (IOException e)
                {
                    throw new NucleusException(e.getMessage(), e);
                }
            }
            else
            {
                put.add(familyName.getBytes(), columnName.getBytes(), value.getBytes());
            }
        }
    }
}