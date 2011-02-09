/**********************************************************************
Copyright (c) 2011 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
   ...
**********************************************************************/
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

/**
 * FieldManager for the retrieval of a related embedded object (1-1 relation).
 */
public class FetchEmbeddedFieldManager extends AbstractFieldManager
{
    private final AbstractMemberMetaData mmd;
    private final Result result;
    private final ExecutionContext ec;
    private final ObjectProvider sm;
    private final String tableName;

    public FetchEmbeddedFieldManager(ObjectProvider sm, Result result, AbstractMemberMetaData mmd, String tableName)
    {
        this.result = result;
        this.sm = sm;
        this.ec = sm.getExecutionContext();
        this.mmd = mmd;
        this.tableName = tableName;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchBooleanField(int)
     */
    @Override
    public boolean fetchBooleanField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(mmd, fieldNumber, tableName);
        String columnName = HBaseUtils.getQualifierName(mmd, fieldNumber);
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchByteField(int)
     */
    @Override
    public byte fetchByteField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(mmd, fieldNumber, tableName);
        String columnName = HBaseUtils.getQualifierName(mmd, fieldNumber);
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        return bytes[0];
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchCharField(int)
     */
    @Override
    public char fetchCharField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(mmd, fieldNumber, tableName);
        String columnName = HBaseUtils.getQualifierName(mmd, fieldNumber);
        AbstractMemberMetaData embMmd = mmd.getEmbeddedMetaData().getMemberMetaData()[fieldNumber];
        char value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            if (embMmd.isSerialized())
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchDoubleField(int)
     */
    @Override
    public double fetchDoubleField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(mmd, fieldNumber, tableName);
        String columnName = HBaseUtils.getQualifierName(mmd, fieldNumber);
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchFloatField(int)
     */
    @Override
    public float fetchFloatField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(mmd, fieldNumber, tableName);
        String columnName = HBaseUtils.getQualifierName(mmd, fieldNumber);
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchIntField(int)
     */
    @Override
    public int fetchIntField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(mmd, fieldNumber, tableName);
        String columnName = HBaseUtils.getQualifierName(mmd, fieldNumber);
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchLongField(int)
     */
    @Override
    public long fetchLongField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(mmd, fieldNumber, tableName);
        String columnName = HBaseUtils.getQualifierName(mmd, fieldNumber);
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchShortField(int)
     */
    @Override
    public short fetchShortField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(mmd, fieldNumber, tableName);
        String columnName = HBaseUtils.getQualifierName(mmd, fieldNumber);
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchStringField(int)
     */
    @Override
    public String fetchStringField(int fieldNumber)
    {
        String familyName = HBaseUtils.getFamilyName(mmd, fieldNumber, tableName);
        String columnName = HBaseUtils.getQualifierName(mmd, fieldNumber);
        AbstractMemberMetaData embMmd = mmd.getEmbeddedMetaData().getMemberMetaData()[fieldNumber];
        String value;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            if (bytes != null)
            {
                if (embMmd.isSerialized())
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

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchObjectField(int)
     */
    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData embMmd = mmd.getEmbeddedMetaData().getMemberMetaData()[fieldNumber];
        int relationType = embMmd.getRelationType(clr);
        if (embMmd.isEmbedded() && Relation.isRelationSingleValued(relationType))
        {
            // Persistable object embedded into table of this object
            Class embcls = embMmd.getType();
            AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(embcls, clr);
            if (embcmd != null)
            {
                // Check for null value (currently need all columns to return null)
                // TODO Cater for null using embmd.getNullIndicatorColumn etc
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

                ObjectProvider embSM = ec.newObjectProviderForMember(embMmd, embcmd);
                embSM.addEmbeddedOwner(sm, fieldNumber);
                FieldManager ffm = new FetchEmbeddedFieldManager(embSM, result, embMmd, tableName);
                embSM.replaceFields(embcmd.getAllMemberPositions(), ffm);
                return embSM.getObject();
            }
            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " marked as embedded but no such metadata");
        }

        String familyName = HBaseUtils.getFamilyName(mmd, fieldNumber, tableName);
        String columnName = HBaseUtils.getQualifierName(mmd, fieldNumber);
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

        if (Relation.isRelationSingleValued(relationType))
        {
            if (embMmd.isSerialized())
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
            if (embMmd.hasCollection())
            {
                if (embMmd.isSerialized())
                {
                    return value;
                }

                Collection<Object> coll;
                try
                {
                    Class instanceType = SCOUtils.getContainerInstanceType(embMmd.getType(), embMmd.getOrderMetaData() != null);
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
            else if (embMmd.hasMap())
            {
                if (embMmd.isSerialized())
                {
                    return value;
                }
            }
            else if (embMmd.hasArray())
            {
                if (embMmd.isSerialized())
                {
                    return value;
                }

                Collection arrIds = (Collection)value;
                Object array = Array.newInstance(embMmd.getType().getComponentType(), arrIds.size());
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
                sm.getExecutionContext().getNucleusContext().getTypeManager().getStringConverter(value.getClass());
            ObjectLongConverter longConv = 
                sm.getExecutionContext().getNucleusContext().getTypeManager().getLongConverter(value.getClass());
            Object returnValue = null;
            if (!embMmd.isSerialized())
            {
                if (strConv != null)
                {
                    // Persisted as a String, so convert back
                    String strValue = (String)value;
                    returnValue = strConv.toObject(strValue);
                }
                if (longConv != null)
                {
                    // Persisted as a String, so convert back
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
}