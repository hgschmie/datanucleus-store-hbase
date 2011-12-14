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
2011 Andy Jefferson - add family/column getters for datastore id, version, embedded fields
2011 Andy Jefferson - extended schema creation, and added schema deletion
    ...
***********************************************************************/
package org.datanucleus.store.hbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUnsupportedOptionException;
import org.datanucleus.identity.OID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.IdentityMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.Relation;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.types.ObjectStringConverter;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

public class HBaseUtils
{
    /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.hbase.Localisation", HBaseStoreManager.class.getClassLoader());

    /**
     * Accessor for the default value specified for the provided member.
     * If no defaultValue is provided on the column then returns null.
     * @param mmd Metadata for the member
     * @return The default value
     */
    public static String getDefaultValueForMember(AbstractMemberMetaData mmd)
    {
        ColumnMetaData[] colmds = mmd.getColumnMetaData();
        if (colmds == null || colmds.length < 1)
        {
            return null;
        }
        return colmds[0].getDefaultValue();
    }

    /**
     * Accessor for the HBase table name for this class.
     * @param acmd Metadata for the class
     * @return The table name
     */
    public static String getTableName(AbstractClassMetaData acmd)
    {
        if (acmd.getTable() != null)
        {
            return acmd.getTable();
        }
        return acmd.getName();
    }

    /**
     * Accessor for the HBase family name for the field of the embedded field.
     * Extracts the family name using the following priorities
     * <ul>
     * <li>If column is specified as "a:b" then takes "a" as the family name.</li>
     * <li>Otherwise takes table name as the family name</li>
     * </ul>
     * @param mmd Metadata for the embedded field
     * @param fieldNumber Number of the field in the embedded object
     * @return The family name
     */
    public static String getFamilyName(AbstractMemberMetaData mmd, int fieldNumber, String tableNameForClass)
    {
        String columnName = null;

        // Try from the column name if specified as "a:b"
        EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
        AbstractMemberMetaData embMmd = null;
        if (embmd != null)
        {
            AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
            embMmd = embmmds[fieldNumber];
            ColumnMetaData[] colmds = embMmd.getColumnMetaData();
            if (colmds != null && colmds.length > 0)
            {
                columnName = colmds[0].getName();
                if (columnName!= null && columnName.indexOf(":")>-1)
                {
                    return columnName.substring(0,columnName.indexOf(":"));
                }
            }
        }

        // Fallback to table name
        return tableNameForClass;
    }

    /**
     * Accessor for the HBase qualifier name for the field of this embedded field.
     * Extracts the qualifier name using the following priorities
     * <ul>
     * <li>If column is specified as "a:b" then takes "b" as the qualifier name.</li>
     * <li>Otherwise takes the column name as the qualifier name when it is specified</li>
     * <li>Otherwise takes "VERSION" as the qualifier name</li>
     * </ul>
     * @param mmd Metadata for the owning member
     * @param fieldNumber Member number of the embedded object
     * @return The qualifier name
     */
    public static String getQualifierName(AbstractMemberMetaData mmd, int fieldNumber)
    {
        String columnName = null;

        // Try the first column if specified
        EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
        AbstractMemberMetaData embMmd = null;
        if (embmd != null)
        {
            AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
            embMmd = embmmds[fieldNumber];
        }

        ColumnMetaData[] colmds = embMmd.getColumnMetaData();
        if (colmds != null && colmds.length > 0)
        {
            columnName = colmds[0].getName();
        }
        if (columnName == null)
        {
            // Fallback to the field/property name
            columnName = embMmd.getName();
        }
        if (columnName.indexOf(":")>-1)
        {
            columnName = columnName.substring(columnName.indexOf(":")+1);
        }
        return columnName;
    }

    /**
     * Accessor for the HBase family name for the identity of this class.
     * Extracts the family name using the following priorities
     * <ul>
     * <li>If column is specified as "a:b" then takes "a" as the family name.</li>
     * <li>Otherwise takes table name as the family name</li>
     * </ul>
     * @param idmd Metadata for the identity
     * @return The family name
     */
    public static String getFamilyName(IdentityMetaData idmd)
    {
        String columnName = null;

        // Try from the column name if specified as "a:b"
        ColumnMetaData colmd = idmd.getColumnMetaData();
        if (colmd != null)
        {
            columnName = colmd.getName();
            if (columnName!= null && columnName.indexOf(":")>-1)
            {
                return columnName.substring(0,columnName.indexOf(":"));
            }
        }

        // Fallback to table name.
        return getTableName((AbstractClassMetaData)idmd.getParent());
    }

    /**
     * Accessor for the HBase qualifier name for this identity.
     * Extracts the qualifier name using the following priorities
     * <ul>
     * <li>If column is specified as "a:b" then takes "b" as the qualifier name.</li>
     * <li>Otherwise takes the column name as the qualifier name when it is specified</li>
     * <li>Otherwise takes "VERSION" as the qualifier name</li>
     * </ul>
     * @param idmd Metadata for the Identity
     * @return The qualifier name
     */
    public static String getQualifierName(IdentityMetaData idmd)
    {
        String columnName = null;

        // Try the first column if specified
        ColumnMetaData colmd = idmd.getColumnMetaData();
        if (colmd != null)
        {
            columnName = colmd.getName();
        }
        if (columnName == null)
        {
            // Fallback to "IDENTITY"
            columnName = "IDENTITY";
        }
        if (columnName.indexOf(":")>-1)
        {
            columnName = columnName.substring(columnName.indexOf(":")+1);
        }
        return columnName;
    }

    /**
     * Accessor for the HBase family name for the discriminator of this class.
     * Extracts the family name using the following priorities
     * <ul>
     * <li>If column is specified as "a:b" then takes "a" as the family name.</li>
     * <li>Otherwise takes table name as the family name</li>
     * </ul>
     * @param dismd Metadata for the discriminator
     * @return The family name
     */
    public static String getFamilyName(DiscriminatorMetaData dismd)
    {
        String columnName = null;

        // Try from the column name if specified as "a:b"
        ColumnMetaData colmd = dismd.getColumnMetaData();
        if (colmd != null)
        {
            columnName = colmd.getName();
            if (columnName!= null && columnName.indexOf(":")>-1)
            {
                return columnName.substring(0,columnName.indexOf(":"));
            }
        }

        // Fallback to table name
        return getTableName((AbstractClassMetaData)dismd.getParent().getParent());
    }

    /**
     * Accessor for the HBase qualifier name for this discriminator.
     * Extracts the qualifier name using the following priorities
     * <ul>
     * <li>If column is specified as "a:b" then takes "b" as the qualifier name.</li>
     * <li>Otherwise takes the column name as the qualifier name when it is specified</li>
     * <li>Otherwise takes "DISCRIM" as the qualifier name</li>
     * </ul>
     * @param dismd Metadata for the discriminator
     * @return The qualifier name
     */
    public static String getQualifierName(DiscriminatorMetaData dismd)
    {
        String columnName = null;

        // Try the column if specified
        ColumnMetaData colmd = dismd.getColumnMetaData();
        if (colmd != null)
        {
            columnName = colmd.getName();
        }
        if (columnName == null)
        {
            // Fallback to "DISCRIM"
            columnName = "DISCRIM";
        }
        if (columnName.indexOf(":")>-1)
        {
            columnName = columnName.substring(columnName.indexOf(":")+1);
        }
        return columnName;
    }

    /**
     * Accessor for the HBase family name for the version of this class.
     * Extracts the family name using the following priorities
     * <ul>
     * <li>If column is specified as "a:b" then takes "a" as the family name.</li>
     * <li>Otherwise takes table name as the family name</li>
     * </ul>
     * @param vermd Metadata for the version
     * @return The family name
     */
    public static String getFamilyName(VersionMetaData vermd)
    {
        String columnName = null;

        // Try from the column name if specified as "a:b"
        ColumnMetaData colmd = vermd.getColumnMetaData();
        if (colmd != null)
        {
            columnName = colmd.getName();
            if (columnName!= null && columnName.indexOf(":")>-1)
            {
                return columnName.substring(0,columnName.indexOf(":"));
            }
        }

        // Fallback to table name.
        return getTableName((AbstractClassMetaData)vermd.getParent());
    }

    /**
     * Accessor for the HBase qualifier name for this version.
     * Extracts the qualifier name using the following priorities
     * <ul>
     * <li>If column is specified as "a:b" then takes "b" as the qualifier name.</li>
     * <li>Otherwise takes the column name as the qualifier name when it is specified</li>
     * <li>Otherwise takes "VERSION" as the qualifier name</li>
     * </ul>
     * @param vermd Metadata for the version
     * @return The qualifier name
     */
    public static String getQualifierName(VersionMetaData vermd)
    {
        String columnName = null;

        // Try the first column if specified
        ColumnMetaData colmd = vermd.getColumnMetaData();
        if (colmd != null)
        {
            columnName = colmd.getName();
        }
        if (columnName == null)
        {
            // Fallback to "VERSION"
            columnName = "VERSION";
        }
        if (columnName.indexOf(":")>-1)
        {
            columnName = columnName.substring(columnName.indexOf(":")+1);
        }
        return columnName;
    }

    /**
     * Accessor for the HBase family name for this field. Extracts the family name using the following priorities
     * <ul>
     * <li>If column is specified as "a:b" then takes "a" as the family name.</li>
     * <li>Otherwise takes the table name as the family name</li>
     * </ul>
     * @param acmd Metadata for the class
     * @param absoluteFieldNumber Field number
     * @return The family name
     */
    public static String getFamilyName(AbstractClassMetaData acmd, int absoluteFieldNumber)
    {
        AbstractMemberMetaData ammd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(absoluteFieldNumber);
        String columnName = null;

        // Try the first column if specified
        ColumnMetaData[] colmds = ammd.getColumnMetaData();
        if (colmds != null && colmds.length > 0)
        {
            columnName = colmds[0].getName();
            if (columnName != null && columnName.indexOf(":")>-1)
            {
                return columnName.substring(0,columnName.indexOf(":"));
            }
        }

        // Fallback to the table name
        return HBaseUtils.getTableName(acmd);
    }

    /**
     * Accessor for the HBase qualifier name for this field. Extracts the qualifier name using the following priorities
     * <ul>
     * <li>If column is specified as "a:b" then takes "b" as the qualifier name.</li>
     * <li>Otherwise takes the column name as the qualifier name when it is specified</li>
     * <li>Otherwise takes the field name as the qualifier name</li>
     * </ul>
     * @param acmd Metadata for the class
     * @param absoluteFieldNumber Field number
     * @return The qualifier name
     */
    public static String getQualifierName(AbstractClassMetaData acmd, int absoluteFieldNumber)
    {
        AbstractMemberMetaData ammd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(absoluteFieldNumber);
        String columnName = null;

        // Try the first column if specified
        ColumnMetaData[] colmds = ammd.getColumnMetaData();
        if (colmds != null && colmds.length > 0)
        {
            columnName = colmds[0].getName();
        }
        if (columnName == null)
        {
            // Fallback to the field/property name
            columnName = ammd.getName();
        }
        if (columnName.indexOf(":")>-1)
        {
            columnName = columnName.substring(columnName.indexOf(":")+1);
        }
        return columnName;
    }

    /**
     * Create a schema in HBase. Do not make this method public, since it uses privileged actions.
     * @param storeMgr HBase StoreManager
     * @param acmd Metadata for the class
     * @param autoCreateColumns Whether auto-create of columns is set
     * @param validateOnly Whether to only validate for existence and flag missing schema in the log
     */
    static void createSchemaForClass(final HBaseStoreManager storeMgr, final AbstractClassMetaData acmd,
            final boolean validateOnly)
    {
        if (acmd.isEmbeddedOnly())
        {
            // No schema required since only ever embedded
            return;
        }

        final Configuration config = storeMgr.getHbaseConfig();
        try
        {
            final HBaseAdmin hBaseAdmin = (HBaseAdmin) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    return new HBaseAdmin(config);
                }
            });

            final HTableDescriptor hTable = (HTableDescriptor) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    String tableName = HBaseUtils.getTableName(acmd);
                    HTableDescriptor hTable = null;
                    try
                    {
                        hTable = hBaseAdmin.getTableDescriptor(tableName.getBytes());
                    }
                    catch (TableNotFoundException ex)
                    {
                        if (validateOnly)
                        {
                            NucleusLogger.DATASTORE_SCHEMA.info(LOCALISER.msg("HBase.SchemaValidate.Class",
                                acmd.getFullClassName(), tableName));
                            hTable = null;
                        }
                        else if (storeMgr.isAutoCreateTables())
                        {
                            NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("HBase.SchemaCreate.Class",
                                acmd.getFullClassName(), tableName));
                            hTable = new HTableDescriptor(tableName);
                            hBaseAdmin.createTable(hTable);
                        }
                    }
                    return hTable;
                }
            });

            if (hTable != null)
            {
                boolean modified = false;
                String tableName = HBaseUtils.getTableName(acmd);
                if (!hTable.hasFamily(tableName.getBytes()))
                {
                    if (validateOnly)
                    {
                        NucleusLogger.DATASTORE_SCHEMA.info(LOCALISER.msg("HBase.SchemaValidate.Class.Family",
                            tableName, tableName));
                    }
                    else if (storeMgr.isAutoCreateColumns())
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("HBase.SchemaCreate.Class.Family",
                            tableName, tableName));
                        HColumnDescriptor hColumn = new HColumnDescriptor(tableName);
                        hTable.addFamily(hColumn);
                        modified = true;
                    }
                }

                int[] fieldNumbers =  acmd.getAllMemberPositions();
                ClassLoaderResolver clr = storeMgr.getNucleusContext().getClassLoaderResolver(null);
                for(int i=0; i<fieldNumbers.length; i++)
                {
                    AbstractMemberMetaData mmd = acmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]);
                    int relationType = mmd.getRelationType(clr);
                    if ((relationType == Relation.ONE_TO_ONE_UNI || relationType == Relation.ONE_TO_ONE_BI) && mmd.isEmbedded())
                    {
                        createSchemaForEmbeddedMember(storeMgr, hTable, mmd, clr, validateOnly);
                    }
                    else
                    {
                        String familyName = getFamilyName(acmd, fieldNumbers[i]);
                        if (!hTable.hasFamily(familyName.getBytes()))
                        {
                            if (validateOnly)
                            {
                                NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("HBase.SchemaValidate.Class.Family",
                                    tableName, familyName));
                            }
                            else if (storeMgr.isAutoCreateColumns())
                            {
                                NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("HBase.SchemaCreate.Class.Family",
                                    tableName, familyName));
                                HColumnDescriptor hColumn = new HColumnDescriptor(familyName);
                                hTable.addFamily(hColumn);
                                modified = true;
                            }
                        }
                    }
                }

                if (modified)
                {
                    AccessController.doPrivileged(new PrivilegedExceptionAction()
                    {
                        public Object run() throws Exception
                        {
                            hBaseAdmin.disableTable(hTable.getName());
                            hBaseAdmin.modifyTable(hTable.getName(), hTable);
                            hBaseAdmin.enableTable(hTable.getName());
                            return null;
                        }
                    });
                }
            }
        }
        catch (PrivilegedActionException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e.getCause());
        }
    }

    static boolean createSchemaForEmbeddedMember(HBaseStoreManager storeMgr, HTableDescriptor hTable,
            AbstractMemberMetaData mmd, ClassLoaderResolver clr, boolean validateOnly)
    {
        boolean modified = false;

        String tableName = hTable.getNameAsString();
        EmbeddedMetaData embmd = mmd.getEmbeddedMetaData();
        AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
        for (int j=0;j<embmmds.length;j++)
        {
            AbstractMemberMetaData embMmd = embmmds[j];
            int embRelationType = embMmd.getRelationType(clr);
            if ((embRelationType == Relation.ONE_TO_ONE_UNI || embRelationType == Relation.ONE_TO_ONE_BI) && embMmd.isEmbedded())
            {
                // Recurse
                return createSchemaForEmbeddedMember(storeMgr, hTable, embMmd, clr, validateOnly);
            }
            else
            {
                String familyName = HBaseUtils.getFamilyName(embMmd, j, tableName);
                if (!hTable.hasFamily(familyName.getBytes()))
                {
                    if (validateOnly)
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("HBase.SchemaValidate.Class.Family",
                            tableName, familyName));
                    }
                    else
                    {
                        NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("HBase.SchemaCreate.Class.Family",
                            tableName, familyName));
                        HColumnDescriptor hColumn = new HColumnDescriptor(familyName);
                        hTable.addFamily(hColumn);
                        modified = true;
                    }
                }
            }
        }

        return modified;
    }

    /**
     * Delete the schema for the specified class from HBase.
     * Do not make this method public, since it uses privileged actions
     * @param storeMgr HBase StoreManager
     * @param acmd Metadata for the class
     * @param autoCreateColumns
     */
    static void deleteSchemaForClass(final HBaseStoreManager storeMgr, final AbstractClassMetaData acmd)
    {
        if (acmd.isEmbeddedOnly())
        {
            // No schema present since only ever embedded
            return;
        }

        final Configuration config = storeMgr.getHbaseConfig();
        try
        {
            final HBaseAdmin hBaseAdmin = (HBaseAdmin) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    return new HBaseAdmin(config);
                }
            });

            final HTableDescriptor hTable = (HTableDescriptor) AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    String tableName = HBaseUtils.getTableName(acmd);
                    HTableDescriptor hTable;
                    try
                    {
                        hTable = hBaseAdmin.getTableDescriptor(tableName.getBytes());
                    }
                    catch (TableNotFoundException ex)
                    {
                        hTable = new HTableDescriptor(tableName);
                        hBaseAdmin.createTable(hTable);
                    }
                    return hTable;
                }
            });

            AccessController.doPrivileged(new PrivilegedExceptionAction()
            {
                public Object run() throws Exception
                {
                    NucleusLogger.DATASTORE_SCHEMA.debug(LOCALISER.msg("HBase.SchemaDelete.Class",
                        acmd.getFullClassName(), hTable.getNameAsString()));
                    hBaseAdmin.disableTable(hTable.getName());
                    hBaseAdmin.deleteTable(hTable.getName());
                    return null;
                }
            });
        }
        catch (PrivilegedActionException e)
        {
            throw new NucleusDataStoreException(e.getMessage(), e.getCause());
        }
    }

    /**
     * Convenience method that extracts the version for a class of the specified type from the passed Result.
     * @param cmd Metadata for the class
     * @param result The result
     * @return The version
     */
    public static Object getVersionForObject(AbstractClassMetaData cmd, Result result, ExecutionContext ec)
    {
        if (cmd.isVersioned())
        {
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getFieldName() != null)
            {
                // Version stored in a field
                AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                String familyName = HBaseUtils.getFamilyName(cmd, verMmd.getAbsoluteFieldNumber());
                String columnName = HBaseUtils.getQualifierName(cmd, verMmd.getAbsoluteFieldNumber());
                Object version = null;
                try
                {
                    byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
                    if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
                    {
                        version = Bytes.toLong(bytes);
                    }
                    else if (Date.class.isAssignableFrom(verMmd.getType()))
                    {
                        // Field is of type Date (hence persisted as String), but version needs to be Timestamp
                        String strValue = new String(bytes);
                        ObjectStringConverter strConv = ec.getNucleusContext().getTypeManager().getStringConverter(verMmd.getType());
                        version = strConv.toObject(strValue);
                        version = new Timestamp(((Date)version).getTime());
                    }
                    else
                    {
                        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                        ObjectInputStream ois = new ObjectInputStream(bis);
                        version = ois.readObject();
                        ois.close();
                        bis.close();
                    }
                }
                catch (Exception e)
                {
                    throw new NucleusException(e.getMessage(), e);
                }
                return version;
            }
            else
            {
                return getSurrogateVersionForObject(cmd, result);
            }
        }
        return null;
    }

    /**
     * Convenience method that extracts the surrogate version for a class of the specified type from
     * the passed Result.
     * @param cmd Metadata for the class
     * @param result The result
     * @return The surrogate version
     */
    public static Object getSurrogateVersionForObject(AbstractClassMetaData cmd, Result result)
    {
        VersionMetaData vermd = cmd.getVersionMetaDataForClass();
        String familyName = HBaseUtils.getFamilyName(vermd);
        String columnName = HBaseUtils.getQualifierName(vermd);
        Object version = null;
        try
        {
            byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
            if (vermd.getVersionStrategy() == VersionStrategy.VERSION_NUMBER)
            {
                version = Bytes.toLong(bytes);
            }
            else
            {
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                version = ois.readObject();
                ois.close();
                bis.close();
            }
        }
        catch (Exception e)
        {
            throw new NucleusException(e.getMessage(), e);
        }
        return version;
    }

    /**
     * Convenience method that extracts the discriminator for a class of the specified type from
     * the passed Result.
     * @param cmd Metadata for the class
     * @param result The result
     * @return The discriminator
     */
    public static Object getDiscriminatorForObject(AbstractClassMetaData cmd, Result result)
    {
        String familyName = HBaseUtils.getFamilyName(cmd.getDiscriminatorMetaData());
        String columnName = HBaseUtils.getQualifierName(cmd.getDiscriminatorMetaData());
        byte[] bytes = result.getValue(familyName.getBytes(), columnName.getBytes());
        return new String(bytes);
    }

    public static Put getPutForObject(ObjectProvider sm) throws IOException
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        boolean serialisedPk = sm.getExecutionContext().getNucleusContext().getPersistenceConfiguration().getBooleanProperty("datanucleus.hbase.serialisedPK");

        final Object [] pkValues = findKeyObjects(sm, cmd);
        byte[] pkBytes = getBytesForPkValue(pkValues, serialisedPk);
        return new Put(pkBytes);
    }

    public static Delete getDeleteForObject(ObjectProvider sm) throws IOException
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        boolean serialisedPk = sm.getExecutionContext().getNucleusContext().getPersistenceConfiguration().getBooleanProperty("datanucleus.hbase.serialisedPK");

        final Object [] pkValues = findKeyObjects(sm, cmd);
        byte[] pkBytes = getBytesForPkValue(pkValues, serialisedPk);
        return new Delete(pkBytes);
    }

    public static Result getResultForObject(ObjectProvider sm, HTable table) throws IOException
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        boolean serialisedPk = sm.getExecutionContext().getNucleusContext().getPersistenceConfiguration().getBooleanProperty("datanucleus.hbase.serialisedPK");

        final Object [] pkValues = findKeyObjects(sm, cmd);
        byte[] pkBytes = getBytesForPkValue(pkValues, serialisedPk);
        Get get = new Get(pkBytes);
        return table.get(get);
    }

    public static boolean objectExistsInTable(ObjectProvider sm, HTable table) throws IOException
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        boolean serialisedPk = sm.getExecutionContext().getNucleusContext().getPersistenceConfiguration().getBooleanProperty("datanucleus.hbase.serialisedPK");

        final Object [] pkValues = findKeyObjects(sm, cmd);
        byte[] pkBytes = getBytesForPkValue(pkValues, serialisedPk);
        Get get = new Get(pkBytes);
        return table.exists(get);
    }

    private static Object [] findKeyObjects(final ObjectProvider sm, final AbstractClassMetaData cmd)
    {
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            return new Object [] { ((OID)sm.getInternalObjectId()).getKeyValue() };
        }
        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            final int [] pos = sm.getClassMetaData().getPKMemberPositions();
            final Object [] keyObjects = new Object[pos.length];
            for (int i = 0; i < pos.length; i++)
            {
                keyObjects[i] = sm.provideField(pos[i]);
            }
            return keyObjects;
        }

        throw new NucleusUnsupportedOptionException("unsupported identity type: " + cmd.getIdentityType());
    }

    static byte[] getBytesForPkValue(Object [] pkValues, boolean useSerialisation) throws IOException
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        try
        {
            if (useSerialisation)
            {
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                try {
                    // Legacy data, up to and including DN 3.0.1
                    for (Object pkValue : pkValues)
                    {
                        oos.writeObject(pkValue);
                    }
                }
                finally {
                    oos.close();
                }
            }
            else
            {
                for (Object pkValue : pkValues)
                {
                    if (pkValue instanceof String)
                    {
                        bos.write(Bytes.toBytes((String)pkValue));
                    }
                    else if (pkValue instanceof Long)
                    {
                        bos.write(Bytes.toBytes(((Long)pkValue).longValue()));
                    }
                    else if (pkValue instanceof Integer)
                    {
                        bos.write(Bytes.toBytes(((Integer)pkValue).intValue()));
                    }
                    else if (pkValue instanceof Short)
                    {
                        bos.write(Bytes.toBytes(((Short)pkValue).shortValue()));
                    }
                    else
                    {
                        // Object serialisation approach. Can't keep that open from the very
                        // beginning, it messes up the byte stream.
                        ObjectOutputStream oos = new ObjectOutputStream(bos);
                        try {
                            oos.writeObject(pkValue);
                        }
                        finally {
                            oos.close();
                        }
                    }
                }
            }
            return bos.toByteArray();
        }
        finally
        {
            bos.close();
        }
    }
}
