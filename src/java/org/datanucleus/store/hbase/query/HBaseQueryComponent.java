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
package org.datanucleus.store.hbase.query;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Component of a HBase query. Consists of a HTable being queried, and the Scan to use.
 */
public class HBaseQueryComponent
{
    final HTable table;

    final Scan scan;

    public HBaseQueryComponent(HTable tbl, Scan scan)
    {
        this.table = tbl;
        this.scan = scan;
    }

    public HTable getTable()
    {
        return table;
    }

    public Scan getScan()
    {
        return scan;
    }
}