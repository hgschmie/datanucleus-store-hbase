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

import org.apache.hadoop.hbase.client.Scan;

/**
 * Datastore-specific (HBase) compilation information for a java query.
 */
public class HBaseQueryCompilation
{
    boolean filterComplete = true;
    boolean resultComplete = true;

    /** Scan defining the result and filter components (if possible). */
    Scan scan = null;

    public HBaseQueryCompilation()
    {
    }

    public void setScan(Scan scan)
    {
        this.scan = scan;
    }

    public Scan getScan()
    {
        return scan;
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public void setFilterComplete(boolean complete)
    {
        this.filterComplete = complete;
    }

    public boolean isResultComplete()
    {
        return resultComplete;
    }

    public void setResultComplete(boolean complete)
    {
        this.resultComplete = complete;
    }
}