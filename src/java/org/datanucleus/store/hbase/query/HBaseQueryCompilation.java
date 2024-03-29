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

import org.apache.hadoop.hbase.filter.Filter;

/**
 * Datastore-specific (HBase) compilation information for a java query.
 */
public class HBaseQueryCompilation
{
    boolean filterComplete = true;

    Filter filter;

    boolean precompilable = true;

    public HBaseQueryCompilation()
    {
    }

    public void setPrecompilable(boolean precompilable)
    {
        this.precompilable = precompilable;
    }

    public boolean isPrecompilable()
    {
        return precompilable;
    }

    public void setFilter(Filter filter)
    {
        this.filter = filter;
    }

    public Filter getFilter()
    {
        return filter;
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public void setFilterComplete(boolean complete)
    {
        this.filterComplete = complete;
    }
}