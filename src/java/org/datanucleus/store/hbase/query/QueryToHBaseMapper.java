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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hbase.filter.Filter;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.hbase.HBaseUtils;
import org.datanucleus.store.hbase.query.expression.HBaseBooleanExpression;
import org.datanucleus.store.hbase.query.expression.HBaseExpression;
import org.datanucleus.store.hbase.query.expression.HBaseFieldExpression;
import org.datanucleus.store.hbase.query.expression.HBaseLiteral;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;

/**
 * Class which maps a compiled (generic) query to a HBase query.
 */
public class QueryToHBaseMapper extends AbstractExpressionEvaluator
{
    final ExecutionContext ec;

    final String candidateAlias;

    final AbstractClassMetaData candidateCmd;

    final Query query;

    final QueryCompilation compilation;

    /** Input parameter values, keyed by the parameter name. Will be null if compiled pre-execution. */
    final Map parameters;

    /** State variable for the component being compiled. */
    CompilationComponent compileComponent;

    Filter filter = null;

    /** Whether the filter clause is completely evaluatable in the datastore. */
    boolean filterComplete = true;

    Stack<HBaseExpression> stack = new Stack();

    public QueryToHBaseMapper(QueryCompilation compilation, Map parameters, AbstractClassMetaData cmd,
            ExecutionContext ec, Query q)
    {
        this.ec = ec;
        this.query = q;
        this.compilation = compilation;
        this.parameters = parameters;
        this.candidateCmd = cmd;
        this.candidateAlias = compilation.getCandidateAlias();
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public Filter getFilter()
    {
        return filter;
    }

    public void compile()
    {
        compileFilter();
    }

    /**
     * Method to compile the WHERE clause of the query
     */
    protected void compileFilter()
    {
        if (compilation.getExprFilter() != null)
        {
            compileComponent = CompilationComponent.FILTER;

            try
            {
                NucleusLogger.QUERY.debug(">> Evaluating filter="+compilation.getExprFilter());
                compilation.getExprFilter().evaluate(this);
                HBaseExpression filterExpr = stack.pop();
                if (filterExpr instanceof HBaseBooleanExpression)
                {
                    this.filter = ((HBaseBooleanExpression)filterExpr).getFilter();
                    NucleusLogger.GENERAL.info(">> filter evaluated as "+filter);
                }
                else
                {
                    NucleusLogger.QUERY.error(">> invalid compilation : filter compiled to " + filterExpr);
                    filterComplete = false;
                }
            }
            catch (Exception e)
            {
                // Impossible to compile all to run in the datastore, so just exit
                filterComplete = false;
                NucleusLogger.QUERY.debug(">> compileFilter caught exception ", e);
            }

            compileComponent = null;
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processOrExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processOrExpression(Expression expr)
    {
        HBaseBooleanExpression right = (HBaseBooleanExpression) stack.pop();
        HBaseBooleanExpression left = (HBaseBooleanExpression) stack.pop();
        HBaseBooleanExpression orExpr = new HBaseBooleanExpression(left, right, Expression.OP_OR);
        stack.push(orExpr);
        return orExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processAndExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processAndExpression(Expression expr)
    {
        HBaseBooleanExpression right = (HBaseBooleanExpression) stack.pop();
        HBaseBooleanExpression left = (HBaseBooleanExpression) stack.pop();
        HBaseBooleanExpression andExpr = new HBaseBooleanExpression(left, right, Expression.OP_AND);
        stack.push(andExpr);
        return andExpr;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processEqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processEqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_EQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_EQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processEqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processNoteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processNoteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_NOTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_NOTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processNoteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGtExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_LTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_GT);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processGtExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLtExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_GTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_LT);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processLtExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_LT);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_GTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processGteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLteqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        if (left instanceof HBaseLiteral && right instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)right;
            HBaseLiteral litExpr = (HBaseLiteral)left;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_GT);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }
        else if (right instanceof HBaseLiteral && left instanceof HBaseFieldExpression)
        {
            HBaseFieldExpression fieldExpr = (HBaseFieldExpression)left;
            HBaseLiteral litExpr = (HBaseLiteral)right;
            HBaseExpression hbaseExpr = new HBaseBooleanExpression(fieldExpr.getFamilyName(), fieldExpr.getColumnName(),
                litExpr.getValue(), Expression.OP_LTEQ);
            stack.push(hbaseExpr);
            return hbaseExpr;
        }

        return super.processLteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processPrimaryExpression(org.datanucleus.query.expression.PrimaryExpression)
     */
    @Override
    protected Object processPrimaryExpression(PrimaryExpression expr)
    {
        Expression left = expr.getLeft();
        if (left == null)
        {
            List<String> tuples = expr.getTuples();
            String famColName = getFamilyColumnNameForPrimary(tuples);
            if (famColName == null)
            {
                if (compileComponent == CompilationComponent.FILTER)
                {
                    filterComplete = false;
                }
                NucleusLogger.QUERY.debug(">> Primary " + expr +
                    " is not stored in this document, so unexecutable in datastore");
            }
            else
            {
                int sepPos = famColName.indexOf("###");
                String familyName = famColName.substring(0, sepPos);
                String columnName = famColName.substring(sepPos+3);
                HBaseFieldExpression fieldExpr = new HBaseFieldExpression(familyName, columnName);
                stack.push(fieldExpr);
                return fieldExpr;
            }
        }

        // TODO Auto-generated method stub
        return super.processPrimaryExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processParameterExpression(org.datanucleus.query.expression.ParameterExpression)
     */
    @Override
    protected Object processParameterExpression(ParameterExpression expr)
    {
        // TODO Auto-generated method stub
        return super.processParameterExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLiteral(org.datanucleus.query.expression.Literal)
     */
    @Override
    protected Object processLiteral(Literal expr)
    {
        Object litValue = expr.getLiteral();
        if (litValue instanceof Number)
        {
            HBaseLiteral lit = new HBaseLiteral(litValue);
            stack.push(lit);
            return lit;
        }
        else if (litValue instanceof String)
        {
            HBaseLiteral lit = new HBaseLiteral(litValue);
            stack.push(lit);
            return lit;
        }
        // TODO Handle all HBase supported (literal) types

        return super.processLiteral(expr);
    }

    /**
     * Convenience method to return the "{familyName}###{columnName}" in candidate for this primary.
     * Allows for non-relation fields, and (nested) embedded PC fields - i.e all fields that are present
     * in the table.
     * @param tuples Tuples for the primary
     * @return The family+column name for this primary (or null if not resolvable in this document)
     */
    protected String getFamilyColumnNameForPrimary(List<String> tuples)
    {
        if (tuples == null || tuples.size() == 0)
        {
            return null;
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractClassMetaData cmd = candidateCmd;

        Iterator<String> iter = tuples.iterator();
        while (iter.hasNext())
        {
            String name = iter.next();
            if (name.equals(candidateAlias))
            {
                cmd = candidateCmd;
            }
            else
            {
                AbstractMemberMetaData mmd = cmd.getMetaDataForMember(name);
                int relationType = mmd.getRelationType(clr);
                if (relationType == Relation.NONE)
                {
                    if (iter.hasNext())
                    {
                        throw new NucleusUserException("Query has reference to " +
                            StringUtils.collectionToString(tuples) + " yet " + name + " is a non-relation field!");
                    }

                    String familyName = HBaseUtils.getFamilyName(cmd, mmd.getAbsoluteFieldNumber());
                    String columnName = HBaseUtils.getQualifierName(cmd, mmd.getAbsoluteFieldNumber());
                    return familyName + "###" + columnName;
                }
                else
                {
                    if (compileComponent == CompilationComponent.FILTER)
                    {
                        filterComplete = false;
                    }
                    NucleusLogger.QUERY.debug("Query has reference to " +
                        StringUtils.collectionToString(tuples) + " and " + mmd.getFullFieldName() +
                        " is not persisted into this document, so unexecutable in the datastore");
                    return null;
                }
            }
        }

        return null;
    }
}