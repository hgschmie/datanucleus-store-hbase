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

import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hbase.client.Scan;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.compiler.CompilationComponent;
import org.datanucleus.query.compiler.QueryCompilation;
import org.datanucleus.query.evaluator.AbstractExpressionEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.ParameterExpression;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.NucleusLogger;

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

    Scan scan = null;

    /** Whether the filter clause is completely evaluatable in the datastore. */
    boolean filterComplete = true;

    /** Whether the result clause is completely evaluatable in the datastore. */
    boolean resultComplete = true;

    Stack stack = new Stack();

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

    public boolean isResultComplete()
    {
        return resultComplete;
    }

    public Scan getScan()
    {
        return scan;
    }

    public void compile()
    {
        scan = new Scan();

        compileFrom();
        compileFilter();
        compileResult();
        compileGrouping();
        compileHaving();
        compileOrdering();
    }

    /**
     * Method to compile the FROM clause of the query
     */
    protected void compileFrom()
    {
        if (compilation.getExprFrom() != null)
        {
            // Process all ClassExpression(s) in the FROM, adding joins to the statement as required
            compileComponent = CompilationComponent.FROM;
            Expression[] fromExprs = compilation.getExprFrom();
            for (int i=0;i<fromExprs.length;i++)
            {
                // TODO Compile FROM class expression
            }
        }
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

    /**
     * Method to compile the result clause of the query
     */
    protected void compileResult()
    {
        if (compilation.getExprResult() != null)
        {
            compileComponent = CompilationComponent.RESULT;

            // Select any result expressions
            Expression[] resultExprs = compilation.getExprResult();
            for (int i=0;i<resultExprs.length;i++)
            {
                // TODO Compile this
            }
        }
        // TODO Handle distinct
        compileComponent = null;
    }

    /**
     * Method to compile the grouping clause of the query
     */
    protected void compileGrouping()
    {
        if (compilation.getExprGrouping() != null)
        {
            // Apply any grouping to the statement
            compileComponent = CompilationComponent.GROUPING;
            Expression[] groupExprs = compilation.getExprGrouping();
            for (int i = 0; i < groupExprs.length; i++)
            {
                // TODO Compile grouping
            }
            compileComponent = null;
        }
    }

    /**
     * Method to compile the having clause of the query
     */
    protected void compileHaving()
    {
        if (compilation.getExprHaving() != null)
        {
            // Apply any having to the statement
            compileComponent = CompilationComponent.HAVING;
            /*Expression havingExpr = */compilation.getExprHaving();
            // TODO Compile having
            compileComponent = null;
        }
    }

    /**
     * Method to compile the ordering clause of the query
     */
    protected void compileOrdering()
    {
        if (compilation.getExprOrdering() != null)
        {
            compileComponent = CompilationComponent.ORDERING;
            Expression[] orderingExpr = compilation.getExprOrdering();
            for (int i=0;i<orderingExpr.length;i++)
            {
                // TODO Compile ordering
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
        // TODO Auto-generated method stub
        return super.processOrExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processAndExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processAndExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processAndExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processEqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processEqExpression(Expression expr)
    {
        Object right = stack.pop();
        Object left = stack.pop();
        NucleusLogger.QUERY.debug(">> HBase.eq left="+left+" right="+right);
        // TODO Auto-generated method stub
        return super.processEqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processNoteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processNoteqExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processNoteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGtExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processGtExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLtExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLtExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processLtExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processGteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processGteqExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processGteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processLteqExpression(org.datanucleus.query.expression.Expression)
     */
    @Override
    protected Object processLteqExpression(Expression expr)
    {
        // TODO Auto-generated method stub
        return super.processLteqExpression(expr);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.query.evaluator.AbstractExpressionEvaluator#processPrimaryExpression(org.datanucleus.query.expression.PrimaryExpression)
     */
    @Override
    protected Object processPrimaryExpression(PrimaryExpression expr)
    {
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
        // TODO Auto-generated method stub
        return super.processLiteral(expr);
    }
}