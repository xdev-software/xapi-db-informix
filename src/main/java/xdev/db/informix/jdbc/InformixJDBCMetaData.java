/*
 * SqlEngine Database Adapter Informix - XAPI SqlEngine Database Adapter for Informix
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package xdev.db.informix.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import com.informix.jdbc.IfxConnection;
import com.informix.jdbc.IfxDateTime;
import com.informix.jdbc.IfxStatement;
import com.informix.lang.IfxTypes;
import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;

import xdev.db.ColumnMetaData;
import xdev.db.DBException;
import xdev.db.DataType;
import xdev.db.Index;
import xdev.db.Index.IndexType;
import xdev.db.Result;
import xdev.db.StoredProcedure;
import xdev.db.StoredProcedure.Param;
import xdev.db.StoredProcedure.ParamType;
import xdev.db.StoredProcedure.ReturnTypeFlavor;
import xdev.db.jdbc.JDBCConnection;
import xdev.db.jdbc.JDBCMetaData;
import xdev.db.sql.Functions;
import xdev.db.sql.SELECT;
import xdev.db.sql.Table;
import xdev.util.ProgressMonitor;
import xdev.util.logging.LoggerFactory;
import xdev.util.logging.XdevLogger;
import xdev.vt.Cardinality;
import xdev.vt.EntityRelationship;
import xdev.vt.EntityRelationship.Entity;
import xdev.vt.EntityRelationshipModel;


public class InformixJDBCMetaData extends JDBCMetaData
{
	
	private static final XdevLogger LOGGER = LoggerFactory.getLogger(InformixJDBCMetaData.class);
	private HashMap<String, String> synMap;
	private boolean isGetSynonym;
	
	public InformixJDBCMetaData(final InformixJDBCDataSource dataSource) throws DBException
	{
		super(dataSource);
	}
	
	/**
	 * @return {@link xdev.db.DBMetaData.TableType#TABLE}   || {@link xdev.db.DBMetaData.TableType#VIEW}    ||
	 * {@link xdev.db.DBMetaData.TableType#SYNONYM} || {@link xdev.db.DBMetaData.TableType#OTHER}
	 */
	private static TableType identifyCorrectTableType(final String tableTypeName)
	{
		final TableType type;
		if(tableTypeName != null
			&& tableTypeName.equalsIgnoreCase(TableType.TABLE.name()))
		{
			type = TableType.TABLE;
		}
		else if(tableTypeName != null
			&& tableTypeName.equalsIgnoreCase(TableType.VIEW.name()))
		{
			type = TableType.VIEW;
		}
		else if(tableTypeName != null
			&& tableTypeName.equalsIgnoreCase(TableType.SYNONYM.name()))
		{
			type = TableType.SYNONYM;
		}
		else
		{
			type = TableType.OTHER;
		}
		return type;
	}
	
	/**
	 * Checks the <b>paramTypes</b> on position <b> i </b> to add one of the following ParamTypes to the list
	 * <b>params</b>
	 * <ol>
	 *     <li>{@link ParamType#OUT}    </li>
	 *     <li>{@link ParamType#IN_OUT} </li>
	 *     <li>{@link ParamType#IN}     </li>
	 * </ol>
	 */
	private static void addParamAccordingToParamType(
		final List<String> paramTypes,
		final List<Integer> paramIds,
		final List<Param> params,
		final int i,
		final String paramName)
	{
		final DataType dataType = DataType.get(paramIds.get(i));
		
		if(paramTypes.get(i).startsWith("out ") //$NON-NLS-1$
			|| paramTypes.get(i).startsWith("OUT ")) //$NON-NLS-1$
		{
			params.add(new Param(ParamType.OUT, paramName, dataType));
		}
		else if(paramTypes.get(i).startsWith("inout ") //$NON-NLS-1$
			|| paramTypes.get(i).startsWith("INOUT ")) //$NON-NLS-1$
		{
			params.add(new Param(ParamType.IN_OUT, paramName, dataType));
		}
		else
		{
			params.add(new Param(ParamType.IN, paramName, dataType));
		}
	}
	
	/**
	 * Checks if remarks.equals(t)
	 */
	private static boolean isToCreateProcedure(final String remarks)
	{
		return remarks.equals("t");
	}
	
	@Override
	public TableInfo[] getTableInfos(final ProgressMonitor monitor, final EnumSet<TableType> types)
		throws DBException
	{
		monitor.beginTask("", ProgressMonitor.UNKNOWN); //$NON-NLS-1$
		
		final List<TableInfo> list = new ArrayList<>();
		
		try
		{
			try(final JDBCConnection jdbcConnection = (JDBCConnection)this.dataSource.openConnection())
			{
				final Connection connection = jdbcConnection.getConnection();
				final DatabaseMetaData meta = connection.getMetaData();
				final String catalog = this.getCatalog(this.dataSource);
				final String schema = this.getSchema(this.dataSource);
				final String[] castTypes = this.castEnumSetToStringArray(types);
				final ResultSet rs = meta.getTables(catalog, schema, null, castTypes);
				
				while(rs.next() && !monitor.isCanceled())
				{
					final String tableTypeName = rs.getString("TABLE_TYPE"); //$NON-NLS-1$
					final TableType type = identifyCorrectTableType(tableTypeName);
					
					if(types.contains(type))
					{
						// no schema (== null)
						list.add(new TableInfo(type, null, rs.getString("TABLE_NAME"))); //$NON-NLS-1$
					}
				}
				rs.close();
			}
		}
		catch(final SQLException e)
		{
			throw new DBException(this.dataSource, e);
		}
		
		monitor.done();
		
		final TableInfo[] tables = list.toArray(new TableInfo[list.size()]);
		Arrays.sort(tables);
		return tables;
	}
	
	@Override
	protected TableMetaData getTableMetaData(
		final JDBCConnection jdbcConnection, final DatabaseMetaData meta,
		final int flags, final TableInfo table) throws DBException, SQLException
	{
		final String catalog = this.getCatalog(this.dataSource);
		final String schema = this.getSchema(this.dataSource);
		
		final String tableName = table.getName();
		final Table tableIdentity = new Table(tableName, "META_DUMMY"); //$NON-NLS-1$
		
		final Map<String, Object> defaultValues = new HashMap<>();
		final Map<String, Integer> columnSizes = new HashMap<>();
		
		ResultSet rs = meta.getColumns(catalog, schema, tableName, null);
		
		while(rs.next())
		{
			final String columnName = rs.getString("COLUMN_NAME"); //$NON-NLS-1$
			defaultValues.put(columnName, rs.getObject("COLUMN_DEF")); //$NON-NLS-1$
			columnSizes.put(columnName, rs.getInt("COLUMN_SIZE")); //$NON-NLS-1$
		}
		rs.close();
		
		final SELECT select = new SELECT().FROM(tableIdentity).WHERE("1 = 0"); //$NON-NLS-1$
		Result result = jdbcConnection.query(select);
		final int cc = result.getColumnCount();
		final ColumnMetaData[] columns = new ColumnMetaData[cc];
		
		for(int i = 0; i < cc; i++)
		{
			this.fillMetaDataColumns(tableName, defaultValues, columnSizes, result, columns, i);
		}
		result.close();
		
		final Map<IndexInfo, Set<String>> indexMap = new HashMap<>();
		int count = UNKNOWN_ROW_COUNT;
		
		if(table.getType() == TableType.TABLE)
		{
			if((flags & INDICES) != 0)
			{
				final Set<String> primaryKeyColumns = new HashSet<>();
				
				try
				{
					rs = meta.getPrimaryKeys(catalog, schema, tableName);
					while(rs.next())
					{
						primaryKeyColumns.add(rs.getString("COLUMN_NAME")); //$NON-NLS-1$
					}
					rs.close();
				}
				catch(final Exception e)
				{
					// no rights to read system table
					e.printStackTrace();
				}
				
				if(!primaryKeyColumns.isEmpty())
				{
					indexMap.put(
						new IndexInfo("PRIMARY_KEY", IndexType.PRIMARY_KEY), //$NON-NLS-1$
						primaryKeyColumns);
				}
				
				rs = meta.getIndexInfo(catalog, schema, tableName, false, true);
				while(rs.next())
				{
					final String indexName = rs.getString("INDEX_NAME"); //$NON-NLS-1$
					final String columnName = rs.getString("COLUMN_NAME"); //$NON-NLS-1$
					if(indexName != null
						&& columnName != null
						&& !primaryKeyColumns.contains(columnName))
					{
						final boolean unique = !rs.getBoolean("NON_UNIQUE"); //$NON-NLS-1$
						final IndexInfo info = new IndexInfo(indexName, unique ? IndexType.UNIQUE
							: IndexType.NORMAL);
						Set<String> columnNames = indexMap.get(info);
						if(columnNames == null)
						{
							columnNames = new HashSet<>();
							indexMap.put(info, columnNames);
						}
						columnNames.add(columnName);
					}
				}
				rs.close();
			}
			
			if((flags & ROW_COUNT) != 0)
			{
				try
				{
					result = jdbcConnection.query(new SELECT().columns(Functions.COUNT()).FROM(
						tableIdentity));
					if(result.next())
					{
						count = result.getInt(0);
					}
					result.close();
				}
				catch(final Exception e)
				{
					e.printStackTrace();
				}
			}
		}
		
		final Index[] indices = new Index[indexMap.size()];
		int i = 0;
		for(final IndexInfo indexInfo : indexMap.keySet())
		{
			final Set<String> columnList = indexMap.get(indexInfo);
			final String[] indexColumns = columnList.toArray(new String[columnList.size()]);
			indices[i++] = new Index(indexInfo.name, indexInfo.type, indexColumns);
		}
		
		return new TableMetaData(table, columns, indices, count);
	}
	
	private void fillMetaDataColumns(
		final String tableName,
		final Map<String, Object> defaultValues,
		final Map<String, Integer> columnSizes,
		final Result result,
		final ColumnMetaData[] columns,
		final int i)
	{
		final ColumnMetaData column = result.getMetadata(i);
		final String name = column.getName();
		
		Object defaultValue = column.getDefaultValue();
		if(defaultValue == null && defaultValues.containsKey(name))
		{
			defaultValue = defaultValues.get(name);
		}
		defaultValue = this.checkDefaultValue(defaultValue, column);
		
		int length = column.getLength();
		if(length == 0 && columnSizes.containsKey(name))
		{
			final DataType type = column.getType();
			if(type.isBlob() || type.isString())
			{
				length = columnSizes.get(name);
			}
		}
		
		columns[i] = new ColumnMetaData(
			tableName,
			name,
			column.getCaption(),
			column.getType(),
			length,
			column.getScale(),
			defaultValue,
			column.isNullable(),
			column.isAutoIncrement()
		);
	}
	
	/**
	 * @author XDEV Software (MP)
	 * @since 4.0
	 */
	@Override
	public StoredProcedure[] getStoredProcedures(final ProgressMonitor monitor) throws DBException
	{
		monitor.beginTask("", ProgressMonitor.UNKNOWN); //$NON-NLS-1$
		
		final List<StoredProcedure> list = new ArrayList<>();
		
		try
		{
			final ConnectionProvider<?> connectionProvider = this.dataSource.getConnectionProvider();
			
			try(final Connection connection = connectionProvider.getConnection())
			{
				connection.setReadOnly(false);
				final DatabaseMetaData meta = connection.getMetaData();
				
				final String catalog = this.getCatalog(this.dataSource);
				final String schema = this.getSchema(this.dataSource);
				
				final ResultSet rs = this.getProcedures(
					(IfxConnection)meta.getConnection(),
					catalog
				);
				final String proc_cat = null;
				String proc_schema = null;
				String proc_name = null;
				String procId = null;
				String remarks = null;
				String param_types = null;
				String param_ids = null;
				String ret_types = null;
				String ret_ids = null;
				List<String> paramTypes = null;
				List<Integer> paramIds = null;
				List<Param> params = null;
				ReturnTypeFlavor returnTypeFlavor = null;
				DataType returnType = null;
				List<Integer> retIds = null;
				
				int i = 0;
				while(rs.next() && !monitor.isCanceled())
				{
					if(procId == null || !procId.equals(rs.getString(4)))
					{
						if(proc_name != null)
						{
							final String proc_description = this.getProcedureDescription(
								ret_types,
								param_types,
								isToCreateProcedure(remarks),
								proc_cat,
								proc_schema
							);
							
							list.add(new StoredProcedure(
								returnTypeFlavor,
								returnType,
								proc_name,
								proc_description,
								params.toArray(new Param[params.size()])
							));
						}
						proc_schema = rs.getString("owner").trim(); //$NON-NLS-1$
						proc_name = rs.getString("procname"); //$NON-NLS-1$
						procId = rs.getString("procid"); //$NON-NLS-1$
						remarks = rs.getString("isproc"); //$NON-NLS-1$
						param_types = rs.getString("ifx_param_types"); //$NON-NLS-1$
						param_ids = rs.getString("ifx_param_ids"); //$NON-NLS-1$
						ret_types = rs.getString("ifx_ret_types"); //$NON-NLS-1$
						ret_ids = rs.getString("ifx_ret_ids"); //$NON-NLS-1$
						paramTypes = Arrays.asList(param_types.split(",")); //$NON-NLS-1$
						paramIds = this.parseIfxIds(param_ids);
						params = new ArrayList<>();
						retIds = this.parseIfxIds(ret_ids);
						i = 0;
						
						switch(retIds.size())
						{
							case 0:
								returnTypeFlavor = ReturnTypeFlavor.VOID;
								break;
							case 1:
								returnTypeFlavor = ReturnTypeFlavor.TYPE;
								
								returnType = DataType.get(retIds.get(0));
								break;
							
							default:
								returnTypeFlavor = ReturnTypeFlavor.RESULT_SET;
								break;
						}
					}
					
					String paramName = rs.getString("paramname"); //$NON-NLS-1$
					if(paramName != null)
					{
						paramName = paramName.toUpperCase();
					}
					
					if(paramIds != null
						&& !paramIds.isEmpty()
						&& paramName != null)
					{
						addParamAccordingToParamType(paramTypes, paramIds, params, i, paramName);
						i++;
					}
				}
				if(ret_ids != null)
				{
					final String proc_description = this.getProcedureDescription(
						ret_types,
						param_types,
						isToCreateProcedure(remarks),
						proc_cat,
						proc_schema
					);
					list.add(new StoredProcedure(returnTypeFlavor, returnType, proc_name,
						proc_description, params.toArray(new Param[params.size()])));
				}
			}
		}
		catch(final SQLException e)
		{
			LOGGER.error("Cannot get stored procedures.", e); //$NON-NLS-1$
			throw new DBException(this.dataSource, e);
		}
		
		monitor.done();
		
		return list.toArray(new StoredProcedure[list.size()]);
	}
	
	/**
	 * Parse a string of Informix- Datatypes to a list of JDBC-Types represented by a integer value
	 *
	 * @return a list of Integers, JDBC-Types
	 */
	private List<Integer> parseIfxIds(final String paramString)
	{
		final boolean bool = IfxStatement.t;
		final StringTokenizer localStringTokenizer = new StringTokenizer(paramString, "(,)"); //$NON-NLS-1$
		
		int i2 = 1;
		final List<Integer> list = new ArrayList<>();
		do
		{
			do
			{
				if(!(localStringTokenizer.hasMoreTokens()))
				{
					return list;
				}
				
				final String token = localStringTokenizer.nextToken();
				
				if(i2 != 1)
				{
					break;
				}
				
				final int tokenValueAsInt = Integer.parseInt(token.trim());
				int jdbc2TypeAsInt = IfxTypes.FromIfxToJDBC2Type(tokenValueAsInt);
				
				if(tokenValueAsInt == 4)
				{
					jdbc2TypeAsInt = 6;
				}
				list.add(jdbc2TypeAsInt);
				i2 = 0;
			}
			while(!(bool));
			
			i2 = 1;
		}
		while(!(bool));
		
		return list;
	}
	
	private String getProcedureDescription(
		final String retTypes,
		final String paramTypes,
		final boolean createProcedure,
		final String procCat,
		final String procSchema)
	{
		
		String procDescription;
		if(createProcedure) //$NON-NLS-1$
		{
			procDescription = "create procedure "; //$NON-NLS-1$
		}
		else
		{
			procDescription = "create function "; //$NON-NLS-1$
		}
		
		procDescription = procDescription + procCat + "." + procSchema + "("; //$NON-NLS-1$ //$NON-NLS-2$
		
		if(!retTypes.equals("")) //$NON-NLS-1$
		{
			if(!paramTypes.equals("")) //$NON-NLS-1$
			{
				
				procDescription = procDescription + paramTypes + ") returning " + retTypes //$NON-NLS-1$
					+ ";"; //$NON-NLS-1$
			}
			else
			{
				procDescription = procDescription + ") returning " + retTypes + ";"; //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
		
		return procDescription;
	}
	
	private ResultSet getProcedures(final IfxConnection connection, final String catalog) throws SQLException
	{
		
		String sql = ""; //$NON-NLS-1$
		try(final IfxStatement statement = (IfxStatement)connection.createStatement())
		{
			statement.setAutoFree(true);
			
			sql = "select '" //$NON-NLS-1$
				+ catalog
				+ "', " //$NON-NLS-1$
				+ "SP.owner, SP.procname, SP.procid, SP.isproc, ifx_param_types(SP.procid) AS ifx_param_types,"
				+ "ifx_param_ids(SP.procid) AS ifx_param_ids,ifx_ret_types(SP.procid) AS ifx_ret_types, ifx_ret_ids(SP"
				+ ".procid) AS ifx_ret_ids, SPC.paramid, SPC.paramname from "
				//$NON-NLS-1$
				+ "informix.sysprocedures SP INNER JOIN  informix.sysproccolumns SPC ON SPC.procid = SP.procid where "
				+ "not mode='d' and not mode='r' and SP.procid > 452"; //$NON-NLS-1$
			
			statement.executeQuery(sql, false);
			
			return statement.getResultSet();
		}
		catch(final SQLException e)
		{
			final String err = "Cannot get stored procedures. Sql statement: " + sql; //$NON-NLS-1$
			LOGGER.error(err, Arrays.toString(e.getStackTrace()));
			throw new SQLException(err, e);
		}
	}
	
	@Override
	protected void createTable(final JDBCConnection jdbcConnection, final TableMetaData table)
		throws DBException, SQLException
	{
	}
	
	@Override
	protected void addColumn(
		final JDBCConnection jdbcConnection, final TableMetaData table,
		final ColumnMetaData column, final ColumnMetaData columnBefore, final ColumnMetaData columnAfter)
		throws DBException, SQLException
	{
	}
	
	@Override
	protected void alterColumn(
		final JDBCConnection jdbcConnection, final TableMetaData table,
		final ColumnMetaData column, final ColumnMetaData existing) throws DBException, SQLException
	{
	}
	
	@Override
	public boolean equalsType(final ColumnMetaData clientColumn, final ColumnMetaData dbColumn)
	{
		return false;
	}
	
	@Override
	protected void dropColumn(
		final JDBCConnection jdbcConnection, final TableMetaData table,
		final ColumnMetaData column) throws DBException, SQLException
	{
	}
	
	@Override
	protected void createIndex(final JDBCConnection jdbcConnection, final TableMetaData table, final Index index)
		throws DBException, SQLException
	{
	}
	
	@Override
	protected void dropIndex(final JDBCConnection jdbcConnection, final TableMetaData table, final Index index)
		throws DBException, SQLException
	{
	}
	
	/**
	 * @author XDEV Software (MP)
	 * @since 4.0
	 */
	@Override
	public TableMetaData[] getTableMetaData(final ProgressMonitor monitor, final int flags, final TableInfo... tables)
		throws DBException
	{
		TableMetaData[] tableMetaDatas = null;
		final TableMetaData[] result = new TableMetaData[tables.length];
		
		try
		{
			tableMetaDatas = this.getTableMetaData(monitor, TableType.TABLES_VIEWS_AND_SYNONYMS, flags,
				false);
			
			int i = 0;
			for(final TableMetaData tableMetaData : tableMetaDatas)
			{
				
				for(final TableInfo table : tables)
				{
					
					if(tableMetaData.getTableInfo().getName().equalsIgnoreCase(table.getName()))
					{
						
						result[i++] = tableMetaData;
						break;
					}
				}
			}
		}
		catch(final Exception e)
		{
			final String err = "Request to get TableMetaDatas failed!."; //$NON-NLS-1$
			LOGGER.error(err);
			throw new DBException(this.dataSource, err, e);
		}
		
		return result;
	}
	
	private TableMetaData[] getTableMetaData(
		final ProgressMonitor dummy, final EnumSet<TableType> types,
		final int flags, final boolean filterSysTables) throws Exception
	{
		final JDBCConnection<?, ?> jdbcConnection = (JDBCConnection<?, ?>)this.dataSource.openConnection();
		final Connection connection = jdbcConnection.getConnection();
		
		final DatabaseMetaData meta = connection.getMetaData();
		final String catalog = this.getCatalog(this.dataSource);
		final String schema = this.getSchema(this.dataSource);
		
		this.isGetSynonym = types.contains(TableType.SYNONYM);
		
		final String[] castTypes = this.castEnumSetToStringArray(types);
		
		final Map<String, List<ColumnMetaData>> columnsMap = new HashMap<>();
		final Map<String, TableInfo> tableInfoMap = new HashMap<>();
		final Set<String> columnSet = new HashSet<>();
		
		IfxStatement statement = null;
		TableMetaData[] result = null;
		try
		{
			statement = (IfxStatement)connection.createStatement();
			
			this.lodSynonyms(statement);
			
			this.requestStatementForTableMetaDatas(castTypes, statement, filterSysTables);
			final ResultSet rs = statement.getResultSet();
			this.parseResultToMaps(schema, rs, columnsMap, tableInfoMap, columnSet);
			
			final Map<String, List<Index>> indicesMap = new HashMap<>();
			
			this.calculateIndices(flags, meta, catalog, schema, columnSet, indicesMap);
			
			final Map<String, Integer> countsMap = new HashMap<>();
			
			this.calculateRowCounts(flags, statement, tableInfoMap, countsMap);
			
			result = this.convToTableMetaData(indicesMap, tableInfoMap, columnsMap, countsMap);
		}
		catch(final Exception e)
		{
			final String err = "Cannot return TableMetaData"; //$NON-NLS-1$
			throw new DBException(this.dataSource, err, e);
		}
		finally
		{
			if(statement != null)
			{
				statement.close();
			}
		}
		
		return result;
	}
	
	private void lodSynonyms(final IfxStatement statement) throws SQLException
	{
		final ResultSet rs = statement
			.executeQuery("SELECT a.tabname , b.btabid FROM systables a, syssyntable b WHERE a.tabid = b.tabid; ");
		//$NON-NLS-1$
		while(rs.next())
		{
			this.synMap = new HashMap<>();
			final String tabname = rs.getString("tabname"); //$NON-NLS-1$
			final String btabid = rs.getString("btabid"); //$NON-NLS-1$
			this.synMap.put(btabid, tabname);
		}
	}
	
	private void calculateRowCounts(
		final int flags, final IfxStatement statement,
		final Map<String, TableInfo> tableInfoMap, final Map<String, Integer> countsMap) throws Exception
	{
		if((flags & ROW_COUNT) != 0)
		{
			int rowCount = -1;
			String lastTableName = null;
			try
			{
				for(final Entry<String, TableInfo> entrySet : tableInfoMap.entrySet())
				{
					
					lastTableName = entrySet.getValue().getName();
					final String query = "select  count(*) from " + lastTableName; //$NON-NLS-1$
					statement.execute(query);
					
					final ResultSet resultSet = statement.getResultSet();
					if(resultSet.next())
					{
						rowCount = resultSet.getInt(1);
						countsMap.put(lastTableName, rowCount);
					}
					resultSet.close();
				}
			}
			catch(final Exception e)
			{
				final String err =
					"Cannot calculate row count. Row count for " + lastTableName + " is " //$NON-NLS-1$ //$NON-NLS-2$
						+ rowCount;
				throw new Exception(err, e);
			}
		}
	}
	
	private void calculateIndices(
		final int flags, final DatabaseMetaData meta, final String catalog, final String schema,
		final Set<String> columnSet, final Map<String, List<Index>> indicesMap) throws Exception
	{
		if((flags & INDICES) != 0)
		{
			final Set<String> primaryKeyColumns = new HashSet<>();
			
			try
			{
				this.getPrimaryKeys(meta, catalog, schema, columnSet, indicesMap);
				this.getIndexInfos(meta, catalog, schema, columnSet, indicesMap, primaryKeyColumns);
			}
			catch(final Exception e)
			{
				throw e;
			}
		}
	}
	
	private void getIndexInfos(
		final DatabaseMetaData meta, final String catalog, final String schema,
		final Set<String> columnSet, final Map<String, List<Index>> indicesMap,
		final Set<String> primaryKeyColumns) throws SQLException
	{
		final ResultSet rs;
		rs = meta.getIndexInfo(catalog, schema, null, false, true);
		while(rs.next())
		{
			final String indexName = rs.getString("INDEX_NAME"); //$NON-NLS-1$
			final String columnName = rs.getString("COLUMN_NAME"); //$NON-NLS-1$
			final String tableName = rs.getString("TABLE_NAME"); //$NON-NLS-1$
			if(columnSet.contains(columnName) && indexName != null && columnName != null
				&& !primaryKeyColumns.contains(columnName))
			{
				final boolean unique = !rs.getBoolean("NON_UNIQUE"); //$NON-NLS-1$
				final IndexInfo info = new IndexInfo(indexName, unique ? IndexType.UNIQUE
					: IndexType.NORMAL);
				
				if(indicesMap.containsKey(tableName))
				{
					final List<Index> list = indicesMap.get(tableName);
					
					for(final Index index : list)
					{
						if(index.getName().equalsIgnoreCase(info.name))
						{
							final String[] columns = index.getColumns();
							final String[] newColumns = new String[columns.length + 1];
							
							int i = 0;
							for(final String string : columns)
							{
								newColumns[i++] = string;
							}
							
							newColumns[i] = columnName;
							
							index.setColumns(newColumns);
						}
					}
				}
				else
				{
					final List<Index> iList = new ArrayList<>();
					iList.add(new Index(info.name, info.type, columnName));
					indicesMap.put(tableName, iList);
				}
			}
		}
		rs.close();
	}
	
	private void getPrimaryKeys(
		final DatabaseMetaData meta, final String catalog, final String schema,
		final Set<String> columnSet, final Map<String, List<Index>> indicesMap) throws SQLException
	{
		final ResultSet rs;
		rs = meta.getPrimaryKeys(catalog, schema, null);
		// add primary keys
		while(rs.next())
		{
			final String columnName = rs.getString("COLUMN_NAME"); //$NON-NLS-1$
			final String tableName = rs.getString("TABLE_NAME"); //$NON-NLS-1$
			
			if(columnSet.contains(columnName))
			{
				if(indicesMap.containsKey(tableName))
				{
					indicesMap.get(tableName).add(
						new Index("PRIMARY_KEY", IndexType.PRIMARY_KEY, columnName)); //$NON-NLS-1$
					final List<Index> list = indicesMap.get(tableName);
					
					for(final Index index : list)
					{
						final String[] columns = index.getColumns();
						final String[] newColumns = new String[columns.length + 1];
						
						int i = 0;
						for(final String string : columns)
						{
							newColumns[i++] = string;
						}
						
						newColumns[i] = columnName;
						
						index.setColumns(newColumns);
					}
				}
				else
				{
					final List<Index> iList = new ArrayList<>();
					iList.add(new Index("PRIMARY_KEY", IndexType.PRIMARY_KEY, columnName)); //$NON-NLS-1$
					indicesMap.put(tableName, iList);
				}
			}
		}
		rs.close();
	}
	
	private TableMetaData[] convToTableMetaData(
		final Map<String, List<Index>> indicesMap,
		final Map<String, TableInfo> tableInfoMap, final Map<String, List<ColumnMetaData>> columnsMap,
		final Map<String, Integer> countsMap)
	{
		TableMetaData[] tableMetaDatas = null;
		if(this.isGetSynonym && this.synMap != null)
		{
			tableMetaDatas = new TableMetaData[tableInfoMap.size() + this.synMap.size()];
		}
		else
		{
			tableMetaDatas = new TableMetaData[tableInfoMap.size()];
		}
		
		int i = 0;
		for(final Entry<String, TableInfo> entry : tableInfoMap.entrySet())
		{
			final String name = entry.getValue().getName();
			final List<ColumnMetaData> columnsList = columnsMap.get(name);
			final ColumnMetaData[] columns = columnsList.toArray(new ColumnMetaData[columnsList.size()]);
			
			final List<Index> indexList = indicesMap.get(name);
			Index[] indices = new Index[0];
			if(indexList != null)
			{
				indices = indexList.toArray(new Index[indexList.size()]);
			}
			
			int count = UNKNOWN_ROW_COUNT;
			if(countsMap.containsKey(name))
			{
				count = countsMap.get(name);
			}
			
			if(this.isGetSynonym && this.synMap != null && this.synMap.containsKey(entry.getKey()))
			{
				// add original table
				final TableInfo tableInfo = tableInfoMap.get(entry.getKey());
				tableMetaDatas[i++] = new TableMetaData(tableInfo, columns, indices, count);
				
				// add synonym table
				final TableInfo newTableInfo = new TableInfo(tableInfo.getType(), tableInfo.getSchema(),
					this.synMap.get(entry.getKey()));
				final TableMetaData newTableMetaData = new TableMetaData(newTableInfo, columns, indices,
					count);
				tableMetaDatas[i++] = newTableMetaData;
			}
			else
			{
				final TableMetaData tableMetaData = new TableMetaData(tableInfoMap.get(entry.getKey()),
					columns, indices, count);
				tableMetaDatas[i++] = tableMetaData;
			}
		}
		
		return tableMetaDatas;
	}
	
	private void requestStatementForTableMetaDatas(
		final String[] castTypes, final IfxStatement statement,
		final boolean filterSysTables) throws SQLException
	{
		
		final StringBuffer sql = new StringBuffer(
			"select T.tabname, T.tabtype, TC.colname, TC.collength, TC.coltype, TC.extended_id ,sdf.default, sdf.type,"
				+ " TC.colmin, TC.colmax, T.tabid from informix.systables T LEFT JOIN informix.syscolumns TC ON TC"
				+ ".tabid = T.tabid LEFT JOIN informix.sysdefaults sdf ON (TC.tabid = sdf.tabid AND TC.colno = sdf"
				+ ".colno)"); //$NON-NLS-1$
		if(filterSysTables)
		{
			sql.append(" WHERE T.tabid > 99"); //$NON-NLS-1$
		}
		
		if(castTypes != null && castTypes.length > 0)
		{
			if(filterSysTables)
			{
				sql.append(" AND ("); //$NON-NLS-1$
			}
			else
			{
				sql.append(" WHERE ("); //$NON-NLS-1$
			}
			
			int i = 0;
			for(final String type : castTypes)
			{
				
				if(!type.equals("SYNONYM")) //$NON-NLS-1$
				{
					if(i++ > 0)
					{
						sql.append(" OR "); //$NON-NLS-1$
					}
					sql.append("T.tabtype='" + type.toCharArray()[0] + "'"); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
			sql.append(")"); //$NON-NLS-1$
		}
		sql.append(" AND (sdf.colno is null OR sdf.colno=TC.colno)"); //$NON-NLS-1$
		
		statement.executeQuery(sql.toString(), false);
	}
	
	private void parseResultToMaps(
		final String schema, final ResultSet rs,
		final Map<String, List<ColumnMetaData>> columnsMap, final Map<String, TableInfo> tableInfoMap,
		final Set<String> columnSet) throws Exception
	{
		TableType tableType;
		TableInfo tableInfo;
		String tableName = ""; //$NON-NLS-1$
		
		while(rs.next())
		{
			
			if(!tableName.equalsIgnoreCase(rs.getString("tabname"))) //$NON-NLS-1$
			{
				
				tableName = rs.getString("tabname"); //$NON-NLS-1$
				final String typePrefix = rs.getString("tabtype"); //$NON-NLS-1$
				final String tabid = rs.getString("tabid"); //$NON-NLS-1$
				tableType = this.getTableType(typePrefix);
				tableInfo = new TableInfo(tableType, schema, tableName);
				tableInfoMap.put(tabid, tableInfo);
			}
			
			final String columnName = rs.getString("colname"); //$NON-NLS-1$
			columnSet.add(columnName);
			final int colType = rs.getInt("coltype"); //$NON-NLS-1$
			int collength = rs.getShort("collength"); //$NON-NLS-1$
			final short transformedColType = (short)(colType & 0xFF);
			
			final String colDefault = this.getColumnDefaultValue(rs);
			
			final int columnType = (short)IfxTypes.FromIfxToJDBCType(transformedColType);
			final DataType columnDataType = DataType.get(columnType);
			
			final boolean isAutoIncrement = this.isAutoIncrement(transformedColType, columnDataType);
			
			final boolean nullable = this.isNullAllowed(colType);
			
			int scale = 0;
			
			if(columnDataType == DataType.DECIMAL)
			{
				if((collength % 256) == 255)
				{
					/* Floating point. */
					scale = collength % 256;
				}
				else
				{
					/* Fixed point. */
					scale = collength % 256;
					collength = collength / 256;
				}
			}
			else if(columnDataType == DataType.TIMESTAMP)
			{
				final int firstQualifier = IfxDateTime.getStartCode((short)collength);
				final int lastQualifier = IfxDateTime.getEndCode((short)collength);
				
				collength = (collength - (firstQualifier * 16) - lastQualifier) / 256;
			}
			
			this.addToColumnsMap(columnsMap, tableName, columnName, collength, colDefault, columnDataType,
				isAutoIncrement, nullable, scale);
		}
	}
	
	private void addToColumnsMap(
		final Map<String, List<ColumnMetaData>> columnsMap, final String tableName,
		final String columnName, final int collength, final String colDefault, final DataType columnDataType,
		final boolean isAutoIncrement, final boolean nullable, final int scale)
	{
		final List<ColumnMetaData> columns;
		if(columnsMap.containsKey(tableName))
		{
			// caption is set to columnName
			columnsMap.get(tableName).add(
				new ColumnMetaData(tableName, columnName, columnName, columnDataType, collength,
					scale, colDefault, nullable, isAutoIncrement));
		}
		else
		{
			columns = new ArrayList<>();
			columns.add(new ColumnMetaData(tableName, columnName, columnName, columnDataType,
				collength, scale, colDefault, nullable, isAutoIncrement));
			columnsMap.put(tableName, columns);
		}
	}
	
	private boolean isAutoIncrement(final short transformedColType, final DataType columnDataType)
	{
		boolean isAutoIncrement = false;
		if(columnDataType == DataType.INTEGER)
		{
			
			isAutoIncrement = transformedColType == IfxTypes.IFX_TYPE_SERIAL
				|| transformedColType == IfxTypes.IFX_TYPE_SERIAL8;
		}
		return isAutoIncrement;
	}
	
	private String getColumnDefaultValue(final ResultSet rs) throws Exception
	{
		String colDefault = null;
		String defaultType = null;
		try
		{
			defaultType = rs.getString("type"); //$NON-NLS-1$
			colDefault = rs.getString("default"); //$NON-NLS-1$
		}
		catch(final SQLException e)
		{
			final String err = "ResultSet cannot return value. defaultType=" + defaultType //$NON-NLS-1$
				+ " and colDefault=" + colDefault; //$NON-NLS-1$
			LOGGER.error(err, e);
			throw new Exception(err, e);
		}
		
		if(colDefault != null)
		{
			if(defaultType.equalsIgnoreCase("n")) //$NON-NLS-1$
			{
				colDefault = null;
			}
			else if(defaultType.equalsIgnoreCase("l")) //$NON-NLS-1$
			{
				
				final String[] split = colDefault.split(" "); //$NON-NLS-1$
				if(split.length == 1)
				{
					colDefault = split[0];
				}
				else
				{
					colDefault = split[1];
				}
			}
		}
		return colDefault;
	}
	
	private boolean isNullAllowed(final int colType)
	{
		return (colType & 0x100) != 256;
	}
	
	private TableType getTableType(final String typePrefix)
	{
		
		if(typePrefix.equalsIgnoreCase("T")) //$NON-NLS-1$
		{
			return TableType.TABLE;
		}
		else if(typePrefix.equalsIgnoreCase("V")) //$NON-NLS-1$
		{
			return TableType.VIEW;
		}
		else if(typePrefix.equalsIgnoreCase("S")) //$NON-NLS-1$
		{
			return TableType.SYNONYM;
		}
		
		return TableType.OTHER;
	}
	
	@Override
	public EntityRelationshipModel getEntityRelationshipModel(
		final ProgressMonitor monitor,
		final TableInfo... tableInfos) throws DBException
	{
		monitor.beginTask("", tableInfos.length); //$NON-NLS-1$
		
		final EntityRelationshipModel model = new EntityRelationshipModel();
		
		try
		{
			final List<String> tables = new ArrayList<>();
			for(final TableInfo table : tableInfos)
			{
				if(table.getType() == TableType.TABLE)
				{
					tables.add(table.getName());
				}
			}
			Collections.sort(tables);
			
			final ConnectionProvider<?> connectionProvider = this.dataSource.getConnectionProvider();
			final Connection connection = connectionProvider.getConnection();
			
			ResultSet exportedKeys = null;
			
			try
			{
				final DatabaseMetaData meta = connection.getMetaData();
				final String catalog = this.getCatalog(this.dataSource);
				final String schema = this.getSchema(this.dataSource);
				int done = 0;
				
				exportedKeys = meta.getExportedKeys(catalog, schema, "%"); //$NON-NLS-1$
				
				String pkTable = null;
				String fkTable = null;
				final List<String> pkColumns = new ArrayList<>();
				final List<String> fkColumns = new ArrayList<>();
				while(exportedKeys.next())
				{
					
					if(monitor.isCanceled())
					{
						continue;
					}
					
					pkTable = exportedKeys.getString("PKTABLE_NAME"); //$NON-NLS-1$
					fkTable = exportedKeys.getString("FKTABLE_NAME"); //$NON-NLS-1$
					monitor.setTaskName(pkTable);
					
					final short keySeq = exportedKeys.getShort("KEY_SEQ"); //$NON-NLS-1$
					
					if(keySeq == 1 && pkColumns.size() > 0)
					{
						if(tables.contains(pkTable) && tables.contains(fkTable))
						{
							model.add(new EntityRelationship(
								new Entity(pkTable, pkColumns
									.toArray(new String[pkColumns.size()]), Cardinality.ONE),
								new Entity(fkTable, fkColumns.toArray(new String[fkColumns
									.size()]), Cardinality.MANY)));
							pkColumns.clear();
							fkColumns.clear();
						}
					}
					
					pkColumns.add(exportedKeys.getString("PKCOLUMN_NAME")); //$NON-NLS-1$
					fkColumns.add(exportedKeys.getString("FKCOLUMN_NAME")); //$NON-NLS-1$
				}
				if(pkColumns.size() > 0)
				{
					if(tables.contains(pkTable) && tables.contains(fkTable))
					{
						model.add(new EntityRelationship(new Entity(pkTable, pkColumns
							.toArray(new String[pkColumns.size()]), Cardinality.ONE), new Entity(
							fkTable, fkColumns.toArray(new String[fkColumns.size()]),
							Cardinality.MANY)));
						pkColumns.clear();
						fkColumns.clear();
					}
				}
				
				monitor.worked(++done);
			}
			finally
			{
				if(exportedKeys != null)
				{
					exportedKeys.close();
				}
				connection.close();
			}
		}
		catch(final SQLException e)
		{
			throw new DBException(this.dataSource, e);
		}
		
		monitor.done();
		
		return model;
	}
}
