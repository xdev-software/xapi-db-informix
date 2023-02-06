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



import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


public class InformixDbms
		extends
		DbmsAdaptor.Implementation<InformixDbms, InformixDMLAssembler, InformixDDLMapper, InformixRetrospectionAccessor, InformixSyntax>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////
	
	/** The Constant MAX_VARCHAR_LENGTH. */
	protected static final int			MAX_VARCHAR_LENGTH		= Integer.MAX_VALUE;
	
	protected static final char			IDENTIFIER_DELIMITER	= '"';
	
	public static final InformixSyntax	SYNTAX					= new InformixSyntax();
	
	private InformixJDBCDataSource		dataSource;
	
	
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	public InformixDbms()
	{
		this(new SQLExceptionParser.Body());
	}
	
	
	/**
	 * @param sqlExceptionParser
	 *            the sql exception parser
	 */
	public InformixDbms(final SQLExceptionParser sqlExceptionParser)
	{
		super(sqlExceptionParser,false);
		this.setRetrospectionAccessor(new InformixRetrospectionAccessor(this));
		this.setDMLAssembler(new InformixDMLAssembler(this));
		this.setSyntax(SYNTAX);
	}
	
	
	public void setDataSource(InformixJDBCDataSource dataSource)
	{
		this.dataSource = dataSource;
	}
	
	
	/**
	 * @see DbmsAdaptor#createConnectionInformation(String, int, String, String, String, String)
	 */
	@Override
	public InformixConnectionInformation createConnectionInformation(final String host,
			final int port, final String user, final String password, final String catalog, final String properties)
	{
		return new InformixConnectionInformation(host,port,user,password,catalog,
				dataSource.getInformixServer(),properties, this);
	}
	
	
	@Override
	public Object updateSelectivity(final SqlTableIdentity table)
	{
		return null;
	}
	
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#assembleTransformBytes(byte[],
	 *      java.lang.StringBuilder)
	 */
	@Override
	public StringBuilder assembleTransformBytes(final byte[] bytes, final StringBuilder sb)
	{
		return null;
	}
	
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor.Implementation#getRetrospectionAccessor()
	 */
	@Override
	public InformixRetrospectionAccessor getRetrospectionAccessor()
	{
		throw new RuntimeException("HSQL Retrospection not implemented yet!");
	}
	
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#initialize(com.xdev.jadoth.sqlengine.internal.DatabaseGateway)
	 */
	@Override
	public void initialize(final DatabaseGateway<InformixDbms> dbc)
	{
	}
	
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#rebuildAllIndices(java.lang.String)
	 */
	@Override
	public Object rebuildAllIndices(final String fullQualifiedTableName)
	{
		return null;
	}
	
	
	@Override
	public boolean supportsOFFSET_ROWS()
	{
		return true;
	}
	
	
	/**
	 * @see com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor#getMaxVARCHARlength()
	 */
	@Override
	public int getMaxVARCHARlength()
	{
		return MAX_VARCHAR_LENGTH;
	}
	
	
	@Override
	public char getIdentifierDelimiter()
	{
		return IDENTIFIER_DELIMITER;
	}
}
