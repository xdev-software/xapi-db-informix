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
import java.sql.Statement;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import xdev.db.DBException;
import xdev.db.jdbc.JDBCConnection;


public class InformixJDBCConnection extends JDBCConnection<InformixJDBCDataSource, InformixDbms>
{
	public InformixJDBCConnection(final InformixJDBCDataSource dataSource)
	{
		super(dataSource);
	}
	
	@Override
	public void createTable(
		final String tableName, final String primaryKey, final Map<String, String> columnMap,
		final boolean isAutoIncrement, final Map<String, String> foreignKeys) throws Exception
	{
		
		if(!columnMap.containsKey(primaryKey))
		{
			columnMap.put(primaryKey, "INTEGER"); //$NON-NLS-1$
		}
		StringBuffer createStatement = null;
		
		if(isAutoIncrement)
		{
			createStatement =
				new StringBuffer("CREATE TABLE IF NOT EXISTS " + tableName + "(" //$NON-NLS-1$ //$NON-NLS-2$
					+ primaryKey + " SERIAL NOT NULL,"); //$NON-NLS-1$
		}
		else
		{
			createStatement =
				new StringBuffer("CREATE TABLE IF NOT EXISTS " + tableName + "(" //$NON-NLS-1$ //$NON-NLS-2$
					+ primaryKey + " " + columnMap.get(primaryKey) + ","); //$NON-NLS-1$ //$NON-NLS-2$
		}
		
		for(final String keySet : columnMap.keySet())
		{
			if(!keySet.equals(primaryKey))
			{
				if(columnMap.get(keySet).contains("TIMESTAMP"))
				{ //$NON-NLS-1$
					createStatement
						.append(keySet
							+ " " + columnMap.get(keySet).replace("TIMESTAMP", "DATETIME YEAR TO FRACTION")
							+ ","); //$NON-NLS-1$ //$NON-NLS-2$
				}
				else
				{
					createStatement.append(keySet + " " + columnMap.get(keySet) + ","); //$NON-NLS-1$ //$NON-NLS-2$
				}
			}
		}
		
		createStatement.append(" PRIMARY KEY (" + primaryKey + "))"); //$NON-NLS-1$ //$NON-NLS-2$
		
		if(log.isDebugEnabled())
		{
			log.debug("SQL Statement to create a table: " + createStatement); //$NON-NLS-1$
		}
		
		try(final Connection connection = super.getConnection();
			final Statement statement = connection.createStatement())
		{
			statement.execute(createStatement.toString());
		}
		catch(final Exception e)
		{
			throw e;
		}
	}
	
	@Override
	public Date getServerTime() throws DBException, ParseException
	{
		return super.getServerTime("SELECT current FROM Systables WHERE Tabid = 1"); //$NON-NLS-1$
	}
}
