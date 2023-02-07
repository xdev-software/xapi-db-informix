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

import xdev.db.DBException;
import xdev.db.jdbc.JDBCDataSource;


public class InformixJDBCDataSource extends JDBCDataSource<InformixJDBCDataSource, InformixDbms>
{
	public final static Parameter<String> INFORMIXSERVER;
	
	static
	{
		INFORMIXSERVER = new Parameter("INFORMIXSERVER", "informixserver");
	}
	
	public InformixJDBCDataSource()
	{
		super(new InformixDbms());
		this.getDbmsAdaptor().setDataSource(this);
	}
	
	@Override
	public Parameter[] getDefaultParameters()
	{
		return new Parameter[]{
			HOST.clone(),
			PORT.clone(9088),
			USERNAME.clone("informix"),
			PASSWORD.clone(),
			CATALOG.clone(),
			INFORMIXSERVER.clone(),
			URL_EXTENSION.clone(),
			IS_SERVER_DATASOURCE.clone(),
			SERVER_URL.clone(),
			AUTH_KEY.clone()
		};
	}
	
	@Override
	protected InformixConnectionInformation getConnectionInformation()
	{
		return new InformixConnectionInformation(
			this.getHost(),
			this.getPort(),
			this.getUserName(),
			this.getPassword().getPlainText(),
			this.getCatalog(),
			this.getInformixServer(),
			this.getUrlExtension(),
			this.getDbmsAdaptor()
		);
	}
	
	public String getInformixServer()
	{
		return this.getParameterValue(INFORMIXSERVER);
	}
	
	@Override
	public InformixJDBCConnection openConnectionImpl() throws DBException
	{
		return new InformixJDBCConnection(this);
	}
	
	@Override
	public InformixJDBCMetaData getMetaData() throws DBException
	{
		return new InformixJDBCMetaData(this);
	}
	
	@Override
	public boolean canExport()
	{
		return false;
	}
}
