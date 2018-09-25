package org.restsql.core.impl.mssql;

import org.restsql.core.impl.AbstractSqlBuilder;

public class MsSqlSqlBuilder extends AbstractSqlBuilder {
		
	@Override
	protected String buildSelectLimitSql(int limit, int offset) {
		return "";
	}


	
}
