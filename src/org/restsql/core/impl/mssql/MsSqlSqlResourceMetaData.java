/* Copyright (c) restSQL Project Contributors. Licensed under MIT. */
package org.restsql.core.impl.mssql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.restsql.core.TableMetaData.TableRole;
import org.restsql.core.impl.AbstractSqlResourceMetaData;
import org.restsql.core.impl.ColumnMetaDataImpl;
import org.restsql.core.sqlresource.SqlResourceDefinition;
import org.restsql.core.sqlresource.SqlResourceDefinitionUtils;

/**
 * Implements SqlResourceMetaData for MsSql.
 * 
 * @author Mark Sawers
 */
public class MsSqlSqlResourceMetaData extends AbstractSqlResourceMetaData {
	private static final String SQL_COLUMNS_QUERY = "select column_name, data_type , COLUMNPROPERTY(object_id(TABLE_SCHEMA+'.'+TABLE_NAME), COLUMN_NAME, 'IsIdentity') as IsIdentity	from information_schema.columns where table_schema = ? and table_name = ?";
	private static final String SQL_PK_QUERY = "select column_name from information_schema.table_constraints tc, information_schema.key_column_usage kcu"
			+ " where tc.CONSTRAINT_CATALOG= ? and tc.table_name = ?"
			+ " and tc.constraint_type = 'PRIMARY KEY'"
			+ " and tc.constraint_schema = kcu.constraint_schema and tc.table_name = kcu.table_name"
			+ " and tc.constraint_name = kcu.constraint_name";

	/**
	 * Retrieves sql for querying columns. Hook method for buildInvisibleForeignKeys() and buildJoinTableMetadata()
	 * allows database-specific overrides.
	 */
	@Override
	protected String getSqlColumnsQuery() {
		return SQL_COLUMNS_QUERY;
	}

	/**
	 * Retrieves sql for querying primary keys. Hook method for buildPrimaryKeys allows database-specific overrides.
	 */
	@Override
	protected String getSqlPkQuery() {
		return SQL_PK_QUERY;
	}

	
	/**
	 * Building the qualified column label for disambiguating duplicate labels in SQL statements. MsSql can use the form
	 * <code>database.table.label</code>, for example <code>sakila.film.id</code>.
	 * @param readOnly true if column is read only
	 * @param label base column label
	 * 
	 * @return qualified label
	 */
	@Override
	protected String getQualifiedColumnLabel(String tableName, String qualifiedTableName, final boolean readOnly, final String label) {
		if (readOnly) {
			return label;
		} else {
			final StringBuilder qualifiedLabel = new StringBuilder(100);
			if (hasMultipleDatabases()) {
				qualifiedLabel.append(qualifiedTableName);
			} else {
				qualifiedLabel.append(tableName);
			}
			qualifiedLabel.append('.');
			qualifiedLabel.append(label);
			return qualifiedLabel.toString();
		}
	}

	/** Retrieves database-specific table name used in SQL statements. */
	@Override
	protected String getQualifiedTableName(final SqlResourceDefinition definition,
			final ResultSetMetaData resultSetMetaData, final int colNumber) throws SQLException {
		// return resultSetMetaData.getCatalogName(colNumber) + resultSetMetaData.getTableName(colNumber);
		return getColumnDatabaseName(definition, resultSetMetaData, colNumber) + "." +
				getColumnTableName (definition, resultSetMetaData, colNumber);
				
	}

	/** Retrieves database-specific table name used in SQL statements. Used to build join table meta data. */
	@Override
	protected String getQualifiedTableName(Connection connection, String databaseName, String tableName)
			throws SQLException {
		return databaseName + "." + tableName;
	}

	/**
	 * Sets sequence metadata for a column with the columns query result set. The extra column value will contain the
	 * string auto_increment if this is a sequence.
	 * 
	 * @throws SQLException when a database error occurs
	 */
	@Override
	protected void setSequenceMetaData(ColumnMetaDataImpl column, ResultSet resultSet) throws SQLException {
		final Boolean seq = resultSet.getBoolean(3);
		if (seq != null && seq.equals(true)) {
			column.setSequence(true);
			column.setSequenceName(column.getTableName());
		}
	}

	@Override
	protected String getColumnDatabaseName(SqlResourceDefinition definition, ResultSetMetaData resultSetMetaData,
			int colNumber) throws SQLException {
		return SqlResourceDefinitionUtils.getDefaultDatabase(definition);
		// return resultSetMetaData.getSchemaName(colNumber);
	}

	@Override
	protected String getColumnTableName(SqlResourceDefinition definition, ResultSetMetaData resultSetMetaData,
			int colNumber) throws SQLException {
		if (resultSetMetaData.getTableName(colNumber) == "") {
			return SqlResourceDefinitionUtils.getTable(definition, TableRole.Parent).getName();
		}
		else return resultSetMetaData.getTableName(colNumber);
		
		
	}
}
