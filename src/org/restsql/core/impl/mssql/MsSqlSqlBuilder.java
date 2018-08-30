package org.restsql.core.impl.mssql;

import java.util.List;

import org.restsql.core.ColumnMetaData;
import org.restsql.core.InvalidRequestException;
import org.restsql.core.Request;
import org.restsql.core.RequestValue;
import org.restsql.core.SqlResourceMetaData;
import org.restsql.core.TableMetaData;
import org.restsql.core.Request.Type;
import org.restsql.core.RequestValue.Operator;
import org.restsql.core.SqlBuilder.SqlStruct;
import org.restsql.core.impl.AbstractSqlBuilder;


public class MsSqlSqlBuilder extends AbstractSqlBuilder {
	private static final int DEFAULT_SELECT_SIZE = 300;
	
	@Override
	protected String buildSelectLimitSql(int limit, int offset) {
		return "";
	}

	@Override
	public SqlStruct buildSelectSql(SqlResourceMetaData metaData, String mainSql, Request request)
			throws InvalidRequestException {
		final SqlStruct sql = new SqlStruct(mainSql.length(), DEFAULT_SELECT_SIZE);
		sql.getMain().append(mainSql);
		buildSelectSql(metaData, request.getResourceIdentifiers(), sql);
		buildSelectSql(metaData, request.getParameters(), sql);
		addOrderBy(metaData, sql);

		// Handle limit and offset
		if (request.getSelectLimit() != null) {
			// Call concrete database-specific class to get the limit clause
			sql.appendToBothClauses(buildSelectLimitSql(request.getSelectLimit().intValue(), request
					.getSelectOffset().intValue()));
		}
		sql.appendToBothClauses(" FOR BROWSE");
		sql.compileStatements();
		return sql;
	}
	
	/** Adds order by statement . */
	private void addOrderBy(final SqlResourceMetaData metaData, final SqlStruct sql) {
		boolean firstColumn = true;
		firstColumn = addOrderByColumn(metaData, sql, firstColumn, metaData.getParent());
		addOrderByColumn(metaData, sql, firstColumn, metaData.getChild());
	}

	/** Adds order by column list for the table's primary keys. */
	private boolean addOrderByColumn(final SqlResourceMetaData metaData, final SqlStruct sql,
			boolean firstColumn, final TableMetaData table) {
		if (table != null) {
			for (final ColumnMetaData column : table.getPrimaryKeys()) {
				if (firstColumn) {
					sql.appendToBothClauses(" ORDER BY ");
					firstColumn = false;
				} else {
					sql.appendToBothClauses(", ");
				}
				sql.appendToBothClauses(column.getQualifiedColumnName());
			}
		}
		return firstColumn;
	}

	private void buildSelectSql(final SqlResourceMetaData metaData, final List<RequestValue> params,
			final SqlStruct sql) throws InvalidRequestException {
		if (params != null && params.size() > 0) {
			boolean validParamFound = false;
			for (final RequestValue param : params) {
				if (sql.getMain().indexOf("where ") > 0 || sql.getMain().indexOf("WHERE ") > 0
						|| sql.getClause().length() != 0) {
					sql.appendToBothClauses(" AND ");
				} else {
					sql.appendToBothClauses(" WHERE ");
				}

				for (final TableMetaData table : metaData.getTables()) {
					final ColumnMetaData column = table.getColumns().get(param.getName());
					if (column != null) {
						if (column.isReadOnly()) {
							throw new InvalidRequestException(InvalidRequestException.MESSAGE_READONLY_PARAM,
									column.getColumnLabel());
						}
						if (!column.isNonqueriedForeignKey()) {
							validParamFound = true;
							setNameValue(Request.Type.SELECT, metaData, column, param, true, sql, false);
						}
					}
				}
			}

			if (sql.getClause().length() > 0 && !validParamFound) {
				throw new InvalidRequestException(InvalidRequestException.MESSAGE_INVALID_PARAMS);
			}
		}
	}

	private void setNameValue(final Type requestType, final SqlResourceMetaData metaData,
			final ColumnMetaData column, final RequestValue param, final boolean columnIsSelector,
			final SqlStruct sql, final boolean useMain) throws InvalidRequestException {

		// Convert String to Number object if required
		column.normalizeValue(param);

		// Append the name
		if (requestType == Request.Type.SELECT) {
			appendToBoth(sql, useMain, column.getQualifiedColumnName());
		} else {
			appendToBoth(sql, useMain, column.getColumnName());
		}

		// Append the operator
		if (columnIsSelector && param.getOperator() == Operator.Equals && containsWildcard(param.getValue())) {
			appendToBoth(sql, useMain, " LIKE ");
		} else if (!columnIsSelector && requestType == Request.Type.UPDATE
				&& param.getOperator() == Operator.IsNull) {
			appendToBoth(sql, useMain, " = ");
		} else {
			switch (param.getOperator()) {
				case Equals:
					appendToBoth(sql, useMain, " = ");
					break;
				case In:
					appendToBoth(sql, useMain, " IN ");
					break;
				case IsNull:
					appendToBoth(sql, useMain, " IS NULL");
					break;
				case IsNotNull:
					appendToBoth(sql, useMain, " IS NOT NULL");
					break;
				case LessThan:
					appendToBoth(sql, useMain, " < ");
					break;
				case LessThanOrEqualTo:
					appendToBoth(sql, useMain, " <= ");
					break;
				case GreaterThan:
					appendToBoth(sql, useMain, " > ");
					break;
				case GreaterThanOrEqualTo:
					appendToBoth(sql, useMain, " >= ");
					break;
				case NotEquals:
					appendToBoth(sql, useMain, " != ");
					break;
				default: // case Escaped
					throw new InvalidRequestException(
							"SqlBuilder.setNameValue() found unexpected operator of type "
									+ param.getOperator());
			}
		}

		// Append the value
		if (param.getOperator() == Operator.In) {
			appendToBoth(sql, useMain, "(");
			boolean firstValue = true;
			for (final Object value : param.getInValues()) {
				if (!firstValue) {
					appendToBoth(sql, useMain, ",");
				}
				appendValue(useMain ? sql.getMain() : sql.getClause(),
						useMain ? sql.getPreparedMain() : sql.getPreparedClause(), sql.getPreparedValues(),
						value, column.isCharOrDateTimeType(), column);
				firstValue = false;
			}
			appendToBoth(sql, useMain, ")");
		} else if ((param.getOperator() != Operator.IsNull && param.getOperator() != Operator.IsNotNull)
				|| (!columnIsSelector && requestType == Request.Type.UPDATE)) {
			appendValue(useMain ? sql.getMain() : sql.getClause(),
					useMain ? sql.getPreparedMain() : sql.getPreparedClause(), sql.getPreparedValues(),
					param.getValue(), column.isCharOrDateTimeType(), column);
		}
	}

	private void appendToBoth(final SqlStruct sql, final boolean useMain, final String string) {
		if (useMain) {
			sql.appendToBothMains(string);
		} else {
			sql.appendToBothClauses(string);
		}
	}

	private boolean containsWildcard(final Object value) {
		boolean contains = false;
		if (value != null && value instanceof String) {
			final int index = ((String) value).indexOf("%");
			contains = index > -1;
		}
		return contains;
	}
	
	private void appendValue(final StringBuilder part, final StringBuilder preparedPart,
			final List<Object> preparedValues, final Object value, final boolean charOrDateTimeType,
			ColumnMetaData column) {
		if (value != null && charOrDateTimeType) {
			part.append('\'');
		}
		part.append(value);
		if (value != null && charOrDateTimeType) {
			part.append('\'');
		}
		preparedPart.append(buildPreparedParameterSql(column));
		preparedValues.add(value);
	}	
	
}
