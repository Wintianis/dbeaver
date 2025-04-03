/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2024 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jkiss.dbeaver.ext.gbase8s;

/**
 * GBase8sConstants
 */
public class GBase8sConstants {

    public static final String JDBC_SQL_MODE = "SQLMODE";
    public static final String JDBC_SQL_MODE_ORACLE = "oracle";
    public static final String JDBC_SQL_MODE_GBASE = "gbase";

    public static final String SQL_TABLE_COMMENT = "COMMENT ON TABLE %s IS %s";

    //////////////////////////////////////////////////////
    /// Constraints

    public static final String CONSTRAINT_TYPE = "CONSTRAINT_TYPE";
    public static final String CONSTRAINT_TYPE_PRIMARY_KEY = "P";
    public static final String CONSTRAINT_TYPE_UNIQUE_KEY = "U";
    public static final String CONSTRAINT_TYPE_CHECK = "C";

    public static final String CHECK_CLAUSE = "CHECK_TEXT";

    //////////////////////////////////////////////////////
    /// Data Types

    public static final String TYPE_BIGINT = "bigint";
    public static final String TYPE_BIGSERIAL = "bigserial";
    public static final String TYPE_BOOLEAN = "boolean";
    public static final String TYPE_CHAR = "char";
    public static final String TYPE_CHARACTER = "character";
    public static final String TYPE_CHARACTER_VARYING = "character varying";
    public static final String TYPE_DECIMAL = "decimal";
    public static final String TYPE_INT = "int";
    public static final String TYPE_INT8 = "int8";
    public static final String TYPE_INTEGER = "integer";
    public static final String TYPE_LVARCHAR = "lvarchar";
    public static final String TYPE_MONEY = "money";
    public static final String TYPE_NCHAR = "nchar";
    public static final String TYPE_NVARCHAR = "nvarchar";
    public static final String TYPE_NVARCHAR2 = "nvarchar2";
    public static final String TYPE_NUMERIC = "numeric";
    public static final String TYPE_REAL = "real";
    public static final String TYPE_SERIAL = "serial";
    public static final String TYPE_SERIAL8 = "serial8";
    public static final String TYPE_SMALLFLOAT = "smallfloat";
    public static final String TYPE_SMALLINT = "smallint";
    public static final String TYPE_VARCHAR = "varchar";
    public static final String TYPE_VARCHAR2 = "varchar2";

    public static final String TYPE_YEAR_TO_YEAR = "datetime year to year";
    public static final String TYPE_YEAR_TO_MONTH = "datetime year to month";
    public static final String TYPE_YEAR_TO_DAY = "datetime year to day";
    public static final String TYPE_YEAR_TO_HOUR = "datetime year to hour";
    public static final String TYPE_YEAR_TO_MINUTE = "datetime year to minute";
    public static final String TYPE_YEAR_TO_SECOND = "datetime year to second";
    public static final String TYPE_YEAR_TO_FRACTION_1 = "datetime year to fraction(1)";
    public static final String TYPE_YEAR_TO_FRACTION_2 = "datetime year to fraction(2)";
    public static final String TYPE_YEAR_TO_FRACTION_3 = "datetime year to fraction(3)";
    public static final String TYPE_YEAR_TO_FRACTION_4 = "datetime year to fraction(4)";
    public static final String TYPE_YEAR_TO_FRACTION_5 = "datetime year to fraction(5)";

    public static final String TYPE_MONTH_TO_MONTH = "datetime month to month";
    public static final String TYPE_MONTH_TO_DAY = "datetime month to day";
    public static final String TYPE_MONTH_TO_HOUR = "datetime month to hour";
    public static final String TYPE_MONTH_TO_MINUTE = "datetime month to minute";
    public static final String TYPE_MONTH_TO_SECOND = "datetime month to second";
    public static final String TYPE_MONTH_TO_FRACTION_1 = "datetime month to fraction(1)";
    public static final String TYPE_MONTH_TO_FRACTION_2 = "datetime month to fraction(2)";
    public static final String TYPE_MONTH_TO_FRACTION_3 = "datetime month to fraction(3)";
    public static final String TYPE_MONTH_TO_FRACTION_4 = "datetime month to fraction(4)";
    public static final String TYPE_MONTH_TO_FRACTION_5 = "datetime month to fraction(5)";

    public static final String TYPE_DAY_TO_DAY = "datetime day to day";
    public static final String TYPE_DAY_TO_HOUR = "datetime day to hour";
    public static final String TYPE_DAY_TO_MINUTE = "datetime day to minute";
    public static final String TYPE_DAY_TO_SECOND = "datetime day to second";
    public static final String TYPE_DAY_TO_FRACTION_1 = "datetime day to fraction(1)";
    public static final String TYPE_DAY_TO_FRACTION_2 = "datetime day to fraction(2)";
    public static final String TYPE_DAY_TO_FRACTION_3 = "datetime day to fraction(3)";
    public static final String TYPE_DAY_TO_FRACTION_4 = "datetime day to fraction(4)";
    public static final String TYPE_DAY_TO_FRACTION_5 = "datetime day to fraction(5)";

    public static final String TYPE_HOUR_TO_HOUR = "datetime hour to hour";
    public static final String TYPE_HOUR_TO_MINUTE = "datetime hour to minute";
    public static final String TYPE_HOUR_TO_SECOND = "datetime hour to second";
    public static final String TYPE_HOUR_TO_FRACTION_1 = "datetime hour to fraction(1)";
    public static final String TYPE_HOUR_TO_FRACTION_2 = "datetime hour to fraction(2)";
    public static final String TYPE_HOUR_TO_FRACTION_3 = "datetime hour to fraction(3)";
    public static final String TYPE_HOUR_TO_FRACTION_4 = "datetime hour to fraction(4)";
    public static final String TYPE_HOUR_TO_FRACTION_5 = "datetime hour to fraction(5)";

    public static final String TYPE_MINUTE_TO_MINUTE = "datetime minute to minute";
    public static final String TYPE_MINUTE_TO_SECOND = "datetime minute to second";
    public static final String TYPE_MINUTE_TO_FRACTION_1 = "datetime minute to fraction(1)";
    public static final String TYPE_MINUTE_TO_FRACTION_2 = "datetime minute to fraction(2)";
    public static final String TYPE_MINUTE_TO_FRACTION_3 = "datetime minute to fraction(3)";
    public static final String TYPE_MINUTE_TO_FRACTION_4 = "datetime minute to fraction(4)";
    public static final String TYPE_MINUTE_TO_FRACTION_5 = "datetime minute to fraction(5)";

    public static final String TYPE_SECOND_TO_SECOND = "datetime second to second";
    public static final String TYPE_SECOND_TO_FRACTION_1 = "datetime second to fraction(1)";
    public static final String TYPE_SECOND_TO_FRACTION_2 = "datetime second to fraction(2)";
    public static final String TYPE_SECOND_TO_FRACTION_3 = "datetime second to fraction(3)";
    public static final String TYPE_SECOND_TO_FRACTION_4 = "datetime second to fraction(4)";
    public static final String TYPE_SECOND_TO_FRACTION_5 = "datetime second to fraction(5)";

    public static final int DEFAULT_CHAR_LENGTH = 1;
    public static final int DEFAULT_LVARCHAR_LENGTH = 2048;
    public static final int DEFAULT_NUMERIC_SCALE = 255;

    public static final int MAX_CHAR_LENGTH = 32767;
    public static final int MAX_LVARCHAR_LENGTH = 32739;
    public static final int MAX_NUMERIC_PRECISION = 32;
    public static final int MAX_VARCHAR_LENGTH = 32765;
}
