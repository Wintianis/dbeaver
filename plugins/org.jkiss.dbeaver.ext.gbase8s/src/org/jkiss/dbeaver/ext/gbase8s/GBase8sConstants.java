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
    // Types

    public static final String TYPE_BIGINT = "bigint";
    public static final String TYPE_BIGSERIAL = "bigserial";
    public static final String TYPE_BOOLEAN = "boolean";
    public static final String TYPE_CHAR = "char";
    public static final String TYPE_CHARACTER = "character";
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

    public static final int DEFAULT_CHAR_LENGTH = 1;
    public static final int DEFAULT_LVARCHAR_LENGTH = 2048;
    public static final int DEFAULT_NUMERIC_SCALE = 255;

    public static final int MAX_CHAR_LENGTH = 32767;
    public static final int MAX_LVARCHAR_LENGTH = 32739;
    public static final int MAX_NUMERIC_PRECISION = 32;
    public static final int MAX_VARCHAR_LENGTH = 32765;

    //////////////////////////////////////////////////////
    // Constraints

    public static final String CONSTRAINT_TYPE = "CONSTRAINT_TYPE";
    public static final String CONSTRAINT_TYPE_PRIMARY_KEY = "P";
    public static final String CONSTRAINT_TYPE_UNIQUE_KEY = "U";
    public static final String CONSTRAINT_TYPE_CHECK = "C";

    public static final String CHECK_CLAUSE = "CHECK_TEXT";
}
