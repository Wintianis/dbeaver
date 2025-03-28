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

package org.jkiss.dbeaver.ext.gbase8s.model;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.ext.gbase8s.GBase8sConstants;
import org.jkiss.dbeaver.ext.generic.model.GenericSQLDialect;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.struct.DBSTypedObject;

/**
 * @author Chao Tian
 */
public class GBase8sSQLDialect extends GenericSQLDialect {

    public GBase8sSQLDialect() {
        super("GBase8s", "gbase8s");
    }

    @Override
    public String getColumnTypeModifiers(
            @NotNull DBPDataSource dataSource,
            @NotNull DBSTypedObject column,
            @NotNull String typeName,
            @NotNull DBPDataKind dataKind) {
        if (dataKind == DBPDataKind.NUMERIC) {
            if (isNumericType(typeName)) {
                // The precision for NUMERIC/DECIMAL/MONEY types ranges from 1 to 32, with a default of 16.
                // The precision for money types ranges from 2 to 32, also with a default of 16.
                // The scale ranges from 0 to p (precision) and can only be specified when precision is defined.
                // By default, the scale is set to null.
                int precision = (int) column.getMaxLength();
                if (precision < 0) {
                    precision = 0;
                } else if (precision > GBase8sConstants.MAX_NUMERIC_PRECISION) {
                    precision = GBase8sConstants.MAX_NUMERIC_PRECISION;
                }
                Integer scale = column.getScale();
                if (scale != null) {
                    if (scale.equals(GBase8sConstants.DEFAULT_NUMERIC_SCALE)) {
                        scale = null;
                    }
                    if (scale > precision) {
                        scale = precision;
                    }
                }
                // Special case for MONEY type fields when the precision equals 1 and the scale is null
                else if (GBase8sConstants.TYPE_MONEY.equalsIgnoreCase(typeName) && precision == 1) {
                    precision = 0;
                }
                if (precision > 0) {
                    return (scale == null) ? "(" + precision + ")" : "(" + precision + "," + scale + ")";
                }
                return null;
            }
            // The size for BIGSERIAL, SERIAL, and SERIAL8 types starts at 1, with a default value of 1.
            else if (isSerialType(typeName)) {
                long size = column.getMaxLength();
                if (size < 0) {
                    size = 0;
                }
                return (size > 0) ? "(" + size + ")" : null;
            }
        }
        // The length for CHAR, CHARACTER, and NCHAR types ranges from 1 to 32,767, with a default value of 1.
        else if (isFixedCharacterType(typeName)) {
            int length = (int) column.getMaxLength();
            if (length <= 0) {
                return null;
            } else if (length > GBase8sConstants.MAX_CHAR_PRECISION) {
                length = GBase8sConstants.MAX_CHAR_PRECISION;
            }
            return "(" + length + ")";
        }
        // The length for lvarchar/character/nchar types ranges from 1 to 32739, with a default of 2048.
        else if (isVariableCharacterType(typeName)) {
            int length = (int) column.getMaxLength();
            if (length <= 0 || length == GBase8sConstants.DEFAULT_LVARCHAR_PRECISION) {
                return null;
            } else if (length > GBase8sConstants.MAX_LVARCHAR_PRECISION) {
                length = GBase8sConstants.MAX_LVARCHAR_PRECISION;
            }
            return "(" + length + ")";
        }
        // The type INT is equivalent to INTEGER.
        else if (GBase8sConstants.TYPE_INT.equalsIgnoreCase(typeName.trim())) {
            typeName = GBase8sConstants.TYPE_INTEGER;
            dataKind = DBPDataKind.NUMERIC;
        }
        // The type REAL ignores length.
        else if (GBase8sConstants.TYPE_REAL.equalsIgnoreCase(typeName.trim())) {
            return null;
        }
        return super.getColumnTypeModifiers(dataSource, column, typeName, dataKind);
    }

    private boolean isNumericType(String typeName) {
        String type = typeName.trim();
        return GBase8sConstants.TYPE_DECIMAL.equalsIgnoreCase(type)
                || GBase8sConstants.TYPE_NUMERIC.equalsIgnoreCase(type)
                || GBase8sConstants.TYPE_MONEY.equalsIgnoreCase(type);
    }

    private boolean isSerialType(String typeName) {
        String type = typeName.trim();
        return GBase8sConstants.TYPE_BIGSERIAL.equalsIgnoreCase(type)
                || GBase8sConstants.TYPE_SERIAL.equalsIgnoreCase(type)
                || GBase8sConstants.TYPE_SERIAL8.equalsIgnoreCase(type);
    }

    private boolean isFixedCharacterType(String typeName) {
        String type = typeName.trim();
        return GBase8sConstants.TYPE_CHAR.equalsIgnoreCase(type)
                || GBase8sConstants.TYPE_CHARACTER.equalsIgnoreCase(type)
                || GBase8sConstants.TYPE_NCHAR.equalsIgnoreCase(type);
    }

    private boolean isVariableCharacterType(String typeName) {
        String type = typeName.trim();
        return GBase8sConstants.TYPE_LVARCHAR.equalsIgnoreCase(type)
                || GBase8sConstants.TYPE_CHARACTER.equalsIgnoreCase(type)
                || GBase8sConstants.TYPE_NCHAR.equalsIgnoreCase(type);
    }
}
