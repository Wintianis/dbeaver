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

package org.jkiss.dbeaver.ext.gbase8s.edit;

import java.util.List;
import java.util.Map;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.gbase8s.model.GBase8sDateTimeDomain;
import org.jkiss.dbeaver.ext.generic.edit.GenericTableColumnManager;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.model.DBConstants;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.DBPDataTypeProvider;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEObjectRenamer;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.impl.edit.SQLDatabasePersistAction;
import org.jkiss.dbeaver.model.impl.sql.edit.SQLObjectEditor;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.dbeaver.model.struct.DBSDataType;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.utils.CommonUtils;

/**
 * @author Chao Tian
 */
public class GBase8sTableColumnManager extends GenericTableColumnManager
        implements DBEObjectRenamer<GenericTableColumn> {

    protected final ColumnModifier<GenericTableColumn> DataTypeModifier = (monitor, column, sql, command) -> {
        final String typeName = column.getTypeName();
        final DBPDataKind dataKind = column.getDataKind();
        final DBSDataType dataType = findDataType(column, typeName);
        if (dataType == null) {
            if (DBPDataKind.STRING.equals(dataKind) && GBase8sDateTimeDomain.getIdByDefinition(typeName) != 0) {
                sql.append(' ').append(typeName);
            } else if (DBPDataKind.DATETIME.equals(dataKind)) {
                long maxLength = column.getMaxLength();
                String def = GBase8sDateTimeDomain.getDefinitionById((int) maxLength);
                if (def != null) {
                    sql.append(' ').append(def);
                } else {
                    log.debug("Unrecognized DATETIME type with maxLength: " + maxLength);
                    sql.append(' ').append(GBase8sDateTimeDomain.YEAR_TO_FRACTION_5.getDefinition());
                }
            } else {
                log.debug("Type name '" + typeName + "' is not supported by driver");
            }
        } else {
            sql.append(' ').append(typeName);
            String modifiers = SQLUtils.getColumnTypeModifiers(column.getDataSource(), column, typeName, dataKind);
            if (modifiers != null) {
                sql.append(modifiers);
            }
        }
    };

    @Override
    protected ColumnModifier[] getSupportedModifiers(GenericTableColumn column, Map<String, Object> options) {
        return new ColumnModifier[] { DataTypeModifier, DefaultModifier, NotNullModifier };
    }

    private static DBSDataType findDataType(DBSObject object, String typeName) {
        DBPDataTypeProvider dataTypeProvider = DBUtils.getParentOfType(DBPDataTypeProvider.class, object);
        if (dataTypeProvider != null) {
            return dataTypeProvider.getLocalDataType(typeName);
        }
        return null;
    }

    @Override
    protected void addObjectCreateActions(
            @NotNull DBRProgressMonitor monitor,
            @NotNull DBCExecutionContext executionContext,
            @NotNull List<DBEPersistAction> actions,
            @NotNull SQLObjectEditor<GenericTableColumn, GenericTableBase>.ObjectCreateCommand command,
            @NotNull Map<String, Object> options) throws DBException {
        super.addObjectCreateActions(monitor, executionContext, actions, command, options);
        if (CommonUtils.isNotEmpty(command.getObject().getDescription())) {
            addColumnCommentAction(actions, command.getObject(), command.getObject().getParentObject());
        }
    }

    @Override
    protected void addObjectModifyActions(
            @NotNull DBRProgressMonitor monitor,
            @NotNull DBCExecutionContext executionContext,
            @NotNull List<DBEPersistAction> actionList,
            @NotNull SQLObjectEditor<GenericTableColumn, GenericTableBase>.ObjectChangeCommand command,
            @NotNull Map<String, Object> options) throws DBException {
        GenericTableColumn column = command.getObject();
        actionList.add(new SQLDatabasePersistAction("Modify column",
                "ALTER TABLE " + column.getTable().getFullyQualifiedName(DBPEvaluationContext.DDL) + " MODIFY "
                        + getNestedDeclaration(monitor, column.getTable(), command, options)));
    }

    @Override
    protected void addObjectExtraActions(
            @NotNull DBRProgressMonitor monitor,
            @NotNull DBCExecutionContext executionContext,
            @NotNull List<DBEPersistAction> actions,
            @NotNull NestedObjectCommand<GenericTableColumn, PropertyHandler> command,
            @NotNull Map<String, Object> options) throws DBException {
        // Add column comment action if column description is specified
        if (command.hasProperty(DBConstants.PROP_ID_DESCRIPTION)) {
            GenericTableColumn column = command.getObject();
            addColumnCommentAction(actions, column, column.getTable());
        }
    }

    @Override
    protected void addObjectRenameActions(
            @NotNull DBRProgressMonitor monitor,
            @NotNull DBCExecutionContext executionContext,
            @NotNull List<DBEPersistAction> actions,
            @NotNull SQLObjectEditor<GenericTableColumn, GenericTableBase>.ObjectRenameCommand command,
            @NotNull Map<String, Object> options) {
        final GenericTableColumn column = command.getObject();
        actions.add(new SQLDatabasePersistAction("Rename column",
                "ALTER TABLE " + column.getTable().getFullyQualifiedName(DBPEvaluationContext.DDL) + " RENAME "
                        + "COLUMN " + DBUtils.getQuotedIdentifier(column.getDataSource(), command.getOldName()) + " TO "
                        + DBUtils.getQuotedIdentifier(column.getDataSource(), command.getNewName())));
    }

    @Override
    public void renameObject(
            DBECommandContext commandContext,
            GenericTableColumn object,
            Map<String, Object> options,
            String newName) throws DBException {
        processObjectRename(commandContext, object, options, newName);
    }
}
