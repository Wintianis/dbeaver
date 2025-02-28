/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2025 DBeaver Corp and others
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
package org.jkiss.dbeaver.tasks.ui.nativetool;

import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Group;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBPDataSourceProvider;
import org.jkiss.dbeaver.model.connection.DBPDriver;
import org.jkiss.dbeaver.model.navigator.*;
import org.jkiss.dbeaver.model.runtime.DBRRunnableContext;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.DBSWrapper;
import org.jkiss.dbeaver.model.task.DBTTaskType;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.tasks.nativetool.AbstractImportExportSettings;
import org.jkiss.dbeaver.tasks.nativetool.AbstractNativeToolSettings;
import org.jkiss.dbeaver.tasks.ui.DBTTaskConfigPanel;
import org.jkiss.dbeaver.tasks.ui.nativetool.internal.TaskNativeUIMessages;
import org.jkiss.dbeaver.tasks.ui.wizard.TaskConfigurationWizard;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.dbeaver.ui.dialogs.connection.ClientHomesSelector;
import org.jkiss.dbeaver.ui.navigator.database.DatabaseObjectsSelectorPanel;
import org.jkiss.utils.CommonUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;

public abstract class NativeToolConfigPanel<OBJECT_TYPE extends DBSObject> implements DBTTaskConfigPanel {

    private static final Log log = Log.getLog(NativeToolConfigPanel.class);

    private final DBRRunnableContext runnableContext;
    private final DBTTaskType taskType;
    private final Class<OBJECT_TYPE> objectClass;
    private final Class<? extends DBPDataSourceProvider> providerClass;

    private AbstractNativeToolWizard ieWizard;
    private ClientHomesSelector homesSelector;
    private OBJECT_TYPE selectedObject;
    private DBPDataSourceContainer curDataSource;
    private DatabaseObjectsSelectorPanel selectorPanel;

    public NativeToolConfigPanel(
        DBRRunnableContext runnableContext,
        DBTTaskType taskType,
        Class<OBJECT_TYPE> objectClass,
        Class<? extends DBPDataSourceProvider> providerClass)
    {
        this.runnableContext = runnableContext;
        this.taskType = taskType;
        this.objectClass = objectClass;
        this.providerClass = providerClass;
    }

    @Override
    public void createControl(Composite parent, TaskConfigurationWizard wizard, Runnable propertyChangeListener) {
        ieWizard = (AbstractNativeToolWizard) wizard;
        {
            Group databasesGroup = UIUtils.createControlGroup(parent, TaskNativeUIMessages.tools_wizard_database_group_title, 1, GridData.FILL_BOTH, 0);

            selectorPanel = new DatabaseObjectsSelectorPanel(
                databasesGroup,
                false,
                this.runnableContext) {
                @Override
                protected boolean isDatabaseFolderVisible(DBNDatabaseFolder folder) {
                    return folder.getChildrenClass() == objectClass;
                }

                @Override
                protected boolean isDatabaseObjectVisible(DBSObject obj) {
                    return objectClass.isInstance(obj);
                }

                @Override
                protected void onSelectionChange(Object element) {
                    selectedObject = element instanceof DBSWrapper && objectClass.isInstance(((DBSWrapper) element).getObject()) ?
                        objectClass.cast(((DBSWrapper) element).getObject()) : null;
                    AbstractNativeToolSettings settings = ieWizard.getSettings();
                    List<DBSObject> databaseObjects = settings.getDatabaseObjects();
                    databaseObjects.clear();
                    if (selectedObject != null) {
                        databaseObjects.add(selectedObject);
                    }
                    if (settings instanceof AbstractImportExportSettings) {
                        ((AbstractImportExportSettings) settings).fillExportObjectsFromInput();
                    }
                    updateHomeSelector();
                    propertyChangeListener.run();
                }

                @Override
                protected boolean isFolderVisible(DBNLocalFolder folder) {
                    for (DBNDataSource ds : folder.getNestedDataSources()) {
                        if (isDataSourceVisible(ds)) {
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                protected boolean isDataSourceVisible(DBNDataSource dataSource) {
                    try {
                        DBPDriver driver = dataSource.getDataSourceContainer().getDriver();
                        return providerClass.isInstance(driver.getDataSourceProvider()) &&
                            (driver.getNativeClientManager() != null && driver.getNativeClientManager().supportsNativeClients());
                    } catch (Exception e) {
                        log.debug(e);
                        return false;
                    }
                }

            };
        }

        {
            Composite clientGroup = UIUtils.createControlGroup((Composite) parent, TaskNativeUIMessages.tools_wizard_client_group_title, 1, GridData.FILL_HORIZONTAL, 0);
            homesSelector = new ClientHomesSelector(clientGroup, TaskNativeUIMessages.tools_wizard_client_group_client);
            homesSelector.addSelectionChangedListener(event -> propertyChangeListener.run());
            homesSelector.getPanel().setLayoutData(new GridData(GridData.FILL_HORIZONTAL));
        }
    }

    private void updateHomeSelector() {
        DBPDataSourceContainer newDataSource = null;
        if (selectedObject instanceof DBPDataSourceContainer) {
            newDataSource = (DBPDataSourceContainer) selectedObject;
        } else if (selectedObject != null) {
            newDataSource = selectedObject.getDataSource().getContainer();
        }
        if (newDataSource != null && curDataSource != newDataSource) {
            homesSelector.populateHomes(newDataSource.getDriver(), newDataSource.getConnectionConfiguration().getClientHomeId(), true);
        }
        curDataSource = newDataSource;
    }

    @Override
    public void loadSettings() {
        List<DBSObject> databaseObjects = ieWizard.getSettings().getDatabaseObjects();
        for (DBSObject obj : databaseObjects) {
            for (DBSObject child = obj; child != null; child = child.getParentObject()) {
                if (objectClass.isInstance(child)) {
                    selectedObject = objectClass.cast(child);
                    break;
                }
            }
        }

        DBPDataSourceContainer container = ieWizard.getSettings().getDataSourceContainer();
        if (selectorPanel != null && (selectedObject != null || container != null)) {
            try {
                DBNDatabaseNode[] catalogNode = new DBNDatabaseNode[1];
                ieWizard.getRunnableContext().run(true, true, monitor ->
                    catalogNode[0] = DBNUtils.getNodeByObject(monitor, selectedObject != null ? selectedObject : container, false));
                if (catalogNode[0] != null) {
                    List<DBNNode> selCatalogs = Collections.singletonList(catalogNode[0]);
                    //selectorPanel.checkNodes(selCatalogs, true);
                    selectorPanel.setSelection(selCatalogs);
                }
            } catch (InvocationTargetException e) {
                DBWorkbench.getPlatformUI().showError("Catalogs", " Error loading catalog list", e.getTargetException());
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    @Override
    public void saveSettings() {
        final String selectedHome = homesSelector.getSelectedHome();
        curDataSource.getConnectionConfiguration().setClientHomeId(selectedHome);
        curDataSource.persistConfiguration();
    }

    @Override
    public boolean isComplete() {
        return homesSelector.getSelectedHome() != null && selectedObject != null;
    }

    @Override
    public String getErrorMessage() {
        if (selectedObject == null) {
            return TaskNativeUIMessages.tools_wizard_error_no_database_object_selected;
        }
        if (CommonUtils.isEmpty(homesSelector.getSelectedHome())) {
            return TaskNativeUIMessages.tools_wizard_error_no_native_client_selected;
        }
        return null;
    }
}
