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

import org.jkiss.dbeaver.ext.gbase8s.GBase8sConstants;

/**
 * @author Chao Tian
 */
public enum GBase8sDateTimeDomain {

    YEAR_TO_YEAR(GBase8sConstants.TYPE_YEAR_TO_YEAR, 1024),
    YEAR_TO_MONTH(GBase8sConstants.TYPE_YEAR_TO_MONTH, 1538),
    YEAR_TO_DAY(GBase8sConstants.TYPE_YEAR_TO_DAY, 2052),
    YEAR_TO_HOUR(GBase8sConstants.TYPE_YEAR_TO_HOUR, 2566),
    YEAR_TO_MINUTE(GBase8sConstants.TYPE_YEAR_TO_MINUTE, 3080),
    YEAR_TO_SECOND(GBase8sConstants.TYPE_YEAR_TO_SECOND, 3594),
    YEAR_TO_FRACTION_1(GBase8sConstants.TYPE_YEAR_TO_FRACTION_1, 3851),
    YEAR_TO_FRACTION_2(GBase8sConstants.TYPE_YEAR_TO_FRACTION_2, 4108),
    YEAR_TO_FRACTION_3(GBase8sConstants.TYPE_YEAR_TO_FRACTION_3, 4365),
    YEAR_TO_FRACTION_4(GBase8sConstants.TYPE_YEAR_TO_FRACTION_4, 4622),
    YEAR_TO_FRACTION_5(GBase8sConstants.TYPE_YEAR_TO_FRACTION_5, 4879),

    MONTH_TO_MONTH(GBase8sConstants.TYPE_MONTH_TO_MONTH, 546),
    MONTH_TO_DAY(GBase8sConstants.TYPE_MONTH_TO_DAY, 1060),
    MONTH_TO_HOUR(GBase8sConstants.TYPE_MONTH_TO_HOUR, 1574),
    MONTH_TO_MINUTE(GBase8sConstants.TYPE_MONTH_TO_MINUTE, 2088),
    MONTH_TO_SECOND(GBase8sConstants.TYPE_MONTH_TO_SECOND, 2602),
    MONTH_TO_FRACTION_1(GBase8sConstants.TYPE_MONTH_TO_FRACTION_1, 2859),
    MONTH_TO_FRACTION_2(GBase8sConstants.TYPE_MONTH_TO_FRACTION_2, 3116),
    MONTH_TO_FRACTION_3(GBase8sConstants.TYPE_MONTH_TO_FRACTION_3, 3373),
    MONTH_TO_FRACTION_4(GBase8sConstants.TYPE_MONTH_TO_FRACTION_4, 3630),
    MONTH_TO_FRACTION_5(GBase8sConstants.TYPE_MONTH_TO_FRACTION_5, 3887),

    DAY_TO_DAY(GBase8sConstants.TYPE_DAY_TO_DAY, 580),
    DAY_TO_HOUR(GBase8sConstants.TYPE_DAY_TO_HOUR, 1094),
    DAY_TO_MINUTE(GBase8sConstants.TYPE_DAY_TO_MINUTE, 1608),
    DAY_TO_SECOND(GBase8sConstants.TYPE_DAY_TO_SECOND, 2122),
    DAY_TO_FRACTION_1(GBase8sConstants.TYPE_DAY_TO_FRACTION_1, 2379),
    DAY_TO_FRACTION_2(GBase8sConstants.TYPE_DAY_TO_FRACTION_2, 2636),
    DAY_TO_FRACTION_3(GBase8sConstants.TYPE_DAY_TO_FRACTION_3, 2893),
    DAY_TO_FRACTION_4(GBase8sConstants.TYPE_DAY_TO_FRACTION_4, 3150),
    DAY_TO_FRACTION_5(GBase8sConstants.TYPE_DAY_TO_FRACTION_5, 3407),

    HOUR_TO_HOUR(GBase8sConstants.TYPE_HOUR_TO_HOUR, 614),
    HOUR_TO_MINUTE(GBase8sConstants.TYPE_HOUR_TO_MINUTE, 1128),
    HOUR_TO_SECOND(GBase8sConstants.TYPE_HOUR_TO_SECOND, 1642),
    HOUR_TO_FRACTION_1(GBase8sConstants.TYPE_HOUR_TO_FRACTION_1, 1899),
    HOUR_TO_FRACTION_2(GBase8sConstants.TYPE_HOUR_TO_FRACTION_2, 2156),
    HOUR_TO_FRACTION_3(GBase8sConstants.TYPE_HOUR_TO_FRACTION_3, 2413),
    HOUR_TO_FRACTION_4(GBase8sConstants.TYPE_HOUR_TO_FRACTION_4, 2670),
    HOUR_TO_FRACTION_5(GBase8sConstants.TYPE_HOUR_TO_FRACTION_5, 2927),

    MINUTE_TO_MINUTE(GBase8sConstants.TYPE_MINUTE_TO_MINUTE, 648),
    MINUTE_TO_SECOND(GBase8sConstants.TYPE_MINUTE_TO_SECOND, 1162),
    MINUTE_TO_FRACTION_1(GBase8sConstants.TYPE_MINUTE_TO_FRACTION_1, 1419),
    MINUTE_TO_FRACTION_2(GBase8sConstants.TYPE_MINUTE_TO_FRACTION_2, 1676),
    MINUTE_TO_FRACTION_3(GBase8sConstants.TYPE_MINUTE_TO_FRACTION_3, 1933),
    MINUTE_TO_FRACTION_4(GBase8sConstants.TYPE_MINUTE_TO_FRACTION_4, 2190),
    MINUTE_TO_FRACTION_5(GBase8sConstants.TYPE_MINUTE_TO_FRACTION_5, 2447),

    SECOND_TO_SECOND(GBase8sConstants.TYPE_SECOND_TO_SECOND, 682),
    SECOND_TO_FRACTION_1(GBase8sConstants.TYPE_SECOND_TO_FRACTION_1, 939),
    SECOND_TO_FRACTION_2(GBase8sConstants.TYPE_SECOND_TO_FRACTION_2, 1196),
    SECOND_TO_FRACTION_3(GBase8sConstants.TYPE_SECOND_TO_FRACTION_3, 1453),
    SECOND_TO_FRACTION_4(GBase8sConstants.TYPE_SECOND_TO_FRACTION_4, 1710),
    SECOND_TO_FRACTION_5(GBase8sConstants.TYPE_SECOND_TO_FRACTION_5, 1967);

    public static int getIdByDefinition(String def) {
        for (GBase8sDateTimeDomain domain : values()) {
            if (domain.def.equalsIgnoreCase(def.trim())) {
                return domain.id;
            }
        }
        return 0;
    }

    public static String getDefinitionById(int id) {
        for (GBase8sDateTimeDomain domain : values()) {
            if (domain.id == id) {
                return domain.def;
            }
        }
        return null;
    }

    private final String def;
    private final int id;

    GBase8sDateTimeDomain(String def, int id) {
        this.def = def;
        this.id = id;
    }

    public String getDefinition() {
        return def;
    }

}
