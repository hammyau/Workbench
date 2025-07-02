package com.ibm.safr.we.model.utilities;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/*
 * Copyright Contributors to the GenevaERS Project. SPDX-License-Identifier: Apache-2.0 (c) Copyright IBM Corporation 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.logging.Level;
import java.util.logging.Logger;

import org.genevaers.rcapps.RCDriver;

import com.ibm.safr.we.exceptions.SAFRException;
import com.ibm.safr.we.model.query.EnvironmentQueryBean;
import com.ibm.safr.we.model.utilities.export.ExportComponent;

public class PassGenerator {

    static transient Logger logger = Logger.getLogger("com.ibm.safr.we.model.utilities.PassGenerator");

    public static void runFromXML(String exportPath, String textType) throws SAFRException {
        RCDriver.setInputType("WBXML");
        RCDriver.setOutputPath(Paths.get(exportPath));
        RCDriver.setRCATextType(textType);
        logger.log(Level.INFO, "runFromXML from path " + exportPath);	
        RCDriver.runRCA(exportPath);
    }
    
    public static void runFromYAML(List<ExportComponent> views, EnvironmentQueryBean currentEnvironment, String exportPath, String textType) throws SAFRException {
    	List<String> viewNames = new ArrayList<String>();
    	views.stream().forEach(v -> viewNames.add(v.getComponent().getName()));
        RCDriver.setInputType("YAML");
        RCDriver.setOutputPath(Paths.get(exportPath));
        RCDriver.setRCATextType(textType);
        RCDriver.setViewNames(viewNames);
        RCDriver.setEnvironmentName(currentEnvironment.getName());
        logger.log(Level.INFO, "runFromXML from path " + exportPath);	
        RCDriver.runRCA(exportPath);
    }
    
    public static void clearOutputDirectory(String dir) {
        RCDriver.clearOutputPath(Paths.get(dir));
    }
    
    public static String getReportHtmlFile() {
        return RCDriver.getRCAreportFileName();
    }
    
}
