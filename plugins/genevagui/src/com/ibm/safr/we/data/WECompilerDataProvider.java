package com.ibm.safr.we.data;

import java.util.Map;

import org.genevaers.genevaio.dataprovider.CompilerDataProvider;
import org.genevaers.repository.components.LogicalRecord;
import org.genevaers.repository.components.LookupPath;
import org.genevaers.repository.components.ViewNode;

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

public class WECompilerDataProvider implements CompilerDataProvider {

	private DAOFactory DAOFact;
	private Integer environID;
	private Integer sourceLRID;
	private String environmentName;

	public WECompilerDataProvider() {
		DAOFact = DAOFactoryHolder.getDAOFactory();
	}
	
	@Override
	public Integer findExitID(String name, boolean procedure) {
		return DAOFact.getUserExitRoutineDAO().getUserExitRoutine(name, environID, procedure);
	}

	@Override
	public Integer findPFAssocID(String lfName, String pfName) {
		return DAOFact.getLogicalFileDAO().getLFPFAssocID(environID, lfName, pfName);
	}

	@Override
	public Map<String, Integer> getFieldsFromLr(int lrid) {
		return DAOFact.getLRFieldDAO().getFields(lrid, environID);
	}

	@Override
	public Map<String, Integer> getLookupTargetFields(String name) {
		return DAOFact.getLookupDAO().getTargetFields(name, environID);
	}

	@Override
	public int getEnvironmentID() {
		return environID;
	}

	public int getLogicalRecordID() {
		return sourceLRID;
	}

	@Override
	public void setEnvironmentID(int e) {
		environID = e;
	}

	@Override
	public void setLogicalRecordID(int lrid) {
		sourceLRID = lrid;
	}

	@Override
	public LogicalRecord getLogicalRecord(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LookupPath getLookup(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ViewNode getView(int arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void loadLR(int arg0, int arg1) {
		// TODO Auto-generated method stub
		
	}

    @Override
    public void setEnvironmentName(String n) {
        environmentName = n;
    }

    @Override
    public String getEnvironmentName() {
        return environmentName;
    }

	
}
