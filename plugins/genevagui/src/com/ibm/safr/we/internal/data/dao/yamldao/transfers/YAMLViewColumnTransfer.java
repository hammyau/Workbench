package com.ibm.safr.we.internal.data.dao.yamldao.transfers;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.ibm.safr.we.data.transfer.SAFREnvironmentalComponentTransfer;
import com.ibm.safr.we.data.transfer.ViewColumnSourceTransfer;
import com.ibm.safr.we.data.transfer.ViewColumnTransfer;
import com.ibm.safr.we.data.transfer.ViewSourceTransfer;



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



public class YAMLViewColumnTransfer {

	private ViewColumnTransfer column;
	private List<ViewColumnSourceTransfer> columnSources;
    
    public YAMLViewColumnTransfer() {
    	
    }
    
	public YAMLViewColumnTransfer(ViewSourceTransfer s) {
	}

	public ViewColumnTransfer getColumn() {
		return column;
	}

	public void setColumn(ViewColumnTransfer vct) {
		this.column = vct;
	}

	public List<ViewColumnSourceTransfer> getColumnSources() {
		return columnSources;
	}

	public void setColumnSources(List<ViewColumnSourceTransfer> columnSources) {
		this.columnSources = columnSources;
	}
	
	public void addColumnSource(ViewColumnSourceTransfer vcs) {
		columnSources.add(vcs);
	}
	
}
