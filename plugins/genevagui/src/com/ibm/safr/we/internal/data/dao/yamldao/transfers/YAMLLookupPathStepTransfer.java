package com.ibm.safr.we.internal.data.dao.yamldao.transfers;

import java.util.ArrayList;
import java.util.List;

import com.ibm.safr.we.data.transfer.LookupPathSourceFieldTransfer;
import com.ibm.safr.we.data.transfer.LookupPathStepTransfer;

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


public class YAMLLookupPathStepTransfer extends LookupPathStepTransfer {

	private List<LookupPathSourceFieldTransfer> sources = new ArrayList<>();
	private int targetLR;
	private int targetLF;
	

	public YAMLLookupPathStepTransfer() {
		
	}
	
	public void addSource(LookupPathSourceFieldTransfer s) {
		sources.add(s);
	}
	
	public List<LookupPathSourceFieldTransfer> getSources() {
		return sources;
	}
	
	public void setSources(List<LookupPathSourceFieldTransfer> srcs) {
		sources = srcs;
	}
	
	public void clearSources() {
		sources.clear();
	}

	public int getTargetLR() {
		return targetLR;
	}

	public void setTargetLR(int targetLR) {
		this.targetLR = targetLR;
	}

	public int getTargetLF() {
		return targetLF;
	}

	public void setTargetLF(int targetLF) {
		this.targetLF = targetLF;
	}

}
