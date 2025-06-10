package com.ibm.safr.we.internal.data.dao.yamldao.transfers;

import com.ibm.safr.we.data.transfer.SAFREnvironmentalComponentTransfer;
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



public class YAMLViewSourceTransfer {

	private int sequenceNumber;
    private String logicalRecord;
    private String logicalFile;
    private String extractFilter;
    private String outputLF;
    private String outputPF;
    private Integer writeExit;
    private String writeExitParms;
    private Boolean outputOverride;
    private String outputLogic;
    
    public YAMLViewSourceTransfer() {
    	
    }
    
	public YAMLViewSourceTransfer(ViewSourceTransfer s) {
		extractFilter = s.getExtractFilterLogic();
		sequenceNumber = s.getSourceSeqNo();
		writeExit = s.getWriteExitId();
		writeExitParms = s.getWriteExitParams();
		outputOverride = s.isExtractOutputOverride();
		outputLogic = s.getExtractRecordOutput();
	}
	public String getLogicalRecord() {
		return logicalRecord;
	}
	public void setLogicalRecord(String logicalRecord) {
		this.logicalRecord = logicalRecord;
	}
	public String getLogicalFile() {
		return logicalFile;
	}
	public void setLogicalFile(String logicalFile) {
		this.logicalFile = logicalFile;
	}
	public int getSequenceNumber() {
		return sequenceNumber;
	}
	public void setSequenceNumber(int sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}
	public String getExtractFilter() {
		return extractFilter;
	}
	public void setExtractFilter(String extractFilter) {
		this.extractFilter = extractFilter;
	}
	public String getOutputLF() {
		return outputLF;
	}
	public void setOutputLF(String outputLF) {
		this.outputLF = outputLF;
	}
	public String getOutputPF() {
		return outputPF;
	}
	public void setOutputPF(String outputPF) {
		this.outputPF = outputPF;
	}
	public Integer getWriteExit() {
		return writeExit;
	}
	public void setWriteExit(Integer writeExit) {
		this.writeExit = writeExit;
	}
	public String getWriteExitParms() {
		return writeExitParms;
	}
	public void setWriteExitParms(String writeExitParms) {
		this.writeExitParms = writeExitParms;
	}
	public Boolean getOutputOverride() {
		return outputOverride;
	}
	public void setOutputOverride(Boolean outputOverride) {
		this.outputOverride = outputOverride;
	}
	public String getOutputLogic() {
		return outputLogic;
	}
	public void setOutputLogic(String outputLogic) {
		this.outputLogic = outputLogic;
	}
}
