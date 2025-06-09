package com.ibm.safr.we.internal.data.dao.yamldao.transfers;

import com.ibm.safr.we.data.transfer.SAFREnvironmentalComponentTransfer;

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



public class YAMLViewSourceTransfer extends SAFREnvironmentalComponentTransfer {

	private Integer viewId;
	private Integer sourceSeqNo;
	private Integer lrFileAssocId;
	private String extractFilterLogic;
    private Integer extractFileAssociationId; // Extr Output File, OUTLFPFASSOCID
    private Integer writeExitId; // Extract Phase, WRITEEXITID
    private String writeExitParams; // Extract Phase, WRITEEXITPARAM
    private boolean extractOutputOverride; // EXTRACTOUTPUTIND
    private String extractRecordOutput;

}
