package com.ibm.safr.we.internal.data.dao.yamldao;

import java.nio.file.Path;

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


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.CodeSetDAO;
import com.ibm.safr.we.data.transfer.CodeTransfer;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.YAMLDAOFactory;
import com.ibm.safr.we.model.CodeSet;
import com.ibm.safr.we.model.SAFRApplication;

/**
 *This class is used to implement the unimplemented methods of
 * <b>CodeSetDAO</b>. This class contains the methods to related to Code Table
 * which require database access.
 * 
 */
public class YAMLCodeSetDAO implements CodeSetDAO {
	static transient Logger logger = Logger
			.getLogger("com.ibm.safr.we.internal.data.dao.PGCodeSetDAO");

	public YAMLCodeSetDAO(Connection con, ConnectionParameters params, UserSessionParameters safrlogin) {
	}

	public List<CodeTransfer> getCodeSet(String codeCategory) throws DAOException {
		List<CodeTransfer> codeSet = new ArrayList<CodeTransfer>();
		return codeSet;
	}

	public Map<String, CodeSet> getCodeSetsMap() throws DAOException {
		new ArrayList<CodeTransfer>();
		Path homePath = YAMLDAOFactory.getGersHome();
		Path codesPath = homePath.resolve("codes.yaml");
		return YAMLizer.readYamlCodes(codesPath);
	}

	@Override
	public List<CodeTransfer> getAllCodeSets() throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}


}
