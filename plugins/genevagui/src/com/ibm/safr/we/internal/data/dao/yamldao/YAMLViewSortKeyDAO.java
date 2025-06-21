package com.ibm.safr.we.internal.data.dao.yamldao;

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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.ViewSortKeyDAO;
import com.ibm.safr.we.data.transfer.ComponentAssociationTransfer;
import com.ibm.safr.we.data.transfer.ViewSortKeyTransfer;
import com.ibm.safr.we.data.transfer.ViewSourceTransfer;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewSourceTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewTransfer;

public class YAMLViewSortKeyDAO implements ViewSortKeyDAO {

	static transient Logger logger = Logger.getLogger("com.ibm.safr.we.internal.data.dao.PGViewSortKeyDAO");

	public YAMLViewSortKeyDAO(Connection con, ConnectionParameters params,
			UserSessionParameters safrLogin) {
	}

	public List<ViewSortKeyTransfer> getViewSortKeys(Integer viewId, Integer environmentId) throws DAOException {
		List<ViewSortKeyTransfer> vstTransferList = new ArrayList<ViewSortKeyTransfer>();
		YAMLViewTransfer vt = YAMLViewDAO.getCurrentView();
		vt.getViewSortKeys().stream().forEach(vsk -> vstTransferList.add(vsk));	
		return vstTransferList;
	}

	public void persistViewSortKeys(List<ViewSortKeyTransfer> vskTransferList) throws DAOException {
		if (vskTransferList == null || vskTransferList.isEmpty()) {
			return;
		}
		YAMLViewTransfer vt = YAMLViewDAO.getCurrentView();
		vt.setViewSortKeys(new ArrayList<>());
		vskTransferList.stream().forEach(s -> addViewSortKeys(vt, s));
	}

	private void addViewSortKeys(YAMLViewTransfer vt, ViewSortKeyTransfer sk) {
		vt.addViewSortKey(sk);
	}

	public void removeViewSortKeys(List<Integer> vskIdsList, Integer environmentId) throws DAOException {
		//Nothing to do
	}
}
