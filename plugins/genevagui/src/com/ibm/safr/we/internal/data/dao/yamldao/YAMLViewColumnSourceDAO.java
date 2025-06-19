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


import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.common.flogger.FluentLogger;
import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.ViewColumnSourceDAO;
import com.ibm.safr.we.data.transfer.ViewColumnSourceTransfer;
import com.ibm.safr.we.data.transfer.ViewColumnTransfer;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewColumnTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewTransfer;
import com.ibm.safr.we.model.SAFRApplication;

public class YAMLViewColumnSourceDAO implements ViewColumnSourceDAO {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private Connection con;
	private ConnectionParameters params;
	private PGSQLGenerator generator = new PGSQLGenerator();
	private static int seq;
	private static int maxid;

	public YAMLViewColumnSourceDAO(Connection con, ConnectionParameters params,	UserSessionParameters safrLogin) {
		this.con = con;
		this.params = params;
	}

	public List<ViewColumnSourceTransfer> getViewColumnSources(Integer viewId, Integer environmentId) throws DAOException {
		maxid=1;
        seq = 1;
		List<ViewColumnSourceTransfer> viewColumnSources = new ArrayList<ViewColumnSourceTransfer>();
		YAMLViewTransfer vt = YAMLViewDAO.getCurrentView();
		vt.getViewColumns().stream().forEach(c -> addToColSources(viewColumnSources, c, vt.getId()));
		return viewColumnSources; 
	}

    private void addToColSources(List<ViewColumnSourceTransfer> viewColumnSources, YAMLViewColumnTransfer c, Integer viewId) {
       	c.getColumnSources().stream().forEach(cs -> addColumnSource(viewColumnSources, cs, viewId, c));
	}

	private void addColumnSource(List<ViewColumnSourceTransfer> viewColumnSources, ViewColumnSourceTransfer cs, int viewId, YAMLViewColumnTransfer c ) {
		cs.setId(maxid++);
		cs.setPersistent(false);
		cs.setViewSourceId(seq);
		ViewColumnSourceTransfer vcs = c.getColumnSources().get(seq-1);
		Integer vsid = vcs.getViewSourceId();
		int srclrid = DAOFactoryHolder.getDAOFactory().getViewSourceDAO().getViewSourceLrId(vsid, null);
		DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLogicalRecord(srclrid, null);
		cs.setViewColumnId(c.getColumn().getId());
		cs.setViewId(viewId);
		cs.setEnvironmentId(0);
		logger.atInfo().log("Add Column Source:%d for view:%d and viewsource:%d, column:%d",cs.getId(), cs.getViewId(), cs.getViewSourceId(), cs.getViewColumnId());
		viewColumnSources.add(cs);
	}

	public int getViewColumnSourceLrId(Integer viewColSrcId, Integer environmentId) throws DAOException {
        int result = 0;
        try {
    
            String selectString = "SELECT A.LOGRECID FROM " +
                params.getSchema() + ".LRLFASSOC A," +
                params.getSchema() + ".VIEWSOURCE B," +
                params.getSchema() + ".VIEWCOLUMNSOURCE C " +
                "WHERE A.ENVIRONID=B.ENVIRONID " + 
                "AND A.LRLFASSOCID=B.INLRLFASSOCID " +
                "AND B.ENVIRONID=C.ENVIRONID " +
                "AND B.VIEWSOURCEID=C.VIEWSOURCEID " +
                "AND C.VIEWCOLUMNSOURCEID=? " +
                "AND C.ENVIRONID=?";
            PreparedStatement pst = null;
            ResultSet rs = null;
            while (true) {
                try {
                    pst = con.prepareStatement(selectString);
                    pst.setInt(1, viewColSrcId);
                    pst.setInt(2, environmentId);
                    rs = pst.executeQuery();
                    break;
                } catch (SQLException se) {
                    if (con.isClosed()) {
                        // lost database connection, so reconnect and retry
                        con = DAOFactoryHolder.getDAOFactory().reconnect();
                    } else {
                        throw se;
                    }
                }
            }
            if (rs.next()) {
                result = rs.getInt(1);
            }
            pst.close();
            rs.close();
        } catch (SQLException e) {
            throw DataUtilities.createDAOException(
                "Database error occurred while retrieving View Column Source with id ["+ viewColSrcId + "]", e);
        }
        return result;
    }
	
	public List<ViewColumnSourceTransfer> persistViewColumnSources(List<ViewColumnSourceTransfer> viewColSrcTransferList) throws DAOException {
		YAMLViewTransfer vt = YAMLViewDAO.getCurrentView();
		//add vcs to each correct column
		vt.getViewColumns().stream().forEach(c -> addViewSourcesToColumn(c, viewColSrcTransferList));
		//viewSrcTransferList.stream().forEach(s -> addViewSources(vt, s));
		YAMLViewDAO.saveView(vt);
		return viewColSrcTransferList;
	}

    private void addViewSourcesToColumn(YAMLViewColumnTransfer c, List<ViewColumnSourceTransfer> viewColSrcTransferList) {
		List<ViewColumnSourceTransfer> vcss = new ArrayList<>();
		c.setColumnSources(vcss);
		//We can probable make a more efficient way to manage this.... like a view column map?
    	viewColSrcTransferList.stream().filter(vcs -> vcs.getViewColumnId() == c.getColumn().getId()).forEach(vcst -> addVCSToColumn(c, vcst));
    }

	private void addVCSToColumn(YAMLViewColumnTransfer c, ViewColumnSourceTransfer vcst) {
		c.addColumnSource(vcst);
	}

    
    protected Map<Integer, Integer> getColIDMap(Integer viewSourceId, List<ViewColumnSourceTransfer> viewColCreateList) throws SQLException {
        
        Map<Integer, Integer> idMap = new HashMap<Integer, Integer>();
        String placeholders = generator.getPlaceholders(viewColCreateList.size());
        int viewID = viewColCreateList.get(0).getViewId();
        int envID = viewColCreateList.get(0).getEnvironmentId();
        
        String chkSt = "select viewcolumnid,viewcolumnsourceid " +
            "from " + params.getSchema() + ".viewcolumnsource " +
            "where environid= ? " +
            "and viewid= ? " +
            "and viewsourceid= ? " +
            "and viewcolumnid in ( " + placeholders + ")";                    
        PreparedStatement pst = null;
        ResultSet rs = null;
        pst = con.prepareCall(chkSt);
        int ndx = 1;
        pst.setInt(ndx++, envID);
        pst.setInt(ndx++, viewID);
        pst.setInt(ndx++, viewSourceId);
        for(int i=0; i<viewColCreateList.size(); i++) {
        	pst.setInt(ndx++, viewColCreateList.get(i).getViewColumnId());
        }
        rs = pst.executeQuery();                    
        // form map of column number to id
        while (rs.next()) {
            Integer colId = rs.getInt(1);
            Integer id = rs.getInt(2);
            idMap.put(colId, id);
        }
        rs.close();
        pst.close();
        
        return idMap;
    }
	
	public void removeViewColumnSources(List<Integer> vwColumnSrcIds,
			Integer environmentId) throws DAOException {
		if (vwColumnSrcIds == null || vwColumnSrcIds.size() == 0) {
			return;
		}
		try {
			String placeholders = generator.getPlaceholders(vwColumnSrcIds.size()); 

			String deleteColSourcesQuery = "Delete From " + params.getSchema()
					+ ".VIEWCOLUMNSOURCE Where VIEWCOLUMNSOURCEID IN (" 
					+ placeholders + ") AND ENVIRONID = ? ";
			PreparedStatement pst = null;

			while (true) {
				try {
					pst = con.prepareStatement(deleteColSourcesQuery);
					int ndx = 1;
					for(int i=0; i<vwColumnSrcIds.size(); i++) {
						pst.setInt(ndx++, vwColumnSrcIds.get(i));
					}
					pst.setInt(ndx, environmentId);
					pst.executeUpdate();
					break;
				} catch (SQLException se) {
					if (con.isClosed()) {
						// lost database connection, so reconnect and retry
						con = DAOFactoryHolder.getDAOFactory().reconnect();
					} else {
						throw se;
					}
				}
			}
			pst.close();

		} catch (SQLException e) {
			throw DataUtilities.createDAOException("Database error occurred while deleting View Column Sources.",e);
		}
	}
}
