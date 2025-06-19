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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.google.common.flogger.FluentLogger;
import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.ViewSourceDAO;
import com.ibm.safr.we.data.transfer.ComponentAssociationTransfer;
import com.ibm.safr.we.data.transfer.LogicalRecordTransfer;
import com.ibm.safr.we.data.transfer.LookupPathSourceFieldTransfer;
import com.ibm.safr.we.data.transfer.LookupPathTransfer;
import com.ibm.safr.we.data.transfer.ViewSourceTransfer;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLookupTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewSourceTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewTransfer;
import com.ibm.safr.we.model.query.EnvironmentalQueryBean;
import com.ibm.safr.we.model.query.LogicalRecordQueryBean;
import com.ibm.safr.we.model.query.LookupQueryBean;

public class YAMLViewSourceDAO implements ViewSourceDAO {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

	private Connection con;
	private ConnectionParameters params;
	private PGSQLGenerator generator = new PGSQLGenerator();

	private YAMLViewSourceTransfer ourViewSource;
	private static int maxid=1;
	private static List<ViewSourceTransfer> viewSources;

	/**
	 * Constructor for this class.
	 * 
	 * @param con
	 *            : The connection set for database access.
	 * @param params
	 *            : The connection parameters which define the URL, userId and
	 *            other details of the connection.
	 * @param safrLogin
	 *            : The parameters related to the user who has logged into the
	 *            workbench.
	 */
	public YAMLViewSourceDAO(Connection con, ConnectionParameters params,
			UserSessionParameters safrLogin) {
		this.con = con;
		this.params = params;
	}

	public int getViewSourceLrId(Integer viewSrcId, Integer environmentId) throws DAOException {
    	YAMLViewTransfer v = YAMLViewDAO.getCurrentView();
    	String lrName = v.getViewSources().stream().filter(vs -> vs.getSequenceNumber()==viewSrcId).findFirst().get().getLogicalRecord();
    	return YAMLLogicalRecordDAO.getLogicalRecord(lrName, environmentId).getId();
    }
	
	public List<ViewSourceTransfer> getViewSources(Integer viewId, Integer environmentId) throws DAOException {
		//Within the scope of this view
		maxid=1;
		viewSources = new ArrayList<>();
		YAMLViewTransfer v = YAMLViewDAO.getCurrentView();
		v.getViewSources().stream().forEach(vs -> makeAndAddViewSource(viewSources, vs, environmentId, v.getId()));	
		return viewSources;
	}

	private void makeAndAddViewSource(List<ViewSourceTransfer> viewSources, YAMLViewSourceTransfer yvs, Integer environmentId, Integer viewId) {
		ViewSourceTransfer vs = new ViewSourceTransfer();
		vs.setId(maxid++);
		vs.setViewId(viewId);
		vs.setPersistent(false);
		vs.setEnvironmentId(environmentId);
		vs.setExtractFileAssociationId(null);
		vs.setExtractFilterLogic(yvs.getExtractFilter());
		vs.setExtractOutputOverride(yvs.getOutputOverride());
		vs.setExtractRecordOutput(yvs.getOutputLogic());
		ComponentAssociationTransfer lrlf = DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLRLFAssociation(yvs.getLogicalRecord(), yvs.getLogicalFile());
		vs.setLRFileAssocId(lrlf.getAssociationId());
		vs.setSourceSeqNo(yvs.getSequenceNumber());
		vs.setWriteExitId(yvs.getWriteExit());
		vs.setWriteExitParams(yvs.getWriteExitParms());
		logger.atInfo().log("Made VS:%d for view:%d", vs.getId(), vs.getViewId());
		viewSources.add(vs);
	}

	public void persistViewSources(List<ViewSourceTransfer> viewSrcTransferList) throws DAOException {

		YAMLViewTransfer vt = YAMLViewDAO.getCurrentView();
		viewSrcTransferList.stream().forEach(s -> addViewSources(vt, s));
		YAMLViewDAO.saveView(vt);
	}

	private void addViewSources(YAMLViewTransfer vt, ViewSourceTransfer s) {
		ourViewSource =new YAMLViewSourceTransfer(s);
		ComponentAssociationTransfer lrlf = DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLRLFAssociation(s.getLRFileAssocId(), s.getEnvironmentId());
		ourViewSource.setLogicalFile(lrlf.getAssociatedComponentName());
		ourViewSource.setLogicalRecord(lrlf.getAssociatingComponentName());
		vt.addViewSource(ourViewSource);
	}

	public Map<Integer, List<EnvironmentalQueryBean>> getViewSourceLookupPathDetails(Integer srcLRID, Integer environmentId) throws DAOException {
		Map<Integer, List<EnvironmentalQueryBean>> result = new HashMap<Integer, List<EnvironmentalQueryBean>>();
		//Want to find the lookups that logicalRecordId can be the source for
		List<LookupQueryBean> lks = DAOFactoryHolder.getDAOFactory().getLookupDAO().queryAllLookups(environmentId, null);
		LogicalRecordTransfer srclr = DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLogicalRecord(srcLRID, environmentId);
		LogicalRecordQueryBean srclrBean = new LogicalRecordQueryBean(environmentId, srclr.getId(),srclr.getName(), null, null, null, null, null, null, null, null, null, null, null);

		lks.stream().filter(lk -> lk.getSourceLR().equalsIgnoreCase(srclr.getName())).forEach(lk -> buildResult(result, lk, srclrBean, environmentId));
		
		logger.atInfo().log("ViewSourceLookupPathDetails");
		result.entrySet().stream().forEach(e -> loglk(e));
		
		return result;
	}

	private void loglk(Entry<Integer, List<EnvironmentalQueryBean>> e) {
		logger.atInfo().log("srcLr %d", e.getKey());
		e.getValue().stream().forEach(v -> logger.atInfo().log("id %d name %s", v.getId(), v.getName()));
	}

	private void buildResult(Map<Integer, List<EnvironmentalQueryBean>> result, LookupQueryBean lk, LogicalRecordQueryBean srclrBean, Integer environmentId) {
		LookupQueryBean lkBean = new LookupQueryBean(environmentId, lk.getId(), lk.getName(), null, 1, 0, null, null, null, null, null, null, null, null, null);
		YAMLLookupTransfer ylkt = YAMLLookupDAO.getCurrentLKTransfer();
		LogicalRecordTransfer targlr = DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLogicalRecord(ylkt.getTargetLR(), environmentId);
		LogicalRecordQueryBean targlrBean = new LogicalRecordQueryBean(environmentId, targlr.getId(),targlr.getName(), null, null, null, null, null, null, null, null, null, null, null);
		if(result.containsKey(srclrBean.getId())) {
			result.get(srclrBean.getId()).add(lkBean);
		} else {
			ArrayList<EnvironmentalQueryBean> innerList = new ArrayList<EnvironmentalQueryBean>();
			innerList.add(targlrBean);
			innerList.add(lkBean);
			result.put(targlr.getId(), innerList);
		}
	}

	public Map<Integer, List<LookupPathSourceFieldTransfer>> getLookupPathSymbolicFields(
			List<Integer> lkupPathIds, Integer environmentId)
			throws DAOException {
		Map<Integer, List<LookupPathSourceFieldTransfer>> lookupPathSymbolicFields = new HashMap<Integer, List<LookupPathSourceFieldTransfer>>();
		List<LookupPathSourceFieldTransfer> innerList = null;
		if (lkupPathIds == null || lkupPathIds.size() == 0) {
			return lookupPathSymbolicFields;
		}
		try {
			String placeholders = generator.getPlaceholders(lkupPathIds.size());
			String selectQuery = "SELECT ENVIRONID ,LOOKUPSTEPID, LOOKUPID, SYMBOLICNAME FROM "
					+ params.getSchema() + ".LOOKUPSRCKEY WHERE LOOKUPID IN (" + placeholders +")"
					+ " AND ENVIRONID = ? "
					+ " AND FLDTYPE = 3 AND SYMBOLICNAME IS NOT NULL";
			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectQuery);
					int ndx = 1;
					for(int i =0; i<lkupPathIds.size(); i++) {
						pst.setInt(ndx++,  lkupPathIds.get(i));
					}
					pst.setInt(ndx, environmentId);
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
			while (rs.next()) {

				LookupPathSourceFieldTransfer srcFldTrans = new LookupPathSourceFieldTransfer();
				srcFldTrans.setEnvironmentId(rs.getInt("ENVIRONID"));
				srcFldTrans.setId(rs.getInt("LOOKUPSTEPID"));
				srcFldTrans.setSymbolicName(DataUtilities.trimString(rs.getString("SYMBOLICNAME")));
				Integer lookupPathId = rs.getInt("LOOKUPID");

				if (lookupPathSymbolicFields.containsKey(lookupPathId)) {
					lookupPathSymbolicFields.get(lookupPathId).add(srcFldTrans);

				} else {
					innerList = new ArrayList<LookupPathSourceFieldTransfer>();
					innerList.add(srcFldTrans);
					lookupPathSymbolicFields.put(lookupPathId, innerList);
				}
			}
			pst.close();
			rs.close();
		} catch (SQLException e) {
			throw DataUtilities.createDAOException("Database error occurred while retrieving all the source fields of the Lookup Paths which are of type Symbolic.",e);
		}
		return lookupPathSymbolicFields;
	}

	public void removeViewSources(List<Integer> vwSrcIds, Integer environmentId)
			throws DAOException {
		if (vwSrcIds == null || vwSrcIds.size() == 0) {
			return;
		}
		try {
			String placeholders = generator.getPlaceholders(vwSrcIds.size());
			DataUtilities.integerListToString(vwSrcIds);

			// deleting the column sources related to these View Sources.
			String deleteColSourcesQuery = "DELETE FROM " + params.getSchema()
					+ ".VIEWCOLUMNSOURCE WHERE VIEWSOURCEID IN (" + placeholders + " )"
					+ " AND ENVIRONID = ? " ;
			PreparedStatement pst = null;

			while (true) {
				try {
					pst = con.prepareStatement(deleteColSourcesQuery);
					int ndx = 1;
					for(int i =0; i<vwSrcIds.size(); i++) {
						pst.setInt(ndx++,  vwSrcIds.get(i));
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

			// deleting the View Sources
			String deleteViewSourcesQuery = "Delete From " + params.getSchema()
					+ ".VIEWSOURCE Where VIEWSOURCEID IN (" + placeholders + " )"
					+ " AND ENVIRONID = ? " ;
			PreparedStatement pst1 = null;

			while (true) {
				try {
					pst1 = con.prepareStatement(deleteViewSourcesQuery);
					int ndx = 1;
					for(int i =0; i<vwSrcIds.size(); i++) {
						pst1.setInt(ndx++,  vwSrcIds.get(i));
					}
					pst1.setInt(ndx, environmentId);
					pst1.executeUpdate();
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
			pst1.close();

		} catch (SQLException e) {
			throw DataUtilities.createDAOException(
					"Database error occurred while deleting View Sources.", e);
		}

	}
	
	public static List<ViewSourceTransfer> getViewSources() {
		return viewSources;
	}
}
