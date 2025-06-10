package com.ibm.safr.we.internal.data.dao.yamldao;

import java.io.File;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.ibm.safr.we.constants.ComponentType;
import com.ibm.safr.we.constants.EditRights;
import com.ibm.safr.we.constants.SortType;
import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DAOUOWInterruptedException;
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.LookupDAO;
import com.ibm.safr.we.data.transfer.ComponentAssociationTransfer;
import com.ibm.safr.we.data.transfer.DependentComponentTransfer;
import com.ibm.safr.we.data.transfer.LogicalRecordTransfer;
import com.ibm.safr.we.data.transfer.LookupPathTransfer;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLogicalRecordTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLookupTransfer;
import com.ibm.safr.we.model.SAFRApplication;
import com.ibm.safr.we.model.query.LogicalRecordQueryBean;
import com.ibm.safr.we.model.query.LookupQueryBean;

/**
 * This class is used to implement the unimplemented methods of
 * <b>LookupDAODAO</b>. This class contains the methods related to Lookup Path
 * which require database access.
 * 
 */
public class YAMLLookupDAO implements LookupDAO {
	static transient Logger logger = Logger.getLogger("com.ibm.safr.we.internal.data.dao.YAMLLookupDAO");

	private static final String TABLE_NAME = "LOOKUP";
	private static final String COL_ENVID = "ENVIRONID";
	private static final String COL_ID = "LOOKUPID";
	private static final String COL_NAME = "NAME";
	private static final String COL_SOURCE = "SRCLRID";
	private static final String COL_DESTLRLFASSOCID = "DESTLRLFASSOCID";
	private static final String COL_VALID = "VALIDIND";
	private static final String COL_COMMENTS = "COMMENTS";
	private static final String COL_CREATETIME = "CREATEDTIMESTAMP";
	private static final String COL_CREATEBY = "CREATEDUSERID";
	private static final String COL_MODIFYTIME = "LASTMODTIMESTAMP";
	private static final String COL_MODIFYBY = "LASTMODUSERID";
    private static final String COL_ACTIVATETIME = "LASTACTTIMESTAMP";
    private static final String COL_ACTIVATEBY = "LASTACTUSERID";

	private Connection con;
	private ConnectionParameters params;
	private UserSessionParameters safrLogin;
	private PGSQLGenerator generator = new PGSQLGenerator();

	private Map<Integer, LookupQueryBean> lkBeans = new TreeMap<>();

	private static YAMLLookupTransfer ourLkTxf;

	private static int maxid;

	public YAMLLookupDAO(Connection con, ConnectionParameters params, UserSessionParameters safrLogin) {
		this.con = con;
		this.params = params;
		this.safrLogin = safrLogin;
	}

	public List<LookupQueryBean> queryAllLookups(Integer environmentId,	SortType sortType) throws DAOException {
		List<LookupQueryBean> result = new ArrayList<LookupQueryBean>();
		maxid = 0;
		Path lksPath = YAMLizer.getLookupsPath();
		lksPath.toFile().mkdirs();
		File[] lks = lksPath.toFile().listFiles();
		
		if(lks.length > 0) {
			Stream.of(lks)
		    	      .filter(file -> file.isFile())
		    	      .forEach(lk -> addToResults(result, lk, environmentId));
		}
		return result;
	}

	private Object addToResults(List<LookupQueryBean> result, File lk, Integer environmentId) {
		ourLkTxf = (YAMLLookupTransfer) YAMLizer.readYaml(lk.toPath(), ComponentType.LookupPath);
		if(ourLkTxf.getId() > maxid) {
			maxid = ourLkTxf.getId();
		}
		LookupQueryBean lkBean = new LookupQueryBean(
				environmentId, ourLkTxf.getId(), 
				ourLkTxf.getName(),
				DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLogicalRecord(ourLkTxf.getSourceLRId(), environmentId).getName(),
				1,
				ourLkTxf.getSteps().size(),
				DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLogicalRecord(ourLkTxf.getTargetLR(), environmentId).getName(),
				DAOFactoryHolder.getDAOFactory().getLogicalFileDAO().getLogicalFile(ourLkTxf.getTargetLF(), environmentId).getName(), //Should could use names here
				EditRights.ReadModifyDelete,
				ourLkTxf.getCreateTime(), 
				ourLkTxf.getCreateBy(), 
				ourLkTxf.getModifyTime(), 
				ourLkTxf.getModifyBy(),
				ourLkTxf.getCreateTime(),
				ourLkTxf.getCreateBy()); 
			result.add(lkBean);
			lkBeans.put(ourLkTxf.getId(), lkBean);
		return lkBean;
	}

    public List<LookupQueryBean> queryLookupsForBAL(Integer environmentId,
        SortType sortType) throws DAOException {
        List<LookupQueryBean> result = new ArrayList<LookupQueryBean>();
            return result;
    }
	
	public LookupPathTransfer getLookupPath(Integer id, Integer environmentId)	throws DAOException {
		Path lksPath = YAMLizer.getLookupsPath();
		lksPath.toFile().mkdirs();
		if(lkBeans.size() == 0) {
			queryAllLookups(environmentId, null);
		}
		Path lkPath = lksPath.resolve(lkBeans.get(id).getName()+".yaml");
		ourLkTxf = (YAMLLookupTransfer) YAMLizer.readYaml(lkPath, ComponentType.LookupPath);
		List<ComponentAssociationTransfer> lfa = DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getAssociatedLogicalFiles(ourLkTxf.getTargetLR(), environmentId);
		ComponentAssociationTransfer lrlfa =  DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLRLFAssociation(ourLkTxf.getTargetLR(), ourLkTxf.getTargetLF(), environmentId);
		if(lrlfa == null) {
			logger.severe("No association for target LR " + ourLkTxf.getTargetLR());
		} else {
			ourLkTxf.setTargetXLRFileId(lrlfa.getAssociationId());
		}
		return ourLkTxf;
	}

	public LookupPathTransfer persistLookupPath(LookupPathTransfer lkuptrans) throws DAOException, SAFRNotFoundException {
		if (!lkuptrans.isPersistent()) {
			return (createLookupPath(lkuptrans));
		} else {
			return (updateLookupPath(lkuptrans));
		}
	}

	/**
	 * This method is to create the Lookup Path in LOOKUP
	 * 
	 * @param lkuptrans
	 *            : The transfer object which contains the values which are to
	 *            be set in the columns for the corresponding Lookup Path which
	 *            is being created.
	 * @return The transfer object which contains the values which are received
	 *         from the LOOKUP for the Lookup Path which is created.
	 * @throws DAOException
	 */
	private LookupPathTransfer createLookupPath(LookupPathTransfer lkuptrans) throws DAOException {
		ourLkTxf  = new YAMLLookupTransfer();
		ourLkTxf.setEnvironmentId(SAFRApplication.getUserSession().getEnvironment().getId());
		ourLkTxf.setName(lkuptrans.getName());
		ourLkTxf.setId(lkuptrans.getId());
		ourLkTxf.setValidInd(lkuptrans.isValidInd());
		ourLkTxf.setSourceLRId(lkuptrans.getSourceLRId());
		ComponentAssociationTransfer lrlfa =  DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLRLFAssociation(lkuptrans.getTargetXLRFileId(), 0);
		ourLkTxf.setTargetLF(lrlfa.getAssociatedComponentId());
		ourLkTxf.setTargetLR(lrlfa.getAssociatingComponentId());
		ourLkTxf.setTargetXLRFileId(lrlfa.getAssociationId());
		ourLkTxf.setCreateBy(SAFRApplication.getUserSession().getUser().getUserid());
		ourLkTxf.setCreateTime(new Date());
		ourLkTxf.setModifyBy("");
		ourLkTxf.setModifyTime(null);
		ourLkTxf.setId(maxid + 1);
		saveLookup();
		return ourLkTxf;
	}

	private void saveLookup() {
		Path lksPath = YAMLizer.getLookupsPath();
		lksPath.toFile().mkdirs();
		Path lkPath = lksPath.resolve(ourLkTxf.getName()+ ".yaml");
		YAMLizer.writeYaml(lkPath, ourLkTxf);
	}

	/**
	 * This method is to update the Lookup Path in LOOKUP
	 * 
	 * @param lkuptrans
	 *            : The transfer object which contains the values which are to
	 *            be set in the columns for the corresponding Lookup Path which
	 *            is being updated.
	 * @return The transfer object which contains the values which are received
	 *         from the LOOKUP for the Lookup Path which is updated.
	 * @throws DAOException
	 * @throws SAFRNotFoundException
	 */

	private LookupPathTransfer updateLookupPath(LookupPathTransfer lkuptrans) throws DAOException, SAFRNotFoundException {
		ourLkTxf  = new YAMLLookupTransfer();
		ourLkTxf.setEnvironmentId(SAFRApplication.getUserSession().getEnvironment().getId());
		ourLkTxf.setName(lkuptrans.getName());
		ourLkTxf.setId(lkuptrans.getId());
		ourLkTxf.setValidInd(lkuptrans.isValidInd());
		ourLkTxf.setSourceLRId(lkuptrans.getSourceLRId());
		ComponentAssociationTransfer lrlfa =  DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLRLFAssociation(lkuptrans.getTargetXLRFileId(), 0);
		ourLkTxf.setTargetLF(lrlfa.getAssociatedComponentId());
		ourLkTxf.setTargetLR(lrlfa.getAssociatingComponentId());
		ourLkTxf.setTargetXLRFileId(lrlfa.getAssociationId());
		ourLkTxf.setCreateBy(lkuptrans.getCreateBy());
		ourLkTxf.setCreateTime(lkuptrans.getCreateTime());
		ourLkTxf.setModifyBy(SAFRApplication.getUserSession().getUser().getUserid());
		ourLkTxf.setModifyTime(new Date());
		saveLookup();
		return ourLkTxf;
	}

	public void removeLookupPath(Integer id, Integer environmentId) throws DAOException {
	}

	public LookupPathTransfer getDuplicateLookupPath(String lookupName,	Integer lookupId, Integer environmentId) throws DAOException {
		Path lksPath = YAMLizer.getLookupsPath();
		Path lkPath = lksPath.resolve(lookupName + ".yaml");
		LookupPathTransfer result = (LookupPathTransfer) YAMLizer.readYaml(lkPath, ComponentType.LookupPath);
		if(result != null) {
			if(result.getId() != lookupId) { 
				logger.info("Existing Lookup with name '" + lookupName
						+ "' found in Environment  [" + environmentId + "]");
			} else {
				result = null; //if the id equals this must be an update				
			}
		}
		return result;
	}

	public List<DependentComponentTransfer> getLookupPathViewDependencies(
			Integer environmentId, Integer lookupPathId,
			Set<Integer> exceptionList) throws DAOException {
		List<DependentComponentTransfer> dependentViews = new ArrayList<DependentComponentTransfer>();
		return dependentViews;
	}

	public List<DependentComponentTransfer> getLookupPathInactiveLogicalRecordsDependencies(
			Integer environmentId, Integer lookupPathId) throws DAOException {
		List<DependentComponentTransfer> inactiveLogicalRecords = new ArrayList<DependentComponentTransfer>();
		return inactiveLogicalRecords;
	}

	public void makeLookupPathsActive(List<Integer> lookupPathIds, Integer environmentId) throws DAOException {
//		if (lookupPathIds == null || lookupPathIds.isEmpty()) {
//			return;
//		}
//		String placeholders = generator.getPlaceholders(lookupPathIds.size());
//		try {
//			String statementLookups = "Update "
//					+ params.getSchema()
//					+ ".LOOKUP Set VALIDIND = 1, LASTACTTIMESTAMP = CURRENT_TIMESTAMP, LASTACTUSERID = ? "
//					+ "Where LOOKUPID IN (" + placeholders + " ) AND ENVIRONID = ? ";
//			PreparedStatement pst = null;
//
//			while (true) {
//				try {
//					pst = con.prepareStatement(statementLookups);
//					pst.setString(1, safrLogin.getUserId());
//					int ndx = 2;
//					for(int i=0; i<lookupPathIds.size(); i++) {
//						pst.setInt(ndx++, lookupPathIds.get(i));
//					}
//                    pst.setInt(ndx, environmentId );
//					pst.executeUpdate();
//					break;
//				} catch (SQLException se) {
//					if (con.isClosed()) {
//						// lost database connection, so reconnect and retry
//						con = DAOFactoryHolder.getDAOFactory().reconnect();
//					} else {
//						throw se;
//					}
//				}
//			}
//			pst.close();
//
//		} catch (SQLException e) {
//			throw DataUtilities.createDAOException(
//			    "Database error occurred while while making the Lookup Paths active.",e);
//		}
//
	}

	public void makeLookupPathsInactive(Collection<Integer> lookupPathIds,
			Integer environmentId) throws DAOException {
		if (lookupPathIds == null || lookupPathIds.isEmpty()) {
			return;
		}
		String placeholders = generator.getPlaceholders(lookupPathIds.size());
		try {
			String statementLookups = "Update "
					+ params.getSchema()+ ".LOOKUP "
					+ "Set VALIDIND = 0, LASTACTTIMESTAMP = CURRENT_TIMESTAMP, LASTACTUSERID = ? "
					+ "Where LOOKUPID IN (" + placeholders 
					+ ") AND ENVIRONID = ? " 
					+ " AND VALIDIND = 1";
			PreparedStatement pst = null;

			while (true) {
				try {
					pst = con.prepareStatement(statementLookups);
					pst.setString(1, safrLogin.getUserId());
					int ndx = 2;
					Iterator<Integer> lpi = lookupPathIds.iterator();
					while(lpi.hasNext()) {
						Integer lp = lpi.next();
						pst.setInt(ndx++, lp);
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
			throw DataUtilities.createDAOException(
			    "Database error occurred while while making the Lookup Paths inactive.",e);
		}

	}

	public LookupQueryBean queryLookupPath(Integer lookupPathId,
			Integer environmentId) throws DAOException {
		LookupQueryBean lookupPathQueryBean = null;
		try {
		    String selectString;
            boolean admin = SAFRApplication.getUserSession().isSystemAdministrator();           
            if (admin) {
                selectString = "Select A.LOOKUPID, A.NAME From "
					+ params.getSchema() + ".LOOKUP A Where A.LOOKUPID = ? "
					+ " AND A.ENVIRONID = ? ";
            }
            else {
                selectString = "Select A.LOOKUPID, A.NAME, B.RIGHTS "
                    + "From " + params.getSchema() + ".LOOKUP A "
                    + "LEFT OUTER JOIN " + params.getSchema() + ".SECLOOKUP B "
                    + "ON A.LOOKUPID = B.LOOKUPID AND A.ENVIRONID = B.ENVIRONID AND B.GROUPID= ? "
                    + "Where A.LOOKUPID = ? "
                    + "AND A.ENVIRONID = ? ";                
            }
			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					if(admin) {
						pst.setInt(1,  lookupPathId);
						pst.setInt(2,  environmentId);
					} else {
						pst.setInt(1,  SAFRApplication.getUserSession().getGroup().getId());
						pst.setInt(2,  lookupPathId);
						pst.setInt(3,  environmentId);
					}
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
				lookupPathQueryBean = new LookupQueryBean(environmentId, 
				    rs.getInt("LOOKUPID"), 
				    DataUtilities.trimString(rs.getString("NAME")), 
				    null, 0, 0, null, null, 
                    admin ? EditRights.ReadModifyDelete : SAFRApplication.getUserSession().getEditRights(
                        rs.getInt("RIGHTS"), ComponentType.LookupPath, environmentId),
				    null, null, null, null, null, null);

			}
			pst.close();
			rs.close();
		} catch (SQLException e) {
			throw DataUtilities.createDAOException(
			    "Database error occurred while querying the Lookup Path with the specified ID.",e);
		}
		return lookupPathQueryBean;
	}

	public Boolean isSameTarget(List<Integer> lookupPathIds,
			Integer environmentId) throws DAOException {
		Boolean result = false;
		if (lookupPathIds == null || lookupPathIds.size() == 0) {
			return result;
		}
		try {
			String placeholders = generator.getPlaceholders(lookupPathIds.size());
			String selectString = "Select COUNT(DISTINCT DESTLRLFASSOCID) From "
					+ params.getSchema() + ".LOOKUP Where LOOKUPID IN ("
					+ placeholders + " ) AND ENVIRONID = ? ";
			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					int ndx = 1;
					for(int i=0; i<lookupPathIds.size(); i++){
						pst.setInt(ndx++,  lookupPathIds.get(i));
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
			if (rs.next()) {
				if (rs.getInt(1) == 1) {
					result = true;
				}
			}
			pst.close();
			rs.close();
		} catch (SQLException e) {
			throw DataUtilities.createDAOException("Database error occurred while checking if all the Lookup Paths are using the same LR-LF association as their target.",e);
		}
		return result;
	}

	@Override
	public Integer getLookupPath(String name, Integer environID) {
		Integer result = null;
		try {
			List<String> idNames = new ArrayList<String>();
			idNames.add(COL_NAME);
			idNames.add(COL_ENVID);

			String selectString = generator.getSelectStatement(params
					.getSchema(), TABLE_NAME, idNames, null);

			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					int i = 1;
					pst.setString(i++, name);
					pst.setInt(i++, environID);
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
				result = rs.getInt(2); //Note the lookup id is in the second column
			} else {
				logger.info("No such Lookup in Env " + environID + " with name : " + name);
			}
			pst.close();
			rs.close();
		} catch (SQLException e) {
			throw DataUtilities.createDAOException("Database error occurred while retrieving the Lookup Path " + name, e);
		}
		return result;
	}

	@Override
	public Map<String, Integer> getTargetFields(String name, Integer environID) {
		try {
			Map<String, Integer> fields = new TreeMap<>();
			String schema = params.getSchema();
			String selectString = "select l.name, l.lookupid, f.LRFIELDID, f.NAME "
					+ "from " + schema + ".lookup l "
					+ "join " + schema + ".lrlfassoc a "
					+ "on a.environid=l.environid and l.destlrlfassocid=a.lrlfassocid "
					+ "join " + schema + ".LRFIELD f "
					+ "on l.environid=f.environid and a.logrecid=f.logrecid "
					+ "where l.name= ? and f.environid= ?" ;

			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					int i = 1;
					pst.setString(i++, name);
					pst.setInt(i++, environID);
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
			int row = 0;
			while (rs.next()) {
				if(row == 0) {
					fields.put("Lookup_ID", rs.getInt(2));
				}
				fields.put(rs.getString(4), rs.getInt(3));
				row++;
			}
			pst.close();
			rs.close();
			return fields;
		} catch (SQLException e) {
			String msg = "Database error occurred while retrieving Lookup " + name + " fields from Environment [" + environID + "].";
			throw DataUtilities.createDAOException(msg, e);
		}
	}

	public static YAMLLookupTransfer getCurrentLKTransfer() {
		return ourLkTxf;
	}

}
