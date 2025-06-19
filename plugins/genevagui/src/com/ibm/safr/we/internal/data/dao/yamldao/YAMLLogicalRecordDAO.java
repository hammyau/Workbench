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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.ibm.safr.we.constants.CodeCategories;
import com.ibm.safr.we.constants.Codes;
import com.ibm.safr.we.constants.ComponentType;
import com.ibm.safr.we.constants.EditRights;
import com.ibm.safr.we.constants.SortType;
import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DAOUOWInterruptedException;
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.LogicalRecordDAO;
import com.ibm.safr.we.data.transfer.ComponentAssociationTransfer;
import com.ibm.safr.we.data.transfer.DependentComponentTransfer;
import com.ibm.safr.we.data.transfer.LRIndexFieldTransfer;
import com.ibm.safr.we.data.transfer.LRIndexTransfer;
import com.ibm.safr.we.data.transfer.LogicalFileTransfer;
import com.ibm.safr.we.data.transfer.LogicalRecordTransfer;
import com.ibm.safr.we.data.transfer.PhysicalFileTransfer;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.YAMLDAOFactory;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLogicalFileTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLogicalRecordTransfer;
import com.ibm.safr.we.model.SAFRApplication;
import com.ibm.safr.we.model.query.LRIndexQueryBean;
import com.ibm.safr.we.model.query.LogicalFileQueryBean;
import com.ibm.safr.we.model.query.LogicalRecordQueryBean;
import com.ibm.safr.we.model.query.PhysicalFileQueryBean;

/**
 *This class is used to implement the unimplemented methods of
 * <b>LogicalRecordDAO</b>. This class contains the methods to related to
 * Logical Record metadata component which require database access.
 * 
 */
public class YAMLLogicalRecordDAO implements LogicalRecordDAO {

	static transient Logger logger = Logger.getLogger("com.ibm.safr.we.internal.data.dao.YAMLLogicalRecordDAO");

	private static final String TABLE_NAME = "LOGREC";
	private static final String COL_ENVID = "ENVIRONID";
	private static final String COL_ID = "LOGRECID";
	private static final String COL_NAME = "NAME";
	private static final String COL_TYPE = "LRTYPECD";
	private static final String COL_STATUS = "LRSTATUSCD";
	private static final String COL_EXIT = "LOOKUPEXITID";
	private static final String COL_STARTUP = "LOOKUPEXITSTARTUP";
	private static final String COL_COMMENTS = "COMMENTS";
	private static final String COL_CREATETIME = "CREATEDTIMESTAMP";
	private static final String COL_CREATEBY = "CREATEDUSERID";
	private static final String COL_MODIFYTIME = "LASTMODTIMESTAMP";
	private static final String COL_MODIFYBY = "LASTMODUSERID";
    private static final String COL_ACTIVATETIME = "LASTACTTIMESTAMP";
    private static final String COL_ACTIVATEBY = "LASTACTUSERID";

	// Constants for LR indexes
	private static final String TABLE_LRINDEX = "LRINDEX";
	private static final String COL_INDEXID = "LRINDEXID";
	private static final String COL_EFFSTARTDATEID = "EFFDATESTARTFLDID";
	private static final String COL_EFFENDDATEID = "EFFDATEENDFLDID";
	
	private static final String TABLE_LRINDEXFLD = "LRINDEXFLD";
	private static final String COL_INDEXFLDID = "LRINDEXFLDID";
	private static final String COL_SEQNO = "FLDSEQNBR";
	private static final String COL_LRFLDID = "LRFIELDID";

	private Connection con;
	private ConnectionParameters params;
	private UserSessionParameters safrLogin;
	private PGSQLGenerator generator = new PGSQLGenerator();
	
	private String[] indexNames = { 
			COL_ENVID,  
			COL_ID, 
			COL_EFFSTARTDATEID, 
			COL_EFFENDDATEID,
			COL_CREATETIME, 
			COL_CREATEBY, 
			COL_MODIFYTIME, 
			COL_MODIFYBY };

	private static int maxid;
	private static int maxLrLfAssocid = 1;
	private static Map<Integer, LogicalRecordQueryBean> lrBeans = new TreeMap<>();
	
	private static Map<Integer, List<ComponentAssociationTransfer>> ourLRLFAssociateionsByLR = new HashMap<>();
	private static Map<Integer, ComponentAssociationTransfer> ourLRLFAssociateionsById = new HashMap<>();
	
	private static YAMLLogicalRecordTransfer currentLRTxf;
	private static Map<Integer, YAMLLogicalRecordTransfer> lrTxfrsByID = new TreeMap<>();
	private static Map<String, YAMLLogicalRecordTransfer> lrTxfrsByName = new TreeMap<>();

	
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
	public YAMLLogicalRecordDAO(Connection con, ConnectionParameters params,
			UserSessionParameters safrLogin) {
		this.con = con;
		this.params = params;
		this.safrLogin = safrLogin;
		logger.info("New YAMLLogicalRecordDAO");
	}

	public LogicalRecordTransfer getLogicalRecord(Integer id, Integer environmentId) throws DAOException {
		currentLRTxf = null;
		if(id > 0) {
			LogicalRecordTransfer result = null;
			Path lrsPath = YAMLizer.getLRsPath();
			lrsPath.toFile().mkdirs();
			currentLRTxf = lrTxfrsByID.get(id);
			if(currentLRTxf == null) {
				queryAllLogicalRecords(environmentId, null);
				Path lrPath = lrsPath.resolve(lrBeans.get(id).getName()+".yaml");
				currentLRTxf = (YAMLLogicalRecordTransfer) YAMLizer.readYaml(lrPath, ComponentType.LogicalRecord);
				lrTxfrsByID.put(id, currentLRTxf);
				lrTxfrsByName.put(currentLRTxf.getName(), currentLRTxf);
				logger.info("Make LRTxf for LR:" + id);
			} else {
				logger.info("Found LRTxf for LR:" + id);
			}
		}
		return currentLRTxf;
	}

	public static LogicalRecordTransfer getLogicalRecord(String name, Integer environmentId) throws DAOException {	
		return currentLRTxf = lrTxfrsByName.get(name);
	}

	public List<LogicalRecordQueryBean> queryAllLogicalRecords(	Integer environmentId, SortType sortType) throws DAOException {
		List<LogicalRecordQueryBean> result = new ArrayList<LogicalRecordQueryBean>();
		if(lrBeans.isEmpty()) {
			logger.info("No Beans read the dir");
			maxid = 0;
			Path lrsPath = YAMLizer.getLRsPath();
			lrsPath.toFile().mkdirs();
			File[] lrs = lrsPath.toFile().listFiles();
			
			if(lrs.length > 0) {
				Stream.of(lrs)
			    	      .filter(file -> file.isFile())
			    	      .forEach(lr -> addToResults(result, lr, environmentId));
			}
		}
		result.addAll(lrBeans.values());
		return result;
	}

	private void addToResults(List<LogicalRecordQueryBean> result, File lf, Integer environmentId) {
		YAMLLogicalRecordTransfer lrt = (YAMLLogicalRecordTransfer) YAMLizer.readYaml(lf.toPath(), ComponentType.LogicalRecord);
		if(lrt.getId() > maxid) {
			maxid = lrt.getId();
		}
		LogicalRecordQueryBean lrBean = new LogicalRecordQueryBean(
				environmentId, lrt.getId(), 
				lrt.getName(),
				lrt.getLrStatusCode().toString(),
				getTotalLength(lrt), //total length query version uses MAX(B.FIXEDSTARTPOS+C.MAXLEN)-1 B and C field properties  
				getKeyLength(lrt), //key length
				lrt.getLrTypeCode(),
				EditRights.ReadModifyDelete,
				lrt.getCreateTime(), 
				lrt.getCreateBy(), 
				lrt.getModifyTime(), 
				lrt.getModifyBy(),
				lrt.getCreateTime(),
				lrt.getCreateBy()); 
		result.add(lrBean);
		lrBeans.put(lrt.getId(), lrBean);
		lrTxfrsByID.put(lrt.getId(), lrt);
		lrTxfrsByName.put(lrt.getName(), lrt);
		getAssociatedLogicalFiles(lrt.getId(), environmentId);
		logger.info("Beans Add LRTxf for LR:" + lrt.getId());
	}


	private Integer getKeyLength(YAMLLogicalRecordTransfer lrt) {
		 return lrt.getIndexes().stream().map(n -> lrt.getFields().get(n.getAssociatedComponentId()).getLength()).reduce(0, Integer::sum);
	}

	private Integer getTotalLength(YAMLLogicalRecordTransfer lrt) {	
		return lrt.getFields().values().stream().filter(f -> !f.isRedefinesInd()).map(f -> f.getLength()).reduce(0, Integer::sum);
	}

	public List<LogicalRecordQueryBean> queryAllActiveLogicalRecords(Integer environmentId, SortType sortType) throws DAOException {
		logger.info("queryAllActiveLogicalRecords");
		return queryAllLogicalRecords(environmentId, sortType);
	}

    public LogicalRecordTransfer persistLogicalRecord(LogicalRecordTransfer logicalRecordTransfer) throws DAOException,	SAFRNotFoundException {
		if (!logicalRecordTransfer.isPersistent()) {
			return (createLogicalRecord(logicalRecordTransfer));
		} else {
            return (updateLogicalRecord(logicalRecordTransfer));		    
		}
	}

    /**
	 * This function is used to create a Logical Record in LOGREC table
	 * 
	 * @param logicalRecordTransfer
	 *            : The transfer object which contains the values which are to
	 *            be set in the columns for the corresponding Logical Record
	 *            which is being created.
	 * @return The transfer object which contains the values which are received
	 *         from the LOGREC for the Logical Record which is created.
	 * @throws DAOException
	 */
	private LogicalRecordTransfer createLogicalRecord(LogicalRecordTransfer logicalRecord) throws DAOException {
		currentLRTxf  = new YAMLLogicalRecordTransfer();
		currentLRTxf.setEnvironmentId(SAFRApplication.getUserSession().getEnvironment().getId());
		currentLRTxf.setName(logicalRecord.getName());
		currentLRTxf.setId(logicalRecord.getId());
		currentLRTxf.setLrStatusCode(logicalRecord.getLrStatusCode());
		currentLRTxf.setLrTypeCode(logicalRecord.getLrTypeCode());
		currentLRTxf.setCreateBy(SAFRApplication.getUserSession().getUser().getUserid());
		currentLRTxf.setCreateTime(new Date());
		currentLRTxf.setModifyBy("");
		currentLRTxf.setModifyTime(null);
		currentLRTxf.setId(maxid + 1);
		saveLR();
		return currentLRTxf;
	}

	/**
	 * This function is used to update a Logical Record in LOGREC table
	 * 
	 * @param logicalRecordTransfer
	 *            : The transfer object which contains the values which are to
	 *            be set in the columns for the corresponding Logical Record
	 *            which is being updated.
	 * @return The transfer object which contains the values which are received
	 *         from the LOGREC for the Logical Record which is updated.
	 * @throws DAOException
	 * @throws SAFRNotFoundException
	 */
	private LogicalRecordTransfer updateLogicalRecord(LogicalRecordTransfer logicalRecordTransfer) throws DAOException,	SAFRNotFoundException {
		Path lrsPath = YAMLizer.getLRsPath();
		Path lrPath = lrsPath.resolve(logicalRecordTransfer.getName() + ".yaml");
		if(lrPath.toFile().exists()) {
			if(currentLRTxf == null || currentLRTxf.getId() != logicalRecordTransfer.getId()) {
				currentLRTxf  = new YAMLLogicalRecordTransfer();
			}
			currentLRTxf.setEnvironmentId(SAFRApplication.getUserSession().getEnvironment().getId());
			currentLRTxf.setName(logicalRecordTransfer.getName());
			currentLRTxf.setId(logicalRecordTransfer.getId());
			currentLRTxf.setLrStatusCode(logicalRecordTransfer.getLrStatusCode());
			currentLRTxf.setComments(logicalRecordTransfer.getComments());
			currentLRTxf.setModifyBy(SAFRApplication.getUserSession().getUser().getUserid());
			currentLRTxf.setModifyTime(new Date());
			YAMLizer.writeYaml(lrPath, currentLRTxf);
		} else {
			//SaveAs via name change
			createLogicalRecord(logicalRecordTransfer);
		}
		return (logicalRecordTransfer);
	}

	public void removeLogicalRecord(Integer id, Integer environmentId) throws DAOException {
		Path lrsPath = YAMLizer.getLRsPath();
		lrsPath.toFile().mkdirs();
		Path lrPath = lrsPath.resolve(lrBeans.get(id).getName() + ".yaml");
		lrPath.toFile().delete();
	}

	public LogicalRecordTransfer getDuplicateLogicalRecord(String logicalRecordName, Integer logicalRecordId, Integer environmentId) throws DAOException {
		Path lrsPath = YAMLizer.getLRsPath();
		Path lrPath = lrsPath.resolve(logicalRecordName + ".yaml");
		LogicalRecordTransfer result = (LogicalRecordTransfer) YAMLizer.readYaml(lrPath, ComponentType.LogicalRecord);
		if(result != null) {
			if(result.getId() != logicalRecordId) { 
				logger.info("Existing Logical Record with name '" + logicalRecordName
						+ "' found in Environment  [" + environmentId + "]");
			} else {
				result = null; //if the id equals this must be an update				
			}
		}
		return result;
	}

	public List<ComponentAssociationTransfer> getAssociatedLogicalFiles(Integer lrid, Integer environmentId) throws DAOException {
		if(lrTxfrsByID.isEmpty()) {
			logger.info("getAssociatedLogicalFiles queryAllLogicalRecords for LR:" + lrid);
			queryAllLogicalRecords(environmentId, null);
		}	
		List<ComponentAssociationTransfer> LRLFAssociateions = new ArrayList<ComponentAssociationTransfer>();
		currentLRTxf = lrTxfrsByID.get(lrid);
		if(currentLRTxf != null) {
			currentLRTxf.getLfs().entrySet().stream().forEach(lfe -> addToLfs(LRLFAssociateions, lfe, environmentId));
			ourLRLFAssociateionsByLR.put(lrid, LRLFAssociateions);
			logger.info("Current LR:" + currentLRTxf.getId());
		} else {
			logger.severe("Cannot find txfr for LR:" + lrid);
		}
		return LRLFAssociateions;
	}

	private void addToLfs(List<ComponentAssociationTransfer> result, Entry<Integer, String> lfe, Integer environmentId) {
		if(!ourLRLFAssociateionsById.containsKey(lfe.getKey())) {
			ComponentAssociationTransfer cat = new ComponentAssociationTransfer();
			cat.setAssociatedComponentId(lfe.getKey());
			cat.setAssociatedComponentName(lfe.getValue());
			cat.setAssociatingComponentId(currentLRTxf.getId());
			cat.setAssociatingComponentName(currentLRTxf.getName());
			cat.setAssociationId(maxLrLfAssocid++); //Use the lf id as the assoc id. Trick when want to delete
			logger.info("LRLF assoc:" + cat.getAssociationId() + " LR:" + cat.getAssociatingComponentId() + " LF:" + cat.getAssociatedComponentId());
			ourLRLFAssociateionsById.put(cat.getAssociationId(), cat);
			result.add(cat);
		} else {
			logger.info("Already have association for id:" + lfe.getKey());
			result.add(ourLRLFAssociateionsById.get(lfe.getKey()));
		}
	}

	public LRIndexTransfer getLRIndex(Integer lrId, Integer environmentId) throws DAOException {
		LRIndexTransfer result = null;
		return result;
	}

	// TODO  cannot find where getLRIndexes is being used. Code probably can be removed.
	public List<LRIndexTransfer> getLRIndexes(List<Integer> lrIndexIds,
			Integer environmentId) throws DAOException {
		String ids = null;
		List<LRIndexTransfer> result = new ArrayList<LRIndexTransfer>();

		try {
			String schema = params.getSchema();
			
			String placeholders = generator.getPlaceholders(lrIndexIds.size());
			String selectString = "Select ENVIRONID, LRINDEXID, LOGRECID "
					+ "From " + schema + ".LRINDEX "
					+ "Where ENVIRONID = ? ";
					if(lrIndexIds.size() > 0) {
						selectString +=  " AND LRINDEXID IN (" + placeholders + ") ";  
					}
					selectString +=   "ORDER BY LRINDEXID";
			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					int ndx = 1;
					pst.setInt(ndx++,  environmentId);
					for(int i=0; i<lrIndexIds.size();  i++) {
						pst.setInt(ndx++, lrIndexIds.get(i));
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
			while (rs.next()) {
				LRIndexTransfer trans = new LRIndexTransfer();
				trans.setEnvironmentId(rs.getInt("ENVIRONID"));
				trans.setId(rs.getInt("LRINDEXID"));
				trans.setLrId(rs.getInt("LOGRECID"));
				result.add(trans);
			}
			if (result.size() == 0) {
				logger.info("LRIndexes (" + ids + ") were not found in the database.");
			}
			pst.close();
			rs.close();
		} catch (SQLException e) {
			String msg = "Database error occurred while retrieving LR Indexes ("+ ids + ").";
			throw DataUtilities.createDAOException(msg, e);
		}
		return result;
	}
	
	public List<LRIndexFieldTransfer> getLRIndexFields(Integer lrIndexId,
			Integer environid) throws DAOException {
		List<LRIndexFieldTransfer> result = new ArrayList<LRIndexFieldTransfer>();
		try {
			String schema = params.getSchema();
			String selectString = "Select ENVIRONID, LRINDEXFLDID, LRINDEXID, LRFIELDID, "
					+ "CREATEDTIMESTAMP, CREATEDUSERID, LASTMODTIMESTAMP, LASTMODUSERID "
					+ "From " + schema + ".LRINDEXFLD "
					+ "Where ENVIRONID = ?"
					+ " AND LRINDEXID = ? ORDER BY LRINDEXFLDID";
			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					pst.setInt(1,  environid);
					pst.setInt(2,  lrIndexId);
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
				LRIndexFieldTransfer trans = new LRIndexFieldTransfer();
				trans.setEnvironmentId(rs.getInt("ENVIRONID"));
				trans.setAssociationId(rs.getInt("LRINDEXFLDID"));
				trans.setAssociatingComponentId(rs.getInt("LRINDEXID"));
				trans.setAssociatedComponentId(rs.getInt("LRFIELDID"));
				trans.setCreateTime(rs.getTimestamp("CREATEDTIMESTAMP"));
				trans.setCreateBy(rs.getString("CREATEDUSERID"));
				trans.setModifyTime(rs.getTimestamp("LASTMODTIMESTAMP"));
				trans.setModifyBy(rs.getString("LASTMODUSERID"));
				result.add(trans);
			}
			if (result.size() == 0) {
				logger.info("No LR Index Fields exist for LR Index ID ["
						+ lrIndexId + "], Environment ID [" + environid + "].");
			}
			pst.close();
			rs.close();
		} catch (SQLException e) {
			String msg = "Database error occurred while retrieving LR Index Fields "
					+ "for LR Index ID [" + lrIndexId + "], Environment ID [" + environid + "].";
			throw DataUtilities.createDAOException(msg, e);
		}
		return result;	
	}

	public LRIndexTransfer persistLRIndex(LRIndexTransfer lrIndexTransfer)
			throws DAOException {
		currentLRTxf.clearIndexes();
		currentLRTxf.setIndex(lrIndexTransfer);
		saveLR();
		return lrIndexTransfer;
	}

	public void removeLRIndex(Integer lrIndexId, Integer lrId, Integer environmentId) throws DAOException {
	}

	public void removeLRIndexForLR(Integer lrId, Integer environmentId)	throws DAOException {
	}

	public void persistLRIndexFields(List<LRIndexFieldTransfer> lrIndexFieldTransfers)			throws DAOException {
		currentLRTxf.clearIndexes();
		lrIndexFieldTransfers.stream().forEach(ndxf -> currentLRTxf.addIndex(ndxf));
		saveLR();
	}

	public void removeLRIndexFields(Integer lrIndexId, Integer environmentId) throws DAOException {
	}

	public void removeLRIndexFieldsForLR(Integer lrId, Integer environmentId) throws DAOException {
	}

	public List<LogicalFileQueryBean> queryPossibleLFAssociations(Integer environmentId, List<Integer>  notInParam) throws DAOException {

	List<LogicalFileQueryBean> result = new ArrayList<LogicalFileQueryBean>();
		//get PFs but ignore those in the exceptionList
		Path lfsPath = YAMLizer.getLFsPath();
		lfsPath.toFile().mkdirs();
		File[] lfs = lfsPath.toFile().listFiles();
		
		if(lfs.length > 0) {
			Stream.of(lfs)
		    	      .filter(file -> file.isFile())
		    	      .forEach(lf -> getLF(result, lf, environmentId));
		}
		return result;
	}

	private Object getLF(List<LogicalFileQueryBean> result, File lf, Integer environmentId) {
		LogicalFileTransfer lft = (LogicalFileTransfer) YAMLizer.readYaml(lf.toPath(), ComponentType.LogicalFile);
		if(lft.getId() > maxid) {
			maxid = lft.getId();
		}
		LogicalFileQueryBean lfBean = new LogicalFileQueryBean();
		lfBean.setName(lft.getName());
		lfBean.setId(lft.getId()); 
		result.add(lfBean);
		return lfBean;
	}

	public List<ComponentAssociationTransfer> persistAssociatedLF(List<ComponentAssociationTransfer> componentAssociationTransfers,
			Integer logicalRecordId) throws DAOException {
		currentLRTxf.clearLFs();
		for (ComponentAssociationTransfer associateLF : componentAssociationTransfers) {
			currentLRTxf.addLF(associateLF.getAssociatedComponentId(), associateLF.getAssociatedComponentName());
		}
		saveLR();
		return componentAssociationTransfers;

	}

	public void deleteAssociatedLF(Integer environmentId, List<Integer>  inList) throws DAOException {
		//The integer here is the association id. Not the lf id.
		//When we created the association used lf id as the assoc id.
		inList.stream().forEach(lf -> currentLRTxf.removeLF(lf));
		saveLR();
	}

	public Map<Integer, List<DependentComponentTransfer>> getFieldLookupDependencies(
			Integer environmentId, List<Integer>  fieldsToBeDeleted) throws DAOException {
		Map<Integer, List<DependentComponentTransfer>> lookupMap = new HashMap<Integer, List<DependentComponentTransfer>>();
		return lookupMap;
	}

	public Map<Integer, List<DependentComponentTransfer>> getFieldViewDependencies(
			Integer environmentId, List<Integer>  fieldsToBeDeleted) throws DAOException {

		Map<Integer, List<DependentComponentTransfer>> viewMap = new HashMap<Integer, List<DependentComponentTransfer>>();
		return viewMap;
	}

	public List<DependentComponentTransfer> getLRLookupDependencies(
			Integer environmentId, Integer logicalRecordId,
			Collection<Integer> exceptionList) throws DAOException {
		List<DependentComponentTransfer> depLookupsList = new ArrayList<DependentComponentTransfer>();
		return depLookupsList;
	}

	public List<DependentComponentTransfer> getLRViewDependencies(
			Integer environmentId, Integer logicalRecordId,
			Collection<Integer> lookupIdList, Collection<Integer> exceptionList)
			throws DAOException {
		List<DependentComponentTransfer> depViewsList = new ArrayList<DependentComponentTransfer>();
		return depViewsList;
	}

	public Map<Integer, List<DependentComponentTransfer>> getAssociatedLFLookupDependencies(
			Integer environmentId, List<Integer> LRLFAssociationIds)
			throws DAOException {

		Map<Integer, List<DependentComponentTransfer>> lookupMap = new HashMap<Integer, List<DependentComponentTransfer>>();
		return lookupMap;
	}

	public Map<Integer, List<DependentComponentTransfer>> getAssociatedLFViewDependencies(Integer environmentId, List<Integer> LRLFAssociationIds) throws DAOException {
		Map<Integer, List<DependentComponentTransfer>> viewMap = new HashMap<Integer, List<DependentComponentTransfer>>();
		return viewMap;
	}

	public LogicalRecordTransfer getLogicalRecordFromLRLFAssociation(Integer LRLFAssociationId, Integer environmentId) throws DAOException {
		ComponentAssociationTransfer lrlf = ourLRLFAssociateionsById.get(LRLFAssociationId);
		currentLRTxf =  lrTxfrsByID.get(lrlf.getAssociatingComponentId());
		return currentLRTxf;
	}

	public ComponentAssociationTransfer getTargetLogicalFileAssociation(Integer LRLFAssociationId, Integer environmentId) throws DAOException {
		return ourLRLFAssociateionsById.get(LRLFAssociationId);
	}

	public ComponentAssociationTransfer getLRLFAssociation(Integer associationId, Integer environmentId) throws DAOException {
		if(lrTxfrsByID.isEmpty()) {
			logger.info("getLRLFAssociation queryAllLogicalRecords for accoc:" + associationId);
			queryAllLogicalRecords(environmentId, null);
		}
		return ourLRLFAssociateionsById.get(associationId);
	}

	public ComponentAssociationTransfer getLRLFAssociation(String LRname, Integer environmentId) throws DAOException {
		if(lrTxfrsByID.isEmpty()) {
			queryAllLogicalRecords(environmentId, null);
		}
		YAMLLogicalRecordTransfer lrtxfr = lrTxfrsByName.get(LRname);
		Optional<ComponentAssociationTransfer> ct = ourLRLFAssociateionsByLR.get(lrtxfr.getId()).stream().filter(a -> a.getAssociatingComponentName() == LRname).findAny();
		return ct.isPresent()  ? ct.get() : null; //A Migration check
	}
	
	public ComponentAssociationTransfer getLRLFAssociation(String LRname, String LFname) throws DAOException {
		if(lrTxfrsByID.isEmpty()) {
			queryAllLogicalRecords(null, null);
		}
		YAMLLogicalRecordTransfer lrtxfr = lrTxfrsByName.get(LRname);
		Optional<ComponentAssociationTransfer> ct = ourLRLFAssociateionsByLR.get(lrtxfr.getId()).stream().filter(a -> a.getAssociatedComponentName().equals(LFname)).findAny();
		return ct.isPresent()  ? ct.get() : null; 
	}
	
	public ComponentAssociationTransfer getLRLFAssociation(Integer LRId, Integer LFId, Integer environmentId) throws DAOException {
		if(lrTxfrsByID.isEmpty()) {
			queryAllLogicalRecords(environmentId, null);
		}
		Optional<ComponentAssociationTransfer> ct = ourLRLFAssociateionsByLR.get(LRId).stream().filter(a -> a.getAssociatingComponentId() == LRId).findAny();
		return ct.isPresent()  ? ct.get() : null; //A Migration check
	}
	
	public List<ComponentAssociationTransfer> getLRLFAssociations(Integer environmentId) throws DAOException {
		List<ComponentAssociationTransfer> result = new ArrayList<ComponentAssociationTransfer>();
		return result; //An Import check thing
	}

	public LogicalRecordQueryBean queryLogicalRecordByField(Integer LRFieldId, Integer environmentId) throws DAOException {
		return lrBeans.get(currentLRTxf.getId());
	}

	public List<LRIndexQueryBean> queryLRIndexes(Integer environmentId)
			throws DAOException {
		List<LRIndexQueryBean> result = new ArrayList<LRIndexQueryBean>();

		try {
			String schema = params.getSchema();
			String selectString = "Select ENVIRONID, LRINDEXID, LOGRECID, "
					+ "CREATEDTIMESTAMP, CREATEDUSERID, LASTMODTIMESTAMP, LASTMODUSERID "
					+ "From "+ schema + ".LRINDEX "
					+ "Where ENVIRONID = ? "
					+ " ORDER BY LRINDEXID";
			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					pst.setInt(1, environmentId);
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
				LRIndexQueryBean lridx = new LRIndexQueryBean(rs
						.getInt("ENVIRONID"), rs.getInt("LRINDEXID"), rs
						.getInt("LOGRECID"), rs.getDate(COL_CREATETIME),
						DataUtilities.trimString(rs.getString(COL_CREATEBY)),
						rs.getDate(COL_MODIFYTIME), DataUtilities.trimString(rs
								.getString(COL_MODIFYBY)));
				result.add(lridx);
			}
			if (result.size() == 0) {
				logger.info("No LRIndexes were found in Environment [" + environmentId + "].");
			}
			pst.close();
			rs.close();
		} catch (SQLException e) {
			String msg = "Database error occurred while retrieving LR Indexes from Environment [" + environmentId + "].";
			throw DataUtilities.createDAOException(msg, e);
		}
		return result;
	}

    @Override
    public LogicalRecordQueryBean queryLogicalRecord(Integer lRID,
        Integer environmentId) {
        LogicalRecordQueryBean logicalRecordQueryBean = null;
        try {
            String selectString = "SELECT LOGRECID, NAME FROM "
                    + params.getSchema() + ".LOGREC  "
                    + "WHERE LOGRECID = ? "
                    + "AND ENVIRONID = ? ";
            PreparedStatement pst = null;
            ResultSet rs = null;
            while (true) {
                try {
                    pst = con.prepareStatement(selectString);
                    pst.setInt(1, lRID);
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
                logicalRecordQueryBean = new LogicalRecordQueryBean(
                    environmentId, rs.getInt("LOGRECID"), 
                    DataUtilities.trimString(rs.getString("NAME")), 
                    null, null, null, null, null, null, null, null, null, null, null);
            }
            pst.close();
            rs.close();
        } catch (SQLException e) {
            throw DataUtilities.createDAOException("Database error occurred while retrieving the Logical Record to which the Field with specified ID belongs.",e);
        }
        return logicalRecordQueryBean;
    }

	@Override
	public Integer getNextKey() {
        try {
            String statement = "SELECT nextval(pg_get_serial_sequence('" + params.getSchema() + 
                    ".logrec', 'logrecid'))";
            PreparedStatement pst = null;
            ResultSet rs = null;
            while (true) {
                try {
                    pst = con.prepareStatement(statement);
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
            rs.next();
            Integer result = rs.getInt(1);          
            rs.close();            
            pst.close();
            return result;
        } catch (SQLException e) {
            String msg = "Database error occurred while retrieving LR Field next id";
            throw DataUtilities.createDAOException(msg, e);
        }
    }

	public YAMLLogicalRecordTransfer getCurrentLRTransfer() {
		return currentLRTxf;
	}
	
	private void saveLR() {
		Path lrsPath = YAMLizer.getLRsPath();
		lrsPath.toFile().mkdirs();
		Path lrPath = lrsPath.resolve(currentLRTxf.getName()+ ".yaml");
		YAMLizer.writeYaml(lrPath, currentLRTxf);
	}
}
