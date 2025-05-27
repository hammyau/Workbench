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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.ibm.safr.we.data.dao.LogicalFileDAO;
import com.ibm.safr.we.data.transfer.ComponentAssociationTransfer;
import com.ibm.safr.we.data.transfer.DependentComponentTransfer;
import com.ibm.safr.we.data.transfer.FileAssociationTransfer;
import com.ibm.safr.we.data.transfer.LogicalFileTransfer;
import com.ibm.safr.we.data.transfer.PhysicalFileTransfer;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.YAMLDAOFactory;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLogicalFileTransfer;
import com.ibm.safr.we.model.SAFRApplication;
import com.ibm.safr.we.model.query.LogicalFileQueryBean;
import com.ibm.safr.we.model.query.PhysicalFileQueryBean;

public class YAMLLogicalFileDAO implements LogicalFileDAO {
	static transient Logger logger = Logger
			.getLogger("com.ibm.safr.we.internal.data.dao.PGLogicalFileDAO");

    private static final String TABLE_NAME = "LOGFILE";
	private static final String TABLE_LF_PF_ASSOC = "LFPFASSOC";
	private static final String COL_ENVID = "ENVIRONID";
	private static final String COL_ID = "LOGFILEID";
	private static final String COL_NAME = "NAME";
	private static final String COL_COMMENT = "COMMENTS";
	private static final String COL_CREATETIME = "CREATEDTIMESTAMP";
	private static final String COL_CREATEBY = "CREATEDUSERID";
	private static final String COL_MODIFYTIME = "LASTMODTIMESTAMP";
	private static final String COL_MODIFYBY = "LASTMODUSERID";

	private Connection con;
	private ConnectionParameters params;
	private UserSessionParameters safrLogin;
	private PGSQLGenerator generator = new PGSQLGenerator();

	private static YAMLLogicalFileTransfer lfRead;

	private static Map<Integer, LogicalFileQueryBean> lfBeans = new TreeMap<>();

	private static int maxid;
	
	private static YAMLLogicalFileTransfer ourTxf;

	private static Path lfPath;

	public YAMLLogicalFileDAO(Connection con, ConnectionParameters params,
			UserSessionParameters safrlogin) {
		this.con = con;
		this.params = params;
		this.safrLogin = safrlogin;
	}

	private LogicalFileTransfer generateTransfer(ResultSet rs)
			throws SQLException {
		LogicalFileTransfer logicalFileTransfer = new LogicalFileTransfer();
		logicalFileTransfer.setEnvironmentId(rs.getInt(COL_ENVID));
		logicalFileTransfer.setId(rs.getInt(COL_ID));
		logicalFileTransfer.setName(DataUtilities.trimString(rs
				.getString(COL_NAME)));
		logicalFileTransfer.setComments(DataUtilities.trimString(rs
				.getString(COL_COMMENT)));
		logicalFileTransfer.setCreateTime(rs.getDate(COL_CREATETIME));
		logicalFileTransfer.setCreateBy(DataUtilities.trimString(rs
				.getString(COL_CREATEBY)));
		logicalFileTransfer.setModifyTime(rs.getDate(COL_MODIFYTIME));
		logicalFileTransfer.setModifyBy(DataUtilities.trimString(rs
				.getString(COL_MODIFYBY)));

		return logicalFileTransfer;
	}

	public LogicalFileTransfer getDuplicateLogicalFile(String logicalFileName, Integer logicalFileId, Integer environmentId) throws DAOException {
		Path lfsPath = getLFsPath();
		Path lfPath = lfsPath.resolve(logicalFileName + ".yaml");
		LogicalFileTransfer result = (LogicalFileTransfer) YAMLizer.readYaml(lfPath, ComponentType.LogicalFile);
		if(result != null) {
			if(result.getId() != logicalFileId) { 
				logger.info("Existing Logical File with name '" + logicalFileName
						+ "' found in Environment  [" + environmentId + "]");
			} else {
				result = null; //if the id equals this must be an update				
			}
		}
		return result;
	}

	public LogicalFileTransfer getLogicalFile(Integer id, Integer environmentId) throws DAOException {
		Path lfsPath = getLFsPath();
		lfsPath.toFile().mkdirs();
		Path lfPath = lfsPath.resolve(lfBeans.get(id).getName()+".yaml");
		ourTxf = (YAMLLogicalFileTransfer) YAMLizer.readYaml(lfPath, ComponentType.LogicalFile);
		return ourTxf;
	}

	public LogicalFileTransfer getLogicalFile(String name, Integer environmentId) throws DAOException {
		LogicalFileTransfer result = null;
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
					pst.setString(1, name);
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
				result = generateTransfer(rs);
			} else {
				logger.info("No such Logical File in Env " + environmentId + " with id : " + name);
			}
			pst.close();
			rs.close();
		} catch (SQLException e) {
			throw DataUtilities.createDAOException(
					"Database error occurred while retrieving the Logical File with id ["+ name + "]", e);
		}
		return result;
	}

	public LogicalFileTransfer persistLogicalFile(LogicalFileTransfer logicalFileTransfer) throws DAOException,	SAFRNotFoundException {
		if (!logicalFileTransfer.isPersistent()) {
			return (createLogicalFile(logicalFileTransfer));
		} else {
			return (updateLogicalFile(logicalFileTransfer));
		}
	}

	private LogicalFileTransfer updateLogicalFile(LogicalFileTransfer logicalFileTransfer) throws DAOException,	SAFRNotFoundException {
		Path lfsPath = getLFsPath();
		Path lfPath = lfsPath.resolve(logicalFileTransfer.getName() + ".yaml");
		if(lfPath.toFile().exists()) {
			if(ourTxf == null || ourTxf.getId() != logicalFileTransfer.getId()) {
				ourTxf  = new YAMLLogicalFileTransfer();
			}
			ourTxf.setEnvironmentId(SAFRApplication.getUserSession().getEnvironment().getId());
			ourTxf.setName(logicalFileTransfer.getName());
			ourTxf.setId(logicalFileTransfer.getId());
			ourTxf.setComments(logicalFileTransfer.getComments());
			ourTxf.setModifyBy(SAFRApplication.getUserSession().getUser().getUserid());
			ourTxf.setModifyTime(new Date());
			YAMLizer.writeYaml(lfPath, ourTxf);
		} else {
			//SaveAs via name change
			createLogicalFile(logicalFileTransfer);
		}
		return (logicalFileTransfer);
	}

	private LogicalFileTransfer createLogicalFile(LogicalFileTransfer logicalFile) throws DAOException {
		Path lfsPath = getLFsPath();
		lfsPath.toFile().mkdirs();
		lfPath = lfsPath.resolve(logicalFile.getName() + ".yaml");
		ourTxf  = new YAMLLogicalFileTransfer();
		ourTxf.setEnvironmentId(SAFRApplication.getUserSession().getEnvironment().getId());
		ourTxf.setName(logicalFile.getName());
		ourTxf.setId(logicalFile.getId());
		ourTxf.setCreateBy(SAFRApplication.getUserSession().getUser().getUserid());
		ourTxf.setCreateTime(new Date());
		ourTxf.setModifyBy("");
		ourTxf.setModifyTime(null);
		ourTxf.setId(maxid + 1);
		return ourTxf;
	}

	private Object addToResults(List<LogicalFileQueryBean> result, File pf, Integer environmentId) {
		YAMLLogicalFileTransfer lft = (YAMLLogicalFileTransfer) YAMLizer.readYaml(pf.toPath(), ComponentType.LogicalFile);
		if(lft.getId() > maxid) {
			maxid = lft.getId();
		}
		LogicalFileQueryBean lfBean = new LogicalFileQueryBean(
				environmentId, lft.getId(), 
				lft.getName(),
				EditRights.ReadModifyDelete,
				lft.getCreateTime(), 
				lft.getCreateBy(), 
				lft.getModifyTime(), 
				lft.getModifyBy());
			result.add(lfBean);
			lfBeans.put(lft.getId(), lfBean);
		return lfBean;
	}

	public List<LogicalFileQueryBean> queryAllLogicalFiles(
			Integer environmentId, SortType sortType) throws DAOException {
		List<LogicalFileQueryBean> result = new ArrayList<LogicalFileQueryBean>();
		maxid = 0;
		Path lfsPath = getLFsPath();
		lfsPath.toFile().mkdirs();
		File[] lfs = lfsPath.toFile().listFiles();
		
		if(lfs.length > 0) {
			Stream.of(lfs)
		    	      .filter(file -> file.isFile())
		    	      .forEach(ex -> addToResults(result, ex, environmentId));
		}
		return result;
	}

	public void removeLogicalFile(Integer id, Integer environmentId)
			throws DAOException {
		Path lfsPath = getLFsPath();
		lfsPath.toFile().mkdirs();
		lfPath = lfsPath.resolve(lfBeans.get(id).getName() + ".yaml");
		lfPath.toFile().delete();
	}

	public List<FileAssociationTransfer> getAssociatedPhysicalFiles(Integer id, Integer environmentId) throws DAOException {
		List<FileAssociationTransfer> result = new ArrayList<FileAssociationTransfer>();
		Iterator<Entry<Integer, String>> pfi = ourTxf.getPfs().entrySet().iterator();
		int seq = 1;
		while(pfi.hasNext()) {
			Entry<Integer, String> pfe = pfi.next();
			FileAssociationTransfer fileAssociationTransfer = new FileAssociationTransfer();
			fileAssociationTransfer.setAssociatedComponentId(pfe.getKey());
			fileAssociationTransfer.setAssociatedComponentName(pfe.getValue());
			fileAssociationTransfer.setAssociatingComponentId(ourTxf.getId());
			fileAssociationTransfer.setAssociatingComponentName(ourTxf.getName());
			fileAssociationTransfer.setAssociationId(pfe.getKey());
			fileAssociationTransfer.setSequenceNo(seq);
			fileAssociationTransfer.setEnvironmentId(environmentId);
			fileAssociationTransfer.setAssociatedComponentRights(EditRights.ReadModifyDelete);
            fileAssociationTransfer.setCreateTime(ourTxf.getCreateTime());
            fileAssociationTransfer.setCreateBy(ourTxf.getCreateBy());
            fileAssociationTransfer.setModifyTime(ourTxf.getModifyTime());
            fileAssociationTransfer.setModifyBy(ourTxf.getModifyBy());             				
			result.add(fileAssociationTransfer);
		}
		return result;
	}

	public List<ComponentAssociationTransfer> getAssociatedLogicalRecords(
			Integer id, Integer environmentId) throws DAOException {
		List<ComponentAssociationTransfer> result = new ArrayList<ComponentAssociationTransfer>();
		return result;
	}

	public List<PhysicalFileQueryBean> queryPossiblePFAssociations(Integer environmentId, List<Integer> exceptionList) throws DAOException {

		List<PhysicalFileQueryBean> result = new ArrayList<PhysicalFileQueryBean>();
		//get PFs but ignore those in the exceptionList
		Path pfsPath = getPFsPath();
		pfsPath.toFile().mkdirs();
		File[] pfs = pfsPath.toFile().listFiles();
		
		if(pfs.length > 0) {
			Stream.of(pfs)
		    	      .filter(file -> file.isFile())
		    	      .forEach(pf -> getAssocs(result, pf, environmentId));
		}
		return result;
	}

	private Object getAssocs(List<PhysicalFileQueryBean> result, File pf, Integer environmentId) {
		PhysicalFileTransfer pft = (PhysicalFileTransfer) YAMLizer.readYaml(pf.toPath(), ComponentType.PhysicalFile);
		if(pft.getId() > maxid) {
			maxid = pft.getId();
		}
		PhysicalFileQueryBean pfBean = new PhysicalFileQueryBean();
		pfBean.setName(pft.getName());
		pfBean.setId(pft.getId()); 
		result.add(pfBean);
		return pfBean;
	}

	public List<FileAssociationTransfer> persistAssociatedPFs(
			List<FileAssociationTransfer> fileAssociationTransfers,
			Integer logicalFileId) throws DAOException {

		List<FileAssociationTransfer> associatedPFCreates = new ArrayList<FileAssociationTransfer>();
		List<FileAssociationTransfer> associatedPFUpdates = new ArrayList<FileAssociationTransfer>();

		for (FileAssociationTransfer associatePF : fileAssociationTransfers) {
			if (!associatePF.isPersistent()) {
				associatedPFCreates.add(associatePF);
			} else {
				associatedPFUpdates.add(associatePF);
			}
		}
		if (associatedPFCreates.size() > 0) {
			associatedPFCreates = createAssociatedPFs(associatedPFCreates, logicalFileId);
		}
		if (associatedPFUpdates.size() > 0) {
			associatedPFUpdates = updateAssociatedPFs(associatedPFUpdates);
		}
		fileAssociationTransfers = new ArrayList<FileAssociationTransfer>();
		fileAssociationTransfers.addAll(associatedPFCreates);
		fileAssociationTransfers.addAll(associatedPFUpdates);
		return fileAssociationTransfers;

	}

	private List<FileAssociationTransfer> createAssociatedPFs(List<FileAssociationTransfer> associatedPFCreates, Integer logicalFileId) throws DAOException {
		Path lfsPath = getLFsPath();
		Path lfPath = lfsPath.resolve(ourTxf.getName() + ".yaml");
		
		for (FileAssociationTransfer associatedPFtoCreate : associatedPFCreates) {
			ourTxf.addPF(associatedPFtoCreate.getAssociatedComponentId(), associatedPFtoCreate.getAssociatedComponentName());
		}
		YAMLizer.writeYaml(lfPath, ourTxf);
		return associatedPFCreates;
	}

	private List<FileAssociationTransfer> updateAssociatedPFs(List<FileAssociationTransfer> associatedPFUpdates) throws DAOException {
		Path lfsPath = getLFsPath();
		Path lfPath = lfsPath.resolve(ourTxf.getName() + ".yaml");
		
		for (FileAssociationTransfer associatedPFtoCreate : associatedPFUpdates) {
			ourTxf.addPF(associatedPFtoCreate.getAssociatedComponentId(), associatedPFtoCreate.getAssociatedComponentName());
		}
		YAMLizer.writeYaml(lfPath, ourTxf);
		return associatedPFUpdates;
	}

	public void deleteAssociatedPFs(Integer environmentId, List<Integer> deletionIds) throws DAOException {
		//Note the deletionIds are the association ids. Not the actual PF ids.
		Path lfsPath = getLFsPath();
		Path lfPath = lfsPath.resolve(ourTxf.getName() + ".yaml");
		for (Integer did : deletionIds) {
			ourTxf.removePF(did);
		}
		YAMLizer.writeYaml(lfPath, ourTxf);
	}

    public List<DependentComponentTransfer> getAssociatedLookupDependencies(
        Integer environmentId, Integer lfId) throws DAOException  {
        List<DependentComponentTransfer> dependentComponents = new ArrayList<DependentComponentTransfer>();
        return dependentComponents;
    }
        
	
	public List<DependentComponentTransfer> getAssociatedPFViewDependencies(Integer environmentId, Integer LFPFAssociationId) throws DAOException {
		List<DependentComponentTransfer> dependentComponents = new ArrayList<DependentComponentTransfer>();
		return dependentComponents;
	}

	public Map<Integer, List<DependentComponentTransfer>> getAssociatedPFViewDependencies(Integer environmentId, List<Integer> LFPFAssociationIds,
			List<Integer> exceptionList) throws DAOException {
		Map<Integer, List<DependentComponentTransfer>> nonExistentDependentComponents = new HashMap<Integer, List<DependentComponentTransfer>>();
		return nonExistentDependentComponents;
	}

	public FileAssociationTransfer getLFPFAssociation(Integer associationId,
			Integer environmentId) throws DAOException {
		FileAssociationTransfer transfer = new FileAssociationTransfer();

		try {
			String schema = params.getSchema();

			String selectString = "Select A.ENVIRONID, A.LFPFASSOCID, "
					+ "A.LOGFILEID, C.NAME AS LOGNAME, A.PHYFILEID, "
					+ "B.NAME AS PHYNAME, A.PARTSEQNBR, "
                    + "A.CREATEDTIMESTAMP, A.CREATEDUSERID, A.LASTMODTIMESTAMP, A.LASTMODUSERID "                    
					+ "From "
					+ schema
					+ ".LFPFASSOC A, "
					+ schema
					+ ".PHYFILE B, "
					+ schema
					+ ".LOGFILE C "
					+ "Where A.ENVIRONID = ? "
					+ " AND A.LFPFASSOCID = ? "
					+ " AND A.ENVIRONID = B.ENVIRONID AND A.PHYFILEID = B.PHYFILEID"
					+ " AND A.ENVIRONID = C.ENVIRONID AND A.LOGFILEID = C.LOGFILEID";

			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					pst.setInt(1, environmentId );
					pst.setInt(2, associationId );
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
				transfer.setEnvironmentId(rs.getInt("ENVIRONID"));
				transfer.setAssociationId(rs.getInt("LFPFASSOCID"));
				transfer.setAssociatingComponentId(rs.getInt("LOGFILEID"));
				transfer.setAssociatingComponentName(
				    DataUtilities.trimString(rs.getString("LOGNAME")));
				transfer.setAssociatedComponentId(rs.getInt("PHYFILEID"));
				transfer.setAssociatedComponentName(
				    DataUtilities.trimString(rs.getString("PHYNAME")));
				transfer.setSequenceNo(rs.getInt("PARTSEQNBR"));
				transfer.setCreateTime(rs.getDate(COL_CREATETIME));
				transfer.setCreateBy(DataUtilities.trimString(rs.getString(COL_CREATEBY)));
				transfer.setModifyTime(rs.getDate(COL_MODIFYTIME));
				transfer.setModifyBy(DataUtilities.trimString(rs.getString(COL_MODIFYBY)));
			} else {
				logger.info("No LogicalFile-PhysicalFile association found in the database with the ID: "+ associationId);
			}
			pst.close();
			rs.close();
			return transfer;

		} catch (SQLException e) {
			throw DataUtilities.createDAOException("Database error occurred retrieving the LF-PF association with the specified association ID.",e);
		}
	}

	public FileAssociationTransfer getLFPFAssociation(Integer logicalFileId,
			Integer physicalFileId, Integer environmentId) throws DAOException {
		FileAssociationTransfer transfer = null;

		try {
			String schema = params.getSchema();

			String selectString = "Select A.ENVIRONID, A.LFPFASSOCID, "
					+ "A.LOGFILEID, C.NAME AS LOGNAME, A.PHYFILEID, "
					+ "B.NAME AS PHYNAME, A.PARTSEQNBR, "
                    + "A.CREATEDTIMESTAMP, A.CREATEDUSERID, A.LASTMODTIMESTAMP, A.LASTMODUSERID "                    					
					+ "From "
					+ schema
					+ ".LFPFASSOC A, "
					+ schema
					+ ".PHYFILE B, "
					+ schema
					+ ".LOGFILE C "
					+ "Where A.ENVIRONID = ? "
					+ " AND A.LOGFILEID = ? "
					+ " AND A.PHYFILEID = ? "
					+ " AND A.ENVIRONID = B.ENVIRONID AND A.PHYFILEID = B.PHYFILEID"
					+ " AND A.ENVIRONID = C.ENVIRONID AND A.LOGFILEID = C.LOGFILEID";

			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					pst.setInt(1,  environmentId);
					pst.setInt(2,  logicalFileId);
					pst.setInt(3,  physicalFileId);
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
				transfer = new FileAssociationTransfer();
				transfer.setEnvironmentId(rs.getInt("ENVIRONID"));
				transfer.setAssociationId(rs.getInt("LFPFASSOCID"));
				transfer.setAssociatingComponentId(rs.getInt("LOGFILEID"));
				transfer.setAssociatingComponentName(
				    DataUtilities.trimString(rs.getString("LOGNAME")));
				transfer.setAssociatedComponentId(rs.getInt("PHYFILEID"));
				transfer.setAssociatedComponentName(
				    DataUtilities.trimString(rs.getString("PHYNAME")));
				transfer.setSequenceNo(rs.getInt("PARTSEQNBR"));
                transfer.setCreateTime(rs.getDate(COL_CREATETIME));
                transfer.setCreateBy(DataUtilities.trimString(rs.getString(COL_CREATEBY)));
                transfer.setModifyTime(rs.getDate(COL_MODIFYTIME));
                transfer.setModifyBy(DataUtilities.trimString(rs.getString(COL_MODIFYBY)));				
			} else {
				logger
						.info("No LogicalFile-PhysicalFile association found in Environment ["
								+ environmentId
								+ "] with parentfileid ["
								+ logicalFileId
								+ "] and childpartitionid ["
								+ physicalFileId + "]");
			}
			pst.close();
			rs.close();
			return transfer;

		} catch (SQLException e) {
			String msg = "Database error occurred retrieving the LogicalFile-PhysicalFile association from Environment ["
					+ environmentId
					+ "] with parentfileid ["
					+ logicalFileId
					+ "] and childpartitionid [" + physicalFileId + "]";
			throw DataUtilities.createDAOException(msg, e);
		}
	}

	public List<FileAssociationTransfer> getLFPFAssociations(
			Integer environmentId) throws DAOException {
		List<FileAssociationTransfer> result = new ArrayList<FileAssociationTransfer>();

		try {
            boolean admin = SAFRApplication.getUserSession().isSystemAdministrator(); 
			String schema = params.getSchema();

			String selectString = null;
			if (admin) {
				selectString = "Select B.PHYFILEID, C.NAME AS PHYNAME, B.LFPFASSOCID, B.LOGFILEID, A.NAME AS LOGNAME, "
						+ "B.PARTSEQNBR, B.ENVIRONID, "
	                    + "B.CREATEDTIMESTAMP, B.CREATEDUSERID, B.LASTMODTIMESTAMP, B.LASTMODUSERID "                                       						
						+ "From "
						+ schema
						+ ".LOGFILE A, "
						+ schema
						+ ".LFPFASSOC B, "
						+ schema
						+ ".PHYFILE C "
						+ "Where A.ENVIRONID = ? "
						+ " AND A.ENVIRONID = B.ENVIRONID AND A.LOGFILEID = B.LOGFILEID"
						+ " AND B.ENVIRONID = C.ENVIRONID AND B.PHYFILEID = C.PHYFILEID"
						+ " AND B.LFPFASSOCID <> 0" // exclude dummy rows
						+ " Order By B.LFPFASSOCID";
			} else {
				selectString = "Select B.PHYFILEID, C.NAME AS PHYNAME, B.LFPFASSOCID, B.LOGFILEID, A.NAME AS LOGNAME, "
						+ "B.PARTSEQNBR, B.ENVIRONID, D.RIGHTS, "
	                    + "B.CREATEDTIMESTAMP, B.CREATEDUSERID, B.LASTMODTIMESTAMP, B.LASTMODUSERID "                                       						
						+ "From "
						+ schema
						+ ".LOGFILE A INNER JOIN "
						+ schema
						+ ".LFPFASSOC B ON A.ENVIRONID = B.ENVIRONID AND A.LOGFILEID = B.LOGFILEID "
						+ "INNER JOIN "
						+ schema
						+ ".PHYFILE C ON B.ENVIRONID = C.ENVIRONID AND B.PHYFILEID = C.PHYFILEID LEFT OUTER JOIN "
						+ schema
						+ ".SECPHYFILE D ON C.ENVIRONID = D.ENVIRONID AND C.PHYFILEID = D.PHYFILEID"
						+ " AND D.GROUPID = "
						+ SAFRApplication.getUserSession().getGroup().getId()
						+ " Where A.ENVIRONID = ? "
						+ " AND B.LFPFASSOCID <> 0" // exclude dummy rows
						+ " Order By B.LFPFASSOCID";

			}
			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					pst.setInt(1, environmentId );
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
				FileAssociationTransfer fileAssociationTransfer = new FileAssociationTransfer();
				fileAssociationTransfer.setAssociatedComponentId(rs.getInt("PHYFILEID"));
				fileAssociationTransfer.setAssociatedComponentName(
				    DataUtilities.trimString(rs.getString("PHYNAME")));
				fileAssociationTransfer.setAssociatingComponentId(rs.getInt("LOGFILEID"));
				fileAssociationTransfer.setAssociatingComponentName(
				    DataUtilities.trimString(rs.getString("LOGNAME")));
				fileAssociationTransfer.setAssociationId(rs.getInt("LFPFASSOCID"));
				fileAssociationTransfer.setSequenceNo(rs.getInt("PARTSEQNBR"));
				fileAssociationTransfer.setEnvironmentId(rs.getInt("ENVIRONID"));
				fileAssociationTransfer.setAssociatedComponentRights(
				    admin ? EditRights.ReadModifyDelete : SAFRApplication.getUserSession().getEditRights(
				        rs.getInt("RIGHTS"), ComponentType.PhysicalFile, environmentId));
				fileAssociationTransfer.setCreateTime(rs.getDate(COL_CREATETIME));
				fileAssociationTransfer.setCreateBy(DataUtilities.trimString(rs.getString(COL_CREATEBY)));
				fileAssociationTransfer.setModifyTime(rs.getDate(COL_MODIFYTIME));
				fileAssociationTransfer.setModifyBy(DataUtilities.trimString(rs.getString(COL_MODIFYBY)));             
				
				result.add(fileAssociationTransfer);
			}
			pst.close();
			rs.close();
			return result;

		} catch (SQLException e) {
			String msg = "Database error occurred while retrieving all LF/PF associations from Environment ["
					+ environmentId + "]. ";
			throw DataUtilities.createDAOException(msg, e);
		}
	}
	
	public List<DependentComponentTransfer> getAssociatedLogicalRecordsWithOneAssociatedLF(
			Integer environmentId, Integer logicalFileId) throws DAOException {
		List<DependentComponentTransfer> dependencies = new ArrayList<DependentComponentTransfer>();
		return dependencies;
	}

	public Map<ComponentType, List<DependentComponentTransfer>> getDependencies(
			Integer environmentId, Integer logicalFileId) throws DAOException {
		Map<ComponentType, List<DependentComponentTransfer>> dependencies = new HashMap<ComponentType, List<DependentComponentTransfer>>();
		return dependencies;

	}

	@Override
	public Integer getLFPFAssocID(int environid, String lfName, String pfName) {
		Integer result = null;
		try {
			String schema = params.getSchema();

			String selectString = "select lfpfassocid from " + schema + ".lfpfassoc a"
					+ " join " + schema + ".logfile l on l.environid=a.environid and l.logfileid = a.logfileid"
					+ " join " + schema + ".phyfile p on a.environid=p.environid and p.phyfileid = a.phyfileid"
					+ " where l.environid = ? and l.name = ? and p.name = ?";

			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
					pst.setInt(1, environid);
					pst.setString(2, lfName);
					pst.setString(3, pfName);
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
			} else {
				logger
						.info("No LogicalFile-PhysicalFile association found in Environment ["
								+ environid
								+ "] with parentfileid "
								+ lfName
								+ "] and childpartitionid ["
								+ pfName + "]");
			}
			pst.close();
			rs.close();
			return result;

		} catch (SQLException e) {
			String msg = "No LogicalFile-PhysicalFile association found in Environment ["
					+ environid
					+ "] with parentfileid "
					+ lfName
					+ "] and childpartitionid ["
					+ pfName + "]";
			throw DataUtilities.createDAOException(msg, e);
		}
	}

	private Path getLFsPath() {
		return YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("lfs");
	}
	private Path getPFsPath() {
		return YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("pfs");
	}
}
