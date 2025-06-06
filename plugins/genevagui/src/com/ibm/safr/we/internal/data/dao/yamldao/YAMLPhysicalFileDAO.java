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
import java.util.Date;
import java.util.List;
import java.util.Map;
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
import com.ibm.safr.we.data.dao.PhysicalFileDAO;
import com.ibm.safr.we.data.transfer.ControlRecordTransfer;
import com.ibm.safr.we.data.transfer.DependentComponentTransfer;
import com.ibm.safr.we.data.transfer.FileAssociationTransfer;
import com.ibm.safr.we.data.transfer.PhysicalFileTransfer;
import com.ibm.safr.we.data.transfer.UserExitRoutineTransfer;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.YAMLDAOFactory;
import com.ibm.safr.we.model.SAFRApplication;
import com.ibm.safr.we.model.query.PhysicalFileQueryBean;
import com.ibm.safr.we.model.query.UserExitRoutineQueryBean;

/**
 * This class is used to implement the unimplemented methods of
 * <b>PhysicaFileDAO</b>. This class contains the methods related to Physical
 * File metadata component which requires database access.
 * 
 */

public class YAMLPhysicalFileDAO implements PhysicalFileDAO {

	static transient Logger logger = Logger
			.getLogger("com.ibm.safr.we.internal.data.dao.PGPhysicalFileDAO");

	private static final String TABLE_NAME = "PHYFILE";

	// Common fields
	private static final String COL_ENVID = "ENVIRONID";
	private static final String COL_ID = "PHYFILEID";
	private static final String COL_NAME = "NAME";
	private static final String COL_FILETYPE = "FILETYPECD";
    private static final String COL_DISKFILETYPE = "DISKFILETYPECD";
	private static final String COL_ACCESSMETHOD = "ACCESSMETHODCD";
	private static final String COL_EXITID = "READEXITID";
	private static final String COL_EXITPARAM = "READEXITSTARTUP";
	private static final String COL_COMMENT = "COMMENTS";
	private static final String COL_CREATETIME = "CREATEDTIMESTAMP";
	private static final String COL_CREATEBY = "CREATEDUSERID";
	private static final String COL_MODIFYTIME = "LASTMODTIMESTAMP";
	private static final String COL_MODIFYBY = "LASTMODUSERID";

	// SQL fields
    private static final String COL_DBMSSUBSYS = "DBMSSUBSYS";
	private static final String COL_DBMSTABLE = "DBMSTABLE";
	private static final String COL_DBMSROWFMTCD = "DBMSROWFMTCD";
	private static final String COL_DBMSINCLNULLSIND = "DBMSINCLNULLSIND";
	private static final String COL_DBMSSQL = "DBMSSQL";

	// DataSet fields
	private static final String COL_DDNAMEINPUT = "DDNAMEINPUT";
    private static final String COL_DSN = "DSN";
    private static final String COL_MINRECORDLENGTH = "MINRECLEN";
    private static final String COL_MAXRECORDLENGTH = "MAXRECLEN";
    
    private static final String COL_DDNAMEOUTPUT = "DDNAMEOUTPUT";
    private static final String COL_LRECL = "LRECL";
	private static final String COL_RECFM = "RECFM";

	private Connection con;
	private ConnectionParameters params;
	private UserSessionParameters safrLogin;
	private PGSQLGenerator generator = new PGSQLGenerator();

	private static int maxid;
	private static Map<Integer, PhysicalFileQueryBean> pfBeans = new TreeMap<>();

	/**
	 * Constructor for this class
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
	public YAMLPhysicalFileDAO(Connection con, ConnectionParameters params,
			UserSessionParameters safrLogin) {
		this.con = con;
		this.params = params;
		this.safrLogin = safrLogin;
	}

	public List<PhysicalFileQueryBean> queryAllPhysicalFiles(Integer environmentId, SortType sortType) throws DAOException {
		List<PhysicalFileQueryBean> result = new ArrayList<PhysicalFileQueryBean>();
		maxid = 0;
		Path pfsPath = YAMLizer.getPFsPath();
		pfsPath.toFile().mkdirs();
		File[] pfs = pfsPath.toFile().listFiles();
		
		if(pfs.length > 0) {
			Stream.of(pfs)
		    	      .filter(file -> file.isFile())
		    	      .forEach(pf -> addToResults(result, pf, environmentId));
		}
		return result;
	}

	private Object addToResults(List<PhysicalFileQueryBean> result, File pf, Integer environmentId) {
		PhysicalFileTransfer pft = (PhysicalFileTransfer) YAMLizer.readYaml(pf.toPath(), ComponentType.PhysicalFile);
		if(pft.getId() > maxid) {
			maxid = pft.getId();
		}
		PhysicalFileQueryBean pfBean = new PhysicalFileQueryBean(
				environmentId, pft.getId(), 
				pft.getName(),
				pft.getFileTypeCode(),
				pft.getDiskFileTypeCode(),
				pft.getAccessMethodCode(),
				pft.getInputDDName(),
				pft.getDatasetName(),
				pft.getOutputDDName(),
				EditRights.ReadModifyDelete,
				pft.getCreateTime(), 
				pft.getCreateBy(), 
				pft.getModifyTime(), 
				pft.getModifyBy());
			result.add(pfBean);
			pfBeans.put(pft.getId(), pfBean);
		return pfBean;
	}
	

	public PhysicalFileTransfer getPhysicalFile(Integer id,	Integer environmentId) throws DAOException {
		PhysicalFileTransfer result = null;
		Path pfsPath = YAMLizer.getPFsPath();
		pfsPath.toFile().mkdirs();
		if(pfBeans.size() == 0) {
			queryAllPhysicalFiles(environmentId, null);
		}
		Path crPath = pfsPath.resolve(pfBeans.get(id).getName()+".yaml");
		result = (PhysicalFileTransfer) YAMLizer.readYaml(crPath, ComponentType.PhysicalFile);
		return result;
	}

	public PhysicalFileTransfer getPhysicalFile(String name, Integer environmentId) throws DAOException {
		PhysicalFileTransfer result = null;
		Path pfsPath = YAMLizer.getPFsPath();
		pfsPath.toFile().mkdirs();
		Path crPath = pfsPath.resolve(name+".yaml");
		result = (PhysicalFileTransfer) YAMLizer.readYaml(crPath, ComponentType.PhysicalFile);
		return result;
	}

	/**
	 * This function is used to generate a transfer object for the Physical
	 * File.
	 * 
	 * @param rs
	 *            : The result set of a database query run on PHYFILE
	 *            table with which the values for the transfer objects are set.
	 * @return A transfer object for the Physical File with values set according
	 *         to the result set.
	 * @throws SQLException
	 */
	private PhysicalFileTransfer generateTransfer(ResultSet rs)
			throws SQLException {
		PhysicalFileTransfer physicalFile = new PhysicalFileTransfer();
		physicalFile.setEnvironmentId(rs.getInt(COL_ENVID));
		physicalFile.setPartitionId(rs.getInt(COL_ID));
		physicalFile.setPartitionName(DataUtilities.trimString(rs.getString(COL_NAME)));
		physicalFile.setFileTypeCode(DataUtilities.trimString(rs.getString(COL_FILETYPE)));
        physicalFile.setDiskFileTypeCode(DataUtilities.trimString(rs.getString(COL_DISKFILETYPE)));
		physicalFile.setAccessMethodCode(DataUtilities.trimString(rs.getString(COL_ACCESSMETHOD)));
		physicalFile.setReadExitId(rs.getInt(COL_EXITID));
		if(rs.wasNull())  {
			physicalFile.setReadExitId(null);
		}
		physicalFile.setReadExitParams(DataUtilities.trimString(rs.getString(COL_EXITPARAM)));
		physicalFile.setComments(DataUtilities.trimString(rs.getString(COL_COMMENT)));
		physicalFile.setTableName(DataUtilities.trimString(rs.getString(COL_DBMSTABLE)));
		physicalFile.setRowFormatCode(DataUtilities.trimString(rs.getString(COL_DBMSROWFMTCD)));
		physicalFile.setIncludeNullIndicators(DataUtilities.intToBoolean(rs.getInt(COL_DBMSINCLNULLSIND)));
		physicalFile.setSqlStatement(DataUtilities.trimString(rs.getString(COL_DBMSSQL)));
		physicalFile.setSubSystem(DataUtilities.trimString(rs.getString(COL_DBMSSUBSYS)));
		physicalFile.setInputDDName(DataUtilities.trimString(rs.getString(COL_DDNAMEINPUT)));
        physicalFile.setDatasetName(DataUtilities.trimString(rs.getString(COL_DSN)));
		physicalFile.setMinRecordLen(rs.getInt(COL_MINRECORDLENGTH));
		physicalFile.setMaxRecordLen(rs.getInt(COL_MAXRECORDLENGTH));
		physicalFile.setOutputDDName(DataUtilities.trimString(rs.getString(COL_DDNAMEOUTPUT)));
		physicalFile.setRecfm(DataUtilities.trimString(rs.getString(COL_RECFM)));
		physicalFile.setLrecl(rs.getInt(COL_LRECL));
		physicalFile.setCreateTime(rs.getDate(COL_CREATETIME));
		physicalFile.setCreateBy(DataUtilities.trimString(rs.getString(COL_CREATEBY)));
		physicalFile.setModifyTime(rs.getDate(COL_MODIFYTIME));
		physicalFile.setModifyBy(DataUtilities.trimString(rs.getString(COL_MODIFYBY)));

		return physicalFile;
	}

	public PhysicalFileTransfer persistPhysicalFile(PhysicalFileTransfer physicalFile) throws DAOException,	SAFRNotFoundException {
		if (!physicalFile.isPersistent()) {
			return (createPhysicalFile(physicalFile));
		} else {
			return (updatePhysicalFile(physicalFile));
		}
	}

	/**
	 * This function is to create a Physical File in PHYFILE in database.
	 * 
	 * @param physicalFile
	 *            : The transfer object which contains the values which are to
	 *            be set in the fields for the corresponding Physical File which
	 *            is being created.
	 * @return The transfer object which contains the values which are received
	 *         from the PHYFILE for the Physical File which is created.
	 * @throws DAOException
	 */
	private PhysicalFileTransfer createPhysicalFile(PhysicalFileTransfer physicalFile) throws DAOException {
		Path pfsPath = YAMLizer.getPFsPath();
		pfsPath.toFile().mkdirs();
		Path pfPath = pfsPath.resolve(physicalFile.getName() + ".yaml");
		physicalFile.setCreateBy(SAFRApplication.getUserSession().getUser().getUserid());
		physicalFile.setCreateTime(new Date());
		physicalFile.setModifyBy("");
		physicalFile.setModifyTime(null);
		physicalFile.setId(maxid + 1);
		YAMLizer.writeYaml(pfPath, physicalFile);
		return physicalFile;
	}

	/**
	 * This function is to update a Physical File in PHYFILE in database.
	 * 
	 * @param physicalFile
	 *            : The transfer object which contains the values which are to
	 *            be set in the fields for the corresponding Physical File which
	 *            is being created.
	 * @return The transfer object which contains the values which are received
	 *         from the PHYFILE for the Physical File which is updated.
	 * @throws DAOException
	 * @throws SAFRNotFoundException
	 */
	private PhysicalFileTransfer updatePhysicalFile(PhysicalFileTransfer physicalFile) throws DAOException,	SAFRNotFoundException {
		Path pfsPath = YAMLizer.getPFsPath();
		Path pfPath = pfsPath.resolve(physicalFile.getName() + ".yaml");
		if(pfPath.toFile().exists()) {
			physicalFile.setModifyBy(SAFRApplication.getUserSession().getUser().getUserid());
			physicalFile.setModifyTime(new Date());
			YAMLizer.writeYaml(pfPath, physicalFile);
		} else {
			//SaveAs via name change
			createPhysicalFile(physicalFile);
		}
		return (physicalFile);
	}

	public PhysicalFileTransfer getDuplicatePhysicalFile(String physicalFileName, Integer physicalFileId, Integer environmentId) throws DAOException {
		Path pfsPath = YAMLizer.getPFsPath();
		Path pfPath = pfsPath.resolve(physicalFileName + ".yaml");
		PhysicalFileTransfer result = (PhysicalFileTransfer) YAMLizer.readYaml(pfPath, ComponentType.PhysicalFile);
		if(result != null) {
			if(result.getId() != physicalFileId) { 
				logger.info("Existing Physical File with name '" + physicalFileName
						+ "' found in Environment  [" + environmentId + "]");
			} else {
				result = null; //if the id equals this must be an update				
			}
		}
		return result;
	}

	public void removePhysicalFile(Integer id, Integer environmentId) throws DAOException {
		Path pfsPath = YAMLizer.getPFsPath();
		Path pfPath = pfsPath.resolve(pfBeans.get(id).getName()+".yaml");
		pfPath.toFile().delete();
	}

	public List<FileAssociationTransfer> getAssociatedLogicalFiles(Integer id,
			Integer environmentId) throws DAOException {
		List<FileAssociationTransfer> result = new ArrayList<FileAssociationTransfer>();
//
//		String selectString = "";
//		try {
//		    boolean admin = SAFRApplication.getUserSession().isSystemAdministrator();
//			String schema = params.getSchema();
//
//			if (admin) {
//				selectString = "Select B.LOGFILEID, C.NAME, B.LFPFASSOCID "
//						+ "From "
//						+ schema + ".PHYFILE A, "
//						+ schema + ".LFPFASSOC B, "
//						+ schema + ".LOGFILE C "
//						+ "Where A.ENVIRONID = ? "
//						+ " AND A.PHYFILEID = ? "
//						+ " AND A.ENVIRONID = B.ENVIRONID AND A.PHYFILEID = B.PHYFILEID"
//						+ " AND B.ENVIRONID = C.ENVIRONID AND B.LOGFILEID = C.LOGFILEID "
//						+ "Order By UPPER(C.NAME)";
//			} else {
//				selectString = "Select B.LOGFILEID, C.NAME, B.LFPFASSOCID, D.RIGHTS "
//						+ "From "
//						+ schema + ".PHYFILE A INNER JOIN "
//						+ schema + ".LFPFASSOC B ON A.ENVIRONID = B.ENVIRONID AND A.PHYFILEID = B.PHYFILEID INNER JOIN "
//						+ schema + ".LOGFILE C ON B.ENVIRONID = C.ENVIRONID AND B.LOGFILEID = C.LOGFILEID LEFT OUTER JOIN "
//						+ schema + ".SECLOGFILE D ON C.ENVIRONID = D.ENVIRONID AND C.LOGFILEID = D.LOGFILEID "
//						+ " AND D.GROUPID = ? "
//						+ " Where A.ENVIRONID = ?  "
//						+ " AND A.PHYFILEID = ? "
//						+ " Order By UPPER(C.NAME)";
//			}
//			PreparedStatement pst = null;
//			ResultSet rs = null;
//			while (true) {
//				try {
//					pst = con.prepareStatement(selectString);
//					int ndx = 1;
//					if (admin) {
//						pst.setInt(ndx++, environmentId);
//						pst.setInt(ndx++, id);
//					} else {
//						pst.setInt(ndx++,  SAFRApplication.getUserSession().getGroup().getId());
//						pst.setInt(ndx++, environmentId);
//						pst.setInt(ndx++, id);
//					}
//					rs = pst.executeQuery();
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
//			while (rs.next()) {
//				FileAssociationTransfer fileAssociationTransfer = new FileAssociationTransfer();
//				fileAssociationTransfer.setAssociatedComponentId(rs.getInt("LOGFILEID"));
//				fileAssociationTransfer.setAssociatedComponentName(DataUtilities.trimString(rs.getString("NAME")));
//				fileAssociationTransfer.setAssociationId(rs.getInt("LFPFASSOCID"));
//				fileAssociationTransfer.setAssociatedComponentRights(
//					admin ? EditRights.ReadModifyDelete : SAFRApplication.getUserSession().getEditRights(
//                        rs.getInt("RIGHTS"), ComponentType.LogicalFile, environmentId));
//				result.add(fileAssociationTransfer);
//			}
//			pst.close();
//			rs.close();
			return result;
//
//		} catch (SQLException e) {
//			throw DataUtilities.createDAOException(
//			    "Database error occurred while retrieving Logical Files associated with the Physical File.",e);
//		}
	}

	public List<DependentComponentTransfer> getViewDependencies(
			Integer environmentId, Integer physicalFileId) throws DAOException {
		List<DependentComponentTransfer> viewDependencies = new ArrayList<DependentComponentTransfer>();
//		try {
//			// Getting Views in which PF is used as output file.
//			String selectDependentPFs = "Select A.VIEWID, A.NAME From "
//					+ params.getSchema() + ".VIEW A,"
//					+ params.getSchema() + ".LFPFASSOC B "
//					+ "Where A.ENVIRONID =B.ENVIRONID "
//					+ "AND A.LFPFASSOCID=B.LFPFASSOCID "
//					+ "AND A.ENVIRONID =? AND B.PHYFILEID =? "
//					+ "AND B.PHYFILEID>0";
//
//			PreparedStatement pst = null;
//			ResultSet rs = null;
//			while (true) {
//				try {
//					pst = con.prepareStatement(selectDependentPFs);
//					pst.setInt(1, environmentId);
//					pst.setInt(2, physicalFileId);
//					rs = pst.executeQuery();
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
//			while (rs.next()) {
//				DependentComponentTransfer depCompTransfer = new DependentComponentTransfer();
//				depCompTransfer.setId(rs.getInt("VIEWID"));
//				depCompTransfer.setName(DataUtilities.trimString(rs.getString("NAME")));
//				depCompTransfer.setDependencyInfo("[View Properties]");
//				viewDependencies.add(depCompTransfer);
//			}
//
//			pst.close();
//			rs.close();
//
//			// Query for getting views in which PF is used in Logic Text
//			selectDependentPFs = "SELECT DISTINCT A.VIEWID, E.NAME, D.COLUMNNUMBER, D.HDRLINE1 FROM "
//					+ params.getSchema()
//					+ ".VIEWLOGICDEPEND A"
//					+ " INNER JOIN "
//					+ params.getSchema()
//					+ ".LFPFASSOC B ON A.LFPFASSOCID = B.LFPFASSOCID AND A.ENVIRONID=B.ENVIRONID"
//					+ " INNER JOIN "
//					+ params.getSchema()
//					+ ".VIEWCOLUMNSOURCE C ON A.PARENTID=C.VIEWCOLUMNSOURCEID AND A.ENVIRONID=C.ENVIRONID"
//					+ " INNER JOIN "
//					+ params.getSchema()
//					+ ".VIEWCOLUMN D ON C.VIEWCOLUMNID=D.VIEWCOLUMNID AND C.ENVIRONID=D.ENVIRONID"
//					+ " INNER JOIN "
//					+ params.getSchema()
//					+ ".VIEW E ON A.VIEWID=E.VIEWID AND A.ENVIRONID=E.ENVIRONID"
//					+ " WHERE A.ENVIRONID= ? AND B.PHYFILEID= ? AND B.PHYFILEID > 0 AND " 
//					+ "A.LOGICTYPECD = 2";
//
//			pst = null;
//			rs = null;
//			while (true) {
//				try {
//					pst = con.prepareStatement(selectDependentPFs);
//					pst.setInt(1, environmentId);
//					pst.setInt(2, physicalFileId);
//					rs = pst.executeQuery();
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
//
//			while (rs.next()) {
//				DependentComponentTransfer depCompTransfer = new DependentComponentTransfer();
//				depCompTransfer.setId(rs.getInt("VIEWID"));
//				depCompTransfer.setName(DataUtilities.trimString(rs.getString("NAME")));
//				String colName = "";
//				String heading1 = rs.getString("HDRLINE1");
//				if (heading1 != null && !heading1.equals("")) {
//					colName = ", " + heading1;
//				}
//				depCompTransfer.setDependencyInfo("[Col " + rs.getInt("COLUMNNUMBER") + colName + ", Logic Text]");
//				viewDependencies.add(depCompTransfer);
//			}
//
//			pst.close();
//			rs.close();
//			
//            // retrieving Views where this LF is used in Extract Output Logic Text.
//            selectDependentPFs = "SELECT DISTINCT A.VIEWID, D.NAME FROM " +
//                params.getSchema() + ".VIEWLOGICDEPEND A, " +
//                params.getSchema() + ".VIEWSOURCE B, " +
//                params.getSchema() + ".LFPFASSOC C, " +
//                params.getSchema() + ".PHYFILE D " +
//                "WHERE A.ENVIRONID=B.ENVIRONID " +
//                "AND A.PARENTID=B.VIEWSOURCEID " +
//                "AND A.ENVIRONID=C.ENVIRONID " +
//                "AND A.LFPFASSOCID=C.LFPFASSOCID " +
//                "AND C.ENVIRONID=D.ENVIRONID " +
//                "AND C.PHYFILEID=D.PHYFILEID " +
//                "AND A.LOGICTYPECD=5 " +
//                "AND D.ENVIRONID=?" +
//                "AND D.PHYFILEID=?";
//            while (true) {
//                try {
//                    pst = con.prepareStatement(selectDependentPFs);
//                    pst.setInt(1, environmentId);
//                    pst.setInt(2, physicalFileId);
//                    rs = pst.executeQuery();
//                    break;
//                } catch (SQLException se) {
//                    if (con.isClosed()) {
//                        // lost database connection, so reconnect and retry
//                        con = DAOFactoryHolder.getDAOFactory().reconnect();
//                    } else {
//                        rs.close();
//                        throw se;
//                    }
//                }
//            }
//            while (rs.next()) {
//                DependentComponentTransfer depCompTransfer = new DependentComponentTransfer();
//                depCompTransfer.setId(rs.getInt("VIEWID"));
//                depCompTransfer.setName(DataUtilities.trimString(rs.getString("NAME")));
//                depCompTransfer.setDependencyInfo("[View Output]");
//                viewDependencies.add(depCompTransfer);
//            }
//            pst.close();
//            rs.close();         
//			
//
//		} catch (SQLException e) {
//			throw DataUtilities.createDAOException("Database error occurred while retrieving View dependencies of Physical File.",e);
//		}
		return viewDependencies;
	}

	public List<DependentComponentTransfer> getAssociatedLogicalFilesWithOneAssociatedPF(
			Integer environmentId, Integer physicalFileId) throws DAOException {
		//For the moment lets jusr ignore thsee
		List<DependentComponentTransfer> dependencies = new ArrayList<DependentComponentTransfer>();
//		try {
//
//			String query = "SELECT A.PHYFILEID, A.LOGFILEID, C.NAME FROM "
//					+ params.getSchema() + ".LOGFILE C, "
//					+ params.getSchema() + ".LFPFASSOC A  "
//					+ "INNER JOIN (SELECT ENVIRONID,LOGFILEID FROM "
//					+ params.getSchema() + ".LFPFASSOC "
//					+ "GROUP BY LOGFILEID,ENVIRONID HAVING COUNT(LOGFILEID) = 1) B "
//					+ "ON B.LOGFILEID = A.LOGFILEID AND B.ENVIRONID=A.ENVIRONID "
//					+ "WHERE A.PHYFILEID=? AND A.ENVIRONID=C.ENVIRONID "
//					+ "AND A.ENVIRONID =? AND C.LOGFILEID = A.LOGFILEID";
//
//			PreparedStatement pst = null;
//			ResultSet rs = null;
//			while (true) {
//				try {
//					pst = con.prepareStatement(query);
//
//					pst.setInt(1, physicalFileId);
//					pst.setInt(2, environmentId);
//
//					rs = pst.executeQuery();
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
//
//			while (rs.next()) {
//				DependentComponentTransfer depCompTransfer = new DependentComponentTransfer();
//				depCompTransfer.setId(rs.getInt("LOGFILEID"));
//				depCompTransfer.setName(DataUtilities.trimString(rs.getString("NAME")));
//				dependencies.add(depCompTransfer);
//			}
//
//			pst.close();
//			rs.close();
//
//		} catch (SQLException e) {
//			throw DataUtilities.createDAOException("Database error occurred while retrieving Logical File dependencies of Physical File.",e);
//		}
//
		return dependencies;
	}

}
