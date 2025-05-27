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
import java.time.LocalDateTime;
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
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.ControlRecordDAO;
import com.ibm.safr.we.data.transfer.ControlRecordTransfer;
import com.ibm.safr.we.data.transfer.DependentComponentTransfer;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.YAMLDAOFactory;
import com.ibm.safr.we.model.SAFRApplication;
import com.ibm.safr.we.model.query.ControlRecordQueryBean;
import com.ibm.safr.we.model.query.EnvironmentQueryBean;

/**
 * This class is used to implement the unimplemented methods of
 * <b>ControlRecordDAO</b>. This class contains the methods to related to
 * Control Record metadata component which require database access.
 * 
 */
public class YAMLControlRecordDAO implements ControlRecordDAO {
	static transient Logger logger = Logger.getLogger("com.ibm.safr.we.internal.data.dao.YAMLControlRecordDAO");

	private static final String TABLE_NAME = "CONTROLREC";
	private static final String COL_ENVID = "ENVIRONID";
	private static final String COL_ID = "CONTROLRECID";
	private static final String COL_NAME = "NAME";
	private static final String COL_FIRSTMONTH = "FIRSTMONTH";
	private static final String COL_LOWVAL = "LOWVALUE";
	private static final String COL_HIGHVAL = "HIGHVALUE";
	private static final String COL_COMMENT = "COMMENTS";
	private static final String COL_CREATETIME = "CREATEDTIMESTAMP";
	private static final String COL_CREATEBY = "CREATEDUSERID";
	private static final String COL_MODIFYTIME = "LASTMODTIMESTAMP";
	private static final String COL_MODIFYBY = "LASTMODUSERID";

	private Connection con;
	private ConnectionParameters params;
	private UserSessionParameters safrLogin;
	private PGSQLGenerator generator = new PGSQLGenerator();
	private static Map<Integer, ControlRecordQueryBean> crsBeans = new TreeMap<>();
	private static int maxid = 0;

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
	public YAMLControlRecordDAO(Connection con, ConnectionParameters params,
			UserSessionParameters safrlogin) {
		this.con = con;
		this.params = params;
		this.safrLogin = safrlogin;
	}

	/*
	 * This function is used to generate a transfer object for the Control
	 * Record.
	 */
	private ControlRecordTransfer generateTransfer(ResultSet rs)
			throws SQLException {
		ControlRecordTransfer crec = new ControlRecordTransfer();
		crec.setEnvironmentId(rs.getInt(COL_ENVID));
		crec.setId(rs.getInt(COL_ID));
		crec.setName(DataUtilities.trimString(rs.getString(COL_NAME)));
		crec.setFirstFiscalMonth(rs.getInt(COL_FIRSTMONTH));
		crec.setBeginPeriod(rs.getInt(COL_LOWVAL));
		crec.setEndPeriod(rs.getInt(COL_HIGHVAL));
		crec.setComments(DataUtilities.trimString(rs.getString(COL_COMMENT)));
		crec.setCreateTime(rs.getDate(COL_CREATETIME));
		crec.setCreateBy(DataUtilities.trimString(rs.getString(COL_CREATEBY)));
		crec.setModifyTime(rs.getDate(COL_MODIFYTIME));
		crec.setModifyBy(DataUtilities.trimString(rs.getString(COL_MODIFYBY)));

		return crec;
	}

	public List<ControlRecordQueryBean> queryAllControlRecords(Integer environmentId, SortType sortType) throws DAOException {
		List<ControlRecordQueryBean> result = new ArrayList<ControlRecordQueryBean>();
		maxid = 0;
		Path defaultPath = YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		File[] crs = defaultPath.toFile().listFiles();
		
		if(crs.length > 0) {
			Stream.of(crs)
		    	      .filter(file -> file.isFile())
		    	      .forEach(cr -> addToResults(result, cr, environmentId));
		}
		return result;
	}

	private Object addToResults(List<ControlRecordQueryBean> result, File cr, Integer environmentId) {
		ControlRecordTransfer crt = (ControlRecordTransfer) YAMLizer.readYaml(cr.toPath(), ComponentType.ControlRecord);
		if(crt.getId() > maxid) {
			maxid = crt.getId();
		}
		ControlRecordQueryBean controlRecordBean = new ControlRecordQueryBean(
				environmentId,
				crt.getId(),
				crt.getName(),
				EditRights.ReadModifyDelete,
				crt.getCreateTime(), 
				crt.getCreateBy(), 
				crt.getModifyTime(), 
				crt.getModifyBy());
			result.add(controlRecordBean);
			crsBeans.put(crt.getId(), controlRecordBean);
		return controlRecordBean;
	}

	public ControlRecordTransfer getControlRecord(Integer id,
			Integer environmentId) throws DAOException {
		Path defaultPath = YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(crsBeans.get(id).getName()+".yaml");
		ControlRecordTransfer result = (ControlRecordTransfer) YAMLizer.readYaml(crPath, ComponentType.ControlRecord);
		return result;
	}

	public ControlRecordTransfer persistControlRecord(ControlRecordTransfer crec)
			throws DAOException, SAFRNotFoundException {
		if (!crec.isPersistent()) {
			return (createControlRecord(crec));
		} else {
			return (updateControlRecord(crec));
		}
	}

	/**
	 *This function is used to create a Control Record in CONTROLREC table
	 * 
	 * @param crec
	 *            : The transfer object which contains the values which are to
	 *            be set in the fields for the corresponding Control Record
	 *            which is being created.
	 * @return The transfer object which contains the values which are received
	 *         from the CONTROLREC for the Control Record which is created.
	 * @throws DAOException
	 */
	private ControlRecordTransfer createControlRecord(ControlRecordTransfer crec)
			throws DAOException {
		
		Path defaultPath = YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(crec.getName() + ".yaml");
		crec.setCreateBy(SAFRApplication.getUserSession().getUser().getUserid());
		crec.setCreateTime(new Date());
		crec.setId(maxid + 1);
		YAMLizer.writeYaml(crPath, crec);
		return crec;
	}

	/**
	 * This function is used to update a Control Record in CONTROLREC table.
	 * 
	 * @param crec
	 *            : The transfer object which contains the values which are to
	 *            be set in the fields for the corresponding Control Record
	 *            which is being updated.
	 * @return The transfer object which contains the values which are received
	 *         from the CONTROLREC for the Control Record which is updated
	 *         recently.
	 * @throws DAOException
	 * @throws SAFRNotFoundException
	 */
	private ControlRecordTransfer updateControlRecord(ControlRecordTransfer crec)
			throws DAOException, SAFRNotFoundException {
		//If modified the name then it's a saveAs?
		Path defaultPath = YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(crec.getName() + ".yaml");
		crec.setModifyBy(SAFRApplication.getUserSession().getUser().getUserid());
		crec.setModifyTime(new Date());
		YAMLizer.writeYaml(crPath, crec);
		return (crec);
	}

	public void removeControlRecord(Integer id, Integer environmentId)
			throws DAOException {
		Path defaultPath = YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(crsBeans.get(id).getName() + ".yaml");
		crPath.toFile().delete();
	}

	public ControlRecordTransfer getDuplicateControlRecord(String controlRecordName, Integer controlId, Integer environmentId) throws DAOException {
		Path defaultPath = YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(controlRecordName + ".yaml");
		ControlRecordTransfer result = (ControlRecordTransfer) YAMLizer.readYaml(crPath, ComponentType.ControlRecord);
		if(result != null) {
			if(result.getId() != controlId) { 
				logger.info("Existing Control Record with name '" + controlRecordName
						+ "' found in Environment  [" + environmentId + "]");
			} else {
				result = null; //if the id equals this must be an update				
			}
		}
		return result;
	}

	public List<DependentComponentTransfer> getControlRecordViewDependencies(
			Integer environmentId, Integer controlRecordId) throws DAOException {
		List<DependentComponentTransfer> dependencies = new ArrayList<DependentComponentTransfer>();
		//In YAML world this is not a relational database so no check?
		//We could maybe...
		return dependencies;

	}

	@Override
	public void deleteAllControlRecordsFromEnvironment(Integer environmentId) throws DAOException {
		try {
			List<String> idNames = new ArrayList<String>();
			idNames.add(COL_ENVID);

			String statement = generator.getDeleteStatement(params.getSchema(),
					TABLE_NAME, idNames);
			PreparedStatement pst = null;
			while (true) {
				try {
					pst = con.prepareStatement(statement);
					pst.setInt(1, environmentId);
					pst.execute();
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
			throw DataUtilities.createDAOException("Database error occurred while deleting the Control Record.",e);
		}
	}

}
