package com.ibm.safr.we.internal.data.dao.yamldao;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

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
import java.util.Date;
import java.util.Map;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.ibm.safr.we.constants.ComponentType;
import com.ibm.safr.we.constants.SortType;
import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.EnvironmentDAO;
import com.ibm.safr.we.data.transfer.EnvironmentTransfer;
import com.ibm.safr.we.data.transfer.GroupEnvironmentAssociationTransfer;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.YAMLDAOFactory;
import com.ibm.safr.we.model.query.EnvComponentQueryBean;
import com.ibm.safr.we.model.query.EnvironmentQueryBean;
import com.ibm.safr.we.model.query.GroupQueryBean;

/**
 * This class is used to implement the unimplemented methods of
 * <b>EnvironmentDAO</b>. This class contains the methods to related to
 * environment metadata component which requires database access.
 * 
 */
public class YAMLEnvironmentDAO implements EnvironmentDAO {

	static transient Logger logger = Logger.getLogger("com.ibm.safr.we.internal.data.dao.YAMLEnvironmentDAO");

	private static final String TABLE_NAME = "ENVIRON";
	private static final String COL_ID = "ENVIRONID";
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
	
	private static Map<Integer, EnvironmentQueryBean> environs = new TreeMap<>();

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
	public YAMLEnvironmentDAO(Connection con, ConnectionParameters params,
			UserSessionParameters safrLogin) {
		this.con = con;
		this.params = params;
		this.safrLogin = safrLogin;
	}

	public EnvironmentTransfer getEnvironment(Integer id) throws DAOException {
		return generateTransfer(id);
	}

	public EnvironmentTransfer getEnvironment(String name) throws DAOException {

		EnvironmentTransfer result = null;
		return result;
	}
	
	public List<EnvironmentQueryBean> queryAllEnvironments(SortType sortType)
			throws DAOException {
		List<EnvironmentQueryBean> result = new ArrayList<EnvironmentQueryBean>();

		File[] envs = YAMLDAOFactory.getGersHome().toFile().listFiles();
		if(envs.length > 0) {
			Stream.of(envs)
		    	      .filter(file -> file.isDirectory())
		    	      .forEach(env -> addToResults(result, env));
		} else {
			Path defaultPath = YAMLDAOFactory.getGersHome().resolve("DefaultYAML_Env");
			defaultPath.toFile().mkdirs();
			EnvironmentQueryBean environmentBean = new EnvironmentQueryBean(1, "DefaultYAML_Env", true, 
					new Date(),
					"me", 
					new Date(), "me");
			environs.put(1, environmentBean);
			result.add(environmentBean);
		}
		return result;
	}
	
	

	private Object addToResults(List<EnvironmentQueryBean> result, File env) {
		EnvironmentQueryBean environmentBean = null;
		try {
		    BasicFileAttributes attr = Files.readAttributes(env.toPath(), BasicFileAttributes.class);
		    FileTime fileTime = attr.creationTime();
		    Date crDate = new Date(fileTime.toMillis());
		    int id = environs.size()+1; 
			environmentBean = new EnvironmentQueryBean(id, env.getName(), true, 
					crDate,
					"me", 
					crDate, "me");
			environs.put(id, environmentBean);
			result.add(environmentBean);
		} catch (IOException ex) {
		    // handle exception
		}
		return environmentBean;
	}

	public List<EnvironmentQueryBean> queryAllEnvironments(SortType sortType,
			String userid, Boolean onlyWithEnvAdminRights) throws DAOException {
		//Won't be needed only a none admin call
		List<EnvironmentQueryBean> result = new ArrayList<EnvironmentQueryBean>();
		return result;
	}

	public List<EnvironmentQueryBean> queryEnvironmentsForGroup(
			SortType sortType, Integer groupId, Boolean onlyWithAdminRights)
			throws DAOException {
		//Won't be needed only a none admin call
		List<EnvironmentQueryBean> result = new ArrayList<EnvironmentQueryBean>();
		return result;
	}

	/**
	 * This function is used to generate a transfer object for the environment.
	 * @param id 
	 * 
	 * @param rs
	 *            : The result set of a database query run on ENVIRON table
	 *            with which the values for the transfer objects are set.
	 * @return a transfer object for the environment with values set according
	 *         to the result set.
	 * @throws SQLException
	 */
	private EnvironmentTransfer generateTransfer(Integer id)  {
		EnvironmentQueryBean envBean = environs.get(id);
		EnvironmentTransfer environment =   new EnvironmentTransfer();;
		if(envBean != null) {
			environment.setId(envBean.getId());
			environment.setName(envBean.getName());			
		} else {
			environment.setId(1);
			environment.setName("Default_YAML_Env");
			environment.setComments("");
		}
		return environment;
	}

	public EnvironmentTransfer persistEnvironment(
			EnvironmentTransfer environment) throws DAOException,
			SAFRNotFoundException {

		if (environment.getId() == 0) {
			return (createEnvironment(environment));
		} else {
			return (updateEnvironment(environment));
		}

	}

	/**
	 * This function is used to create an environment in ENVIRON table. It
	 * uses a stored procedure GP_INITVALUES to create the environment.
	 * 
	 * @param environment
	 *            : The transfer object which contains the values which are to
	 *            be set in the fields for the corresponding environment which
	 *            is being created.
	 * @return The transfer object which contains the values which are received
	 *         from the ENVIRON for the environment which is created.
	 * @throws DAOException
	 */
	private EnvironmentTransfer createEnvironment(
			EnvironmentTransfer environment) throws DAOException {
		//just a make dir
		return null;
	}

	/**
	 * This function is used to update an environment in ENVIRON table.
	 * 
	 * @param environment
	 *            :The transfer object which contains the values which are to be
	 *            set in the fields for the corresponding environment which is
	 *            being updated.
	 * @return The transfer object which contains the values which are received
	 *         from the ENVIRON for the environment which is updated.
	 * @throws DAOException
	 * @throws SAFRNotFoundException
	 */
	private EnvironmentTransfer updateEnvironment(EnvironmentTransfer environment) throws DAOException,	SAFRNotFoundException {
		//Not sure what this means
			return (environment);
	}

	public EnvironmentTransfer getDuplicateEnvironment(String name,	Integer environmentId) throws DAOException {
		EnvironmentTransfer result = null;
		return result;
	}

    public List<EnvironmentQueryBean> queryBALEnvironments(SortType sortType) throws DAOException {
        List<EnvironmentQueryBean> result = new ArrayList<EnvironmentQueryBean>();
        return result;
    }

    public List<EnvironmentQueryBean> queryBAVEnvironments(SortType sortType) throws DAOException {
    	//Not sure ever used
        List<EnvironmentQueryBean> result = new ArrayList<EnvironmentQueryBean>();
        return result;
    }
    
	public List<GroupEnvironmentAssociationTransfer> getAssociatedGroups(Integer environmentId, SortType sortType) throws DAOException {
		//No groups in YAMLland
		List<GroupEnvironmentAssociationTransfer> result = new ArrayList<GroupEnvironmentAssociationTransfer>();
		return result;
	}

	public void clearEnvironment(Integer environmentId) throws DAOException {
		//If we want to do this means recursively delete directory
	}

	public void removeEnvironment(Integer environmentId) throws DAOException {
		//If we want to do this means recursively delete directory
	}

	public Boolean hasDependencies(Integer environmentId) throws DAOException {
		//Means directory has stuff?
		return true;
	}

	public List<GroupQueryBean> queryPossibleGroupAssociations(
			List<Integer> associatedGroupIds) throws DAOException {
		//No groups in YAMLland
		List<GroupQueryBean> result = new ArrayList<GroupQueryBean>();
		return result;
	}

	public List<EnvComponentQueryBean> queryComponentEnvironments(Integer id, ComponentType type) throws DAOException {
		List<EnvComponentQueryBean> result = new ArrayList<EnvComponentQueryBean>();

		//Not sure this is needed... will end up as a directory search/list
		String selectString = "";
		switch (type) {
		case LogicalRecordField:
			selectString = "Select A." + COL_ID + ", A." + COL_NAME + ", B.NAME, B."
					+ COL_CREATETIME + ", B."+ COL_CREATEBY  
					+ ", B." + COL_MODIFYTIME + ", B."+ COL_MODIFYBY 
					+ " From " + params.getSchema() + "."
					+ TABLE_NAME + " A, " + params.getSchema() + ".LRFIELD B" 
					+ " Where A." + COL_ID + "=B." + COL_ID 
					+ " AND B.LRFIELDID=" +id.toString();		
			break;		
		case LookupPath:
			selectString = "Select A." + COL_ID + ", A." + COL_NAME + ", B.NAME, B."
					+ COL_CREATETIME + ", B."+ COL_CREATEBY  
					+ ", B." + COL_MODIFYTIME + ", B."+ COL_MODIFYBY 
					+ " From " + params.getSchema() + "."
					+ TABLE_NAME + " A, " + params.getSchema() + ".LOOKUP B" 
					+ " Where A." + COL_ID + "=B." + COL_ID 
					+ " AND B.LOOKUPID=" +id.toString();		
			break;		
		case LogicalFile:
			selectString = "Select A." + COL_ID + ", A." + COL_NAME + ", B.NAME, B."
					+ COL_CREATETIME + ", B."+ COL_CREATEBY  
					+ ", B." + COL_MODIFYTIME + ", B."+ COL_MODIFYBY 
					+ " From " + params.getSchema() + "."
					+ TABLE_NAME + " A, " + params.getSchema() + ".LOGFILE B" 
					+ " Where A." + COL_ID + "=B." + COL_ID 
					+ " AND B.LOGFILEID=" +id.toString();		
			break;
		case LogicalRecord:
			selectString = "Select A." + COL_ID + ", A." + COL_NAME + ", B.NAME, B."
					+ COL_CREATETIME + ", B."+ COL_CREATEBY  
					+ ", B." + COL_MODIFYTIME + ", B."+ COL_MODIFYBY 
					+ " From " + params.getSchema() + "."
					+ TABLE_NAME + " A, " + params.getSchema() + ".LOGREC B" 
					+ " Where A." + COL_ID + "=B." + COL_ID 
					+ " AND B.LOGRECID=" +id.toString();		
			break;
		case PhysicalFile:
			selectString = "Select A." + COL_ID + ", A." + COL_NAME + ", B.NAME, B."
					+ COL_CREATETIME + ", B."+ COL_CREATEBY  
					+ ", B." + COL_MODIFYTIME + ", B."+ COL_MODIFYBY 
					+ " From " + params.getSchema() + "."
					+ TABLE_NAME + " A, " + params.getSchema() + ".PHYFILE B" 
					+ " Where A." + COL_ID + "=B." + COL_ID 
					+ " AND B.PHYFILEID=" +id.toString();		
			break;
		case UserExitRoutine:
			selectString = "Select A." + COL_ID + ", A." + COL_NAME + ", B.NAME, B."
					+ COL_CREATETIME + ", B."+ COL_CREATEBY  
					+ ", B." + COL_MODIFYTIME + ", B."+ COL_MODIFYBY 
					+ " From " + params.getSchema() + "."
					+ TABLE_NAME + " A, " + params.getSchema() + ".EXIT B" 
					+ " Where A." + COL_ID + "=B." + COL_ID 
					+ " AND B.EXITID=" +id.toString();		
			break;		
		case View:
			selectString = "Select A." + COL_ID + ", A." + COL_NAME + ", B.NAME, B."
					+ COL_CREATETIME + ", B."+ COL_CREATEBY  
					+ ", B." + COL_MODIFYTIME + ", B."+ COL_MODIFYBY 
					+ " From " + params.getSchema() + "."
					+ TABLE_NAME + " A, " + params.getSchema() + ".VIEW B" 
					+ " Where A." + COL_ID + "=B." + COL_ID 
					+ " AND B.VIEWID=" +id.toString();		
			break;					
		default:
			return null;		
		}
		try {
			PreparedStatement pst = null;
			ResultSet rs = null;

			while (true) {
				try {
					pst = con.prepareStatement(selectString);
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
				int i = 1;
				EnvComponentQueryBean envCompBean = new EnvComponentQueryBean(
				        rs.getInt(i++), DataUtilities.trimString(rs.getString(i++)), 
				        DataUtilities.trimString(rs.getString(i++)), 
						rs.getDate(i++),DataUtilities.trimString(rs.getString(i++)), 
						rs.getDate(i++), DataUtilities.trimString(rs.getString(i++)));

				result.add(envCompBean);
			}
			pst.close();
			rs.close();
			return result;

		} catch (SQLException e) {
			throw DataUtilities.createDAOException(
					"Database error occurred while querying Components in Environments.",e);
		}
	}
}
