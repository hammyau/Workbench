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
import com.ibm.safr.we.data.dao.UserExitRoutineDAO;
import com.ibm.safr.we.data.transfer.ControlRecordTransfer;
import com.ibm.safr.we.data.transfer.DependentComponentTransfer;
import com.ibm.safr.we.data.transfer.UserExitRoutineTransfer;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.YAMLDAOFactory;
import com.ibm.safr.we.model.SAFRApplication;
import com.ibm.safr.we.model.query.ControlRecordQueryBean;
import com.ibm.safr.we.model.query.UserExitRoutineQueryBean;

/**
 * This class implements interface <b>UserExitRoutineDAO</b>. 
 * It provides the database access for model class UserExitRoutine.
 */
public class YAMLUserExitRoutineDAO implements UserExitRoutineDAO {

	static transient Logger logger = Logger
			.getLogger("com.ibm.safr.we.internal.data.dao.PGUserExitRoutineDAO");

	private static final String TABLE_NAME = "EXIT";
	
	private static final String COL_ENVID = "ENVIRONID";
	private static final String COL_ID = "EXITID";
    private static final String COL_NAME = "NAME";
    private static final String COL_EXECUTABLE = "MODULEID";
	private static final String COL_TYPE = "EXITTYPECD";
	private static final String COL_LANGUAGE = "PROGRAMTYPECD";
    private static final String COL_OPTIMIZED = "OPTIMIZEIND";
	private static final String COL_COMMENT = "COMMENTS";
	private static final String COL_CREATETIME = "CREATEDTIMESTAMP";
	private static final String COL_CREATEBY = "CREATEDUSERID";
	private static final String COL_MODIFYTIME = "LASTMODTIMESTAMP";
	private static final String COL_MODIFYBY = "LASTMODUSERID";
	private Connection con;
	private ConnectionParameters params;
	private UserSessionParameters safrLogin;

	private static int maxid;

	private static Map<Integer, UserExitRoutineQueryBean> exitBeans = new TreeMap<>();

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
	public YAMLUserExitRoutineDAO(Connection con, ConnectionParameters params, UserSessionParameters safrLogin) {
		this.con = con;
		this.params = params;
		this.safrLogin = safrLogin;
	}

	public UserExitRoutineTransfer getUserExitRoutine(Integer id, Integer environmentId) throws DAOException {
		Path exitsPath = YAMLizer.getExitsPath();
		exitsPath.toFile().mkdirs();
		Path crPath = exitsPath.resolve(exitBeans.get(id).getName()+".yaml");
		UserExitRoutineTransfer result = (UserExitRoutineTransfer) YAMLizer.readYaml(crPath, ComponentType.UserExitRoutine);
		return result;
	}

	private Object addToResults(List<UserExitRoutineQueryBean> result, File ex, Integer environmentId) {
		UserExitRoutineTransfer ext = (UserExitRoutineTransfer) YAMLizer.readYaml(ex.toPath(), ComponentType.UserExitRoutine);
		if(ext.getId() > maxid) {
			maxid = ext.getId();
		}
		UserExitRoutineQueryBean exitBean = new UserExitRoutineQueryBean(
				environmentId, ext.getId(), 
				ext.getName(),
				ext.getExecutable(),
				ext.getTypeCode(),
				ext.getLanguageCode(),
				EditRights.ReadModifyDelete, 
				ext.getCreateTime(), 
				ext.getCreateBy(), 
				ext.getModifyTime(), 
				ext.getModifyBy());
			result.add(exitBean);
			exitBeans.put(ext.getId(), exitBean);
		return exitBean;
	}

	public List<UserExitRoutineQueryBean> queryAllUserExitRoutines(Integer environmentId, SortType sortType) throws DAOException {
		List<UserExitRoutineQueryBean> result = new ArrayList<UserExitRoutineQueryBean>();
		maxid = 0;
		Path exitsPath = YAMLizer.getExitsPath();
		exitsPath.toFile().mkdirs();
		File[] exits = exitsPath.toFile().listFiles();
		
		if(exits.length > 0) {
			Stream.of(exits)
		    	      .filter(file -> file.isFile())
		    	      .forEach(ex -> addToResults(result, ex, environmentId));
		}
		return result;
	}

	public List<UserExitRoutineQueryBean> queryUserExitRoutines(Integer environmentId, String typeCodeKey, SortType sortType) throws DAOException {
		List<UserExitRoutineQueryBean> result = new ArrayList<UserExitRoutineQueryBean>();
		//Not convinced this is used
		return result;
	}

	/**
	 * This method is used to generate a transfer object for the User Exit
	 * Routine.
	 * 
	 * @param rs
	 *            The result set of a database query run on EXIT table
	 *            with which the values for the transfer objects are set.
	 * @return A transfer object for the User Exit Routine with values set
	 *         according to the result set.
	 * @throws SQLException
	 */
	private UserExitRoutineTransfer generateTransfer(ResultSet rs)
			throws SQLException {
		UserExitRoutineTransfer userExitRoutineTransfer = new UserExitRoutineTransfer();
		userExitRoutineTransfer.setEnvironmentId(rs.getInt(COL_ENVID));
		userExitRoutineTransfer.setId(rs.getInt(COL_ID));
		userExitRoutineTransfer.setName(DataUtilities.trimString(rs.getString(COL_NAME)));
		userExitRoutineTransfer.setExecutable(DataUtilities.trimString(rs.getString(COL_EXECUTABLE)));
		userExitRoutineTransfer.setTypeCode(DataUtilities.trimString(rs.getString(COL_TYPE)));
		userExitRoutineTransfer.setLanguageCode(DataUtilities.trimString(rs.getString(COL_LANGUAGE)));
		userExitRoutineTransfer.setOptimize(DataUtilities.intToBoolean(rs.getInt(COL_OPTIMIZED)));
		userExitRoutineTransfer.setComments(DataUtilities.trimString(rs.getString(COL_COMMENT)));
		userExitRoutineTransfer.setCreateTime(rs.getDate(COL_CREATETIME));
		userExitRoutineTransfer.setCreateBy(DataUtilities.trimString(rs.getString(COL_CREATEBY)));
		userExitRoutineTransfer.setModifyTime(rs.getDate(COL_MODIFYTIME));
		userExitRoutineTransfer.setModifyBy(DataUtilities.trimString(rs.getString(COL_MODIFYBY)));

		return userExitRoutineTransfer;
	}

	public UserExitRoutineTransfer persistUserExitRoutine(UserExitRoutineTransfer userExitRoutineTransfer) throws DAOException, SAFRNotFoundException {

		if (!userExitRoutineTransfer.isPersistent()) {
			return (createUserExitRoutine(userExitRoutineTransfer));
		} else {
			return (updateUserExitRoutine(userExitRoutineTransfer));
		}

	}

	/**
	 * This method is used to create a User Exit Routine in EXIT in the
	 * database.
	 * 
	 * @param userExitRoutineTransfer
	 * @return The transfer object which contains the values retrieved from
	 *         EXIT for the User Exit Routine.
	 * @throws DAOException
	 */
	private UserExitRoutineTransfer createUserExitRoutine(UserExitRoutineTransfer ext)
			throws DAOException {
		Path exitsPath = YAMLizer.getExitsPath();
		exitsPath.toFile().mkdirs();
		Path exPath = exitsPath.resolve(ext.getName() + ".yaml");
		ext.setCreateBy(SAFRApplication.getUserSession().getUser().getUserid());
		ext.setCreateTime(new Date());
		ext.setModifyBy("");
		ext.setModifyTime(null);
		ext.setId(maxid + 1);
		YAMLizer.writeYaml(exPath, ext);
		return ext;

	}

	/**
	 * This method is used to update a User Exit Routine in EXIT of the
	 * database.
	 * 
	 * @param userExitRoutineTransfer
	 *            The transfer object which contains the values to be set in the
	 *            fields for the corresponding User Exit Routine being updated.
	 * @return The transfer object which contains the values retrieved from
	 *         EXIT for the User Exit Routine.
	 * @throws DAOException
	 * @throws SAFRNotFoundException
	 */

	private UserExitRoutineTransfer updateUserExitRoutine(UserExitRoutineTransfer userExitRoutineTransfer)	throws DAOException, SAFRNotFoundException {
		Path exitsPath = YAMLizer.getExitsPath();
		Path exPath = exitsPath.resolve(userExitRoutineTransfer.getName() + ".yaml");
		if(exPath.toFile().exists()) {
			userExitRoutineTransfer.setModifyBy(SAFRApplication.getUserSession().getUser().getUserid());
			userExitRoutineTransfer.setModifyTime(new Date());
			YAMLizer.writeYaml(exPath, userExitRoutineTransfer);
		} else {
			//SaveAs via name change
			createUserExitRoutine(userExitRoutineTransfer);
		}
		return (userExitRoutineTransfer);
	}

	public List<UserExitRoutineTransfer> getAllUserExitRoutines(
			Integer environmentId) throws DAOException {
		List<UserExitRoutineTransfer> result = new ArrayList<UserExitRoutineTransfer>();

		return result;
	}

	public UserExitRoutineTransfer getDuplicateUserExitRoutine(
			String userExitRoutineName, Integer userExitRoutineId,
			Integer environmentId) throws DAOException {
		Path exitsPath = YAMLizer.getExitsPath();
		exitsPath.toFile().mkdirs();
		Path exPath = exitsPath.resolve(userExitRoutineName + ".yaml");
		UserExitRoutineTransfer result = (UserExitRoutineTransfer) YAMLizer.readYaml(exPath, ComponentType.UserExitRoutine);
		if(result != null) {
			if(result.getId() != userExitRoutineId) { 
				logger.info("Existing Exit Record with name '" + userExitRoutineName
						+ "' found in Environment  [" + environmentId + "]");
			} else {
				result = null; //if the id equals this must be an update				
			}
		}
		return result;
	}

	public UserExitRoutineTransfer getDuplicateUserExitExecutable(
			String userExitRoutineExecutable, Integer userExitRoutineId,
			Integer environmentId) throws DAOException {
		UserExitRoutineTransfer userExitRoutineTransfer = null;
		//There is a rule that the exit executable cannot be duplicated with the exit record
		
		return userExitRoutineTransfer;

	}

	public void removeUserExitRoutine(Integer id, Integer environmentId) throws DAOException {
		Path exitsPath = YAMLizer.getExitsPath();
		exitsPath.toFile().mkdirs();
		Path exPath = exitsPath.resolve(exitBeans.get(id).getName()+".yaml");
		exPath.toFile().delete();
	}

	public Map<ComponentType, List<DependentComponentTransfer>> getUserExitRoutineDependencies(
			Integer environmentId, Integer userExitRoutineId)
			throws DAOException {
		//Live with the consequences find problems at activation time
		Map<ComponentType, List<DependentComponentTransfer>> dependencies = new HashMap<ComponentType, List<DependentComponentTransfer>>();
		return dependencies;

	}
	
    public List<DependentComponentTransfer> getUserExitRoutineLogicViewDeps(Integer environmentId, Integer userExitRoutineId) throws DAOException {
        List<DependentComponentTransfer> dependencies = new ArrayList<DependentComponentTransfer>();
        return dependencies;
    }

	@Override
	public Integer getUserExitRoutine(String name, Integer environmentId, boolean procedure) {
		Integer result = null;
		//Is this used?
		return result;
	}
	

}
