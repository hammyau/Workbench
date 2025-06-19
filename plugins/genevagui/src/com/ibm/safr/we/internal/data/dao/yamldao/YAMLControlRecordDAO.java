package com.ibm.safr.we.internal.data.dao.yamldao;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Stream;

import com.google.common.flogger.FluentLogger;
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
import com.ibm.safr.we.data.transfer.EnvironmentTransfer;
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
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

	private static Map<Integer, ControlRecordQueryBean> crsBeans = new TreeMap<>();
	private static int maxid = 0;

	public YAMLControlRecordDAO() {

	}

	public YAMLControlRecordDAO(Connection con, ConnectionParameters params, UserSessionParameters safrlogin) {
	}

	public List<ControlRecordQueryBean> queryAllControlRecords(Integer environmentId, SortType sortType)
			throws DAOException {
		List<ControlRecordQueryBean> result = new ArrayList<ControlRecordQueryBean>();
		maxid = 0;
		Path defaultPath = YAMLDAOFactory.getGersHome()
				.resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		File[] crs = defaultPath.toFile().listFiles();

		if (crs.length > 0) {
			Stream.of(crs).filter(file -> file.isFile()).forEach(cr -> addToResults(result, cr, environmentId));
		}
		return result;
	}

	private Object addToResults(List<ControlRecordQueryBean> result, File cr, Integer environmentId) {
		ControlRecordTransfer crt = (ControlRecordTransfer) YAMLizer.readYaml(cr.toPath(), ComponentType.ControlRecord);
		if (crt.getId() > maxid) {
			maxid = crt.getId();
		}
		ControlRecordQueryBean controlRecordBean = new ControlRecordQueryBean(environmentId, crt.getId(), crt.getName(),
				EditRights.ReadModifyDelete, crt.getCreateTime(), crt.getCreateBy(), crt.getModifyTime(),
				crt.getModifyBy());
		result.add(controlRecordBean);
		crsBeans.put(crt.getId(), controlRecordBean);
		return controlRecordBean;
	}

	public ControlRecordTransfer getControlRecord(Integer id, Integer environmentId) throws DAOException {
		Path defaultPath = YAMLDAOFactory.getGersHome()
				.resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(crsBeans.get(id).getName() + ".yaml");
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

	private ControlRecordTransfer createControlRecord(ControlRecordTransfer crec) throws DAOException {

		Path defaultPath = YAMLDAOFactory.getGersHome()
				.resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(crec.getName() + ".yaml");
		crec.setCreateBy(SAFRApplication.getUserSession().getUser().getUserid());
		crec.setCreateTime(new Date());
		crec.setId(maxid + 1);
		YAMLizer.writeYaml(crPath, crec);
		return crec;
	}

	public void addControlRecordToEnvironment(ControlRecordTransfer crec, String env) throws DAOException {
		Path defaultPath = YAMLDAOFactory.getGersHome().resolve(env).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(crec.getName() + ".yaml");
		crec.setCreateBy(SAFRApplication.getUserSession().getUser().getUserid());
		crec.setCreateTime(new Date());
		crec.setId(maxid + 1);
		YAMLizer.writeYaml(crPath, crec);
	}

	private ControlRecordTransfer updateControlRecord(ControlRecordTransfer crec)
			throws DAOException, SAFRNotFoundException {
		// If modified the name then it's a saveAs?
		Path defaultPath = YAMLDAOFactory.getGersHome()
				.resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(crec.getName() + ".yaml");
		crec.setModifyBy(SAFRApplication.getUserSession().getUser().getUserid());
		crec.setModifyTime(new Date());
		YAMLizer.writeYaml(crPath, crec);
		return (crec);
	}

	public void removeControlRecord(Integer id, Integer environmentId) throws DAOException {
		Path defaultPath = YAMLDAOFactory.getGersHome()
				.resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(crsBeans.get(id).getName() + ".yaml");
		crPath.toFile().delete();
	}

	public ControlRecordTransfer getDuplicateControlRecord(String controlRecordName, Integer controlId,
			Integer environmentId) throws DAOException {
		Path defaultPath = YAMLDAOFactory.getGersHome()
				.resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
		defaultPath.toFile().mkdirs();
		Path crPath = defaultPath.resolve(controlRecordName + ".yaml");
		ControlRecordTransfer result = (ControlRecordTransfer) YAMLizer.readYaml(crPath, ComponentType.ControlRecord);
		if (result != null) {
			if (result.getId() != controlId) {
				logger.atInfo().log("Existing Control Record with name '" + controlRecordName + "' found in Environment  ["
						+ environmentId + "]");
			} else {
				result = null; // if the id equals this must be an update
			}
		}
		return result;
	}

	public List<DependentComponentTransfer> getControlRecordViewDependencies(Integer environmentId,
			Integer controlRecordId) throws DAOException {
		List<DependentComponentTransfer> dependencies = new ArrayList<DependentComponentTransfer>();
		// In YAML world this is not a relational database so no check?
		// We could maybe...
		return dependencies;

	}

	@Override
	public void deleteAllControlRecordsFromEnvironment(Integer environmentId) throws DAOException {
		EnvironmentTransfer env = DAOFactoryHolder.getDAOFactory().getEnvironmentDAO().getEnvironment(environmentId);
		if(env != null) {
		    Path pathToBeDeleted = YAMLDAOFactory.getGersHome().resolve(env.getName()).resolve("crs");
		    try (Stream<Path> paths = Files.walk(pathToBeDeleted)) {
		        paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
		    } catch (IOException e) {
		    	logger.atSevere().log("Clear Control Record failed %s", e.getMessage());
			}
		} else {
	    	logger.atSevere().log("Clear Control Record failed: Unable to find Environment %d", environmentId);
		}
	}

}
