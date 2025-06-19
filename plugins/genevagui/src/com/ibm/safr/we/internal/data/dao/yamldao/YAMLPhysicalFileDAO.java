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

	static transient Logger logger = Logger.getLogger("com.ibm.safr.we.internal.data.dao.PGPhysicalFileDAO");


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

	public List<FileAssociationTransfer> getAssociatedLogicalFiles(Integer id,	Integer environmentId) throws DAOException {
		return new ArrayList<FileAssociationTransfer>();
	}

	public List<DependentComponentTransfer> getViewDependencies(Integer environmentId, Integer physicalFileId) throws DAOException {
		return new ArrayList<DependentComponentTransfer>();
	}

	public List<DependentComponentTransfer> getAssociatedLogicalFilesWithOneAssociatedPF(Integer environmentId, Integer physicalFileId) throws DAOException {
		return new ArrayList<DependentComponentTransfer>();
	}

}
