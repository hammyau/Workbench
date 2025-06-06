package com.ibm.safr.we.internal.data.dao.yamldao;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import com.ibm.safr.we.constants.ComponentType;
import com.ibm.safr.we.constants.ExportElementType;
import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.ExportDAO;
import com.ibm.safr.we.data.transfer.DependentComponentTransfer;
import com.ibm.safr.we.data.transfer.XMLTableDataTransfer;

public class YAMLExportDAO implements ExportDAO {

	public YAMLExportDAO(Connection connection, ConnectionParameters _params, UserSessionParameters _safrLogin) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Map<ComponentType, List<DependentComponentTransfer>> getComponentDependencies(ComponentType compType,
			Integer componentId, Integer environmentId) throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ExportElementType, Map<Integer, List<XMLTableDataTransfer>>> getPhysicalFileData(Integer environmentId,
			List<Integer> physicalFileIds) throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ExportElementType, Map<Integer, List<XMLTableDataTransfer>>> getLogicalFileData(Integer environmentId,
			List<Integer> logicalFileIds) throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ExportElementType, Map<Integer, List<XMLTableDataTransfer>>> getUserExitRoutineData(
			Integer environmentId, List<Integer> userExitIds) throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ExportElementType, Map<Integer, List<XMLTableDataTransfer>>> getLogicalRecordData(Integer environmentId,
			List<Integer> logicalRecordIds) throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ExportElementType, Map<Integer, List<XMLTableDataTransfer>>> getLookupPathData(Integer environmentId,
			List<Integer> lookupPathIds) throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ExportElementType, Map<Integer, List<XMLTableDataTransfer>>> getViewData(Integer environmentId,
			List<Integer> viewIds) throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ExportElementType, Map<Integer, List<XMLTableDataTransfer>>> getViewFolderData(Integer environmentId,
			List<Integer> viewFolderIds) throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ExportElementType, Map<Integer, List<XMLTableDataTransfer>>> getControlRecordData(Integer environmentId,
			List<Integer> controlRecordIds) throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

}
