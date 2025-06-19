package com.ibm.safr.we.internal.data;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;

import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactory;
import com.ibm.safr.we.data.DAOUOW;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.CodeSetDAO;
import com.ibm.safr.we.data.dao.ControlRecordDAO;
import com.ibm.safr.we.data.dao.DependencyCheckerDAO;
import com.ibm.safr.we.data.dao.EnvironmentDAO;
import com.ibm.safr.we.data.dao.ExportDAO;
import com.ibm.safr.we.data.dao.GroupDAO;
import com.ibm.safr.we.data.dao.HeaderFooterDAO;
import com.ibm.safr.we.data.dao.LRFieldDAO;
import com.ibm.safr.we.data.dao.LogicalFileDAO;
import com.ibm.safr.we.data.dao.LogicalRecordDAO;
import com.ibm.safr.we.data.dao.LookupDAO;
import com.ibm.safr.we.data.dao.LookupPathStepDAO;
import com.ibm.safr.we.data.dao.MigrateDAO;
import com.ibm.safr.we.data.dao.NextKeyDAO;
import com.ibm.safr.we.data.dao.OldCompilerDAO;
import com.ibm.safr.we.data.dao.PhysicalFileDAO;
import com.ibm.safr.we.data.dao.ReportsDAO;
import com.ibm.safr.we.data.dao.StoredProcedureDAO;
import com.ibm.safr.we.data.dao.UserDAO;
import com.ibm.safr.we.data.dao.UserExitRoutineDAO;
import com.ibm.safr.we.data.dao.ViewColumnDAO;
import com.ibm.safr.we.data.dao.ViewColumnSourceDAO;
import com.ibm.safr.we.data.dao.ViewDAO;
import com.ibm.safr.we.data.dao.ViewFolderDAO;
import com.ibm.safr.we.data.dao.ViewLogicDependencyDAO;
import com.ibm.safr.we.data.dao.ViewSortKeyDAO;
import com.ibm.safr.we.data.dao.ViewSourceDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLCodeSetDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLControlRecordDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLEnvironmentDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLExportDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLHeaderFooterDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLLogicalFileDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLLogicalRecordDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLLookupDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLLookupPathStepDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLLRFieldDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLPhysicalFileDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLUserExitRoutineDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLViewColumnDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLViewColumnSourceDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLViewDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLViewFolderDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLViewLogicDependencyDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLViewSortKeyDAO;
import com.ibm.safr.we.internal.data.dao.yamldao.YAMLViewSourceDAO;

public class YAMLDAOFactory implements DAOFactory {
	public static final String GENEVAERS = ".genevaers";
	private ConnectionParameters _params;
	UserSessionParameters _safrLogin;
	private static Path gersHome;
	private YAMLDAOUOW _uow;
	private static YAMLLogicalRecordDAO ourLRDAO;
	private static PhysicalFileDAO pfDao;
	private static LogicalFileDAO lfDao;
	private static LRFieldDAO fieldDao;

	public YAMLDAOFactory(ConnectionParameters p) {
		makeGenevaERSDirectory();
		_params = p;
	}
	
	@Override
	public void setSAFRLogin(UserSessionParameters params) {
		_safrLogin = params;
	}

	@Override
	public void disconnect() throws DAOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public EnvironmentDAO getEnvironmentDAO() throws DAOException {
		return new YAMLEnvironmentDAO(null, _params, null);
	}

	@Override
	public UserDAO getUserDAO() throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GroupDAO getGroupDAO() throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ViewDAO getViewDAO() throws DAOException {
		return new YAMLViewDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public ViewSourceDAO getViewSourceDAO() throws DAOException {
		return new YAMLViewSourceDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public ViewColumnDAO getViewColumnDAO() throws DAOException {
		return new YAMLViewColumnDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public ViewColumnSourceDAO getViewColumnSourceDAO() throws DAOException {
		return new YAMLViewColumnSourceDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public ViewSortKeyDAO getViewSortKeyDAO() throws DAOException {
		return new YAMLViewSortKeyDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public ViewLogicDependencyDAO getViewLogicDependencyDAO() throws DAOException {
		return new YAMLViewLogicDependencyDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public HeaderFooterDAO getHeaderFooterDAO() throws DAOException {
		return new YAMLHeaderFooterDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public ViewFolderDAO getViewFolderDAO() throws DAOException {
		return new YAMLViewFolderDAO(getConnection(), _params, getSAFRLogin());
	}

	@Override
	public ControlRecordDAO getControlRecordDAO() throws DAOException {
		return new YAMLControlRecordDAO(getConnection(), _params, getSAFRLogin());
	}

	@Override
	public CodeSetDAO getCodeSetDAO() throws DAOException {
		return new YAMLCodeSetDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public PhysicalFileDAO getPhysicalFileDAO() throws DAOException {
		if(pfDao == null) {
			pfDao = new YAMLPhysicalFileDAO(getConnection(), _params, _safrLogin);
		}
		return pfDao; 
	}

	@Override
	public UserExitRoutineDAO getUserExitRoutineDAO() throws DAOException {
		return new YAMLUserExitRoutineDAO(getConnection(), _params, getSAFRLogin());
	}

	@Override
	public LogicalFileDAO getLogicalFileDAO() throws DAOException {
		if(lfDao == null) {
			lfDao = new YAMLLogicalFileDAO(getConnection(), _params, getSAFRLogin());
		}
		return lfDao; 
	}

	@Override
	public LogicalRecordDAO getLogicalRecordDAO() throws DAOException {
		if(ourLRDAO == null) {
			ourLRDAO = new YAMLLogicalRecordDAO(getConnection(), _params, getSAFRLogin());
		}
		return ourLRDAO; 
	}

	@Override
	public LRFieldDAO getLRFieldDAO() throws DAOException {
		if(fieldDao == null) {
			fieldDao = new YAMLLRFieldDAO(getConnection(), _params, getSAFRLogin());
		}
		return fieldDao;
	}

	@Override
	public OldCompilerDAO getOldCompilerDAO() throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public LookupDAO getLookupDAO() throws DAOException {
		return new YAMLLookupDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public LookupPathStepDAO getLookupPathStepDAO() throws DAOException {
		return new YAMLLookupPathStepDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public ExportDAO getExportDAO() throws DAOException {
		// add all the transfer objects together and export
		return new YAMLExportDAO(getConnection(), _params, _safrLogin);
	}

	@Override
	public MigrateDAO getMigrateDAO() throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DependencyCheckerDAO getDependencyCheckerDAO() throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NextKeyDAO getNextKeyDAO() throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StoredProcedureDAO getStoredProcedureDAO() throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConnectionParameters getConnectionParameters() {
		return _params;
	}

	@Override
	public Connection reconnect() throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Connection getConnection() throws DAOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DAOUOW getDAOUOW() {
		if (_uow == null) {
			_uow = new YAMLDAOUOW();
		}
		return _uow;
	}

	@Override
	public UserSessionParameters getSAFRLogin() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ReportsDAO getReportsDAO() {
		// TODO Auto-generated method stub
		return null;
	}

	public static Path getGersHome() {
		return gersHome;
	}
	
	private void makeGenevaERSDirectory() {
		String home = System.getProperty("user.home");
		Path homep = Paths.get(home);
		gersHome = homep.resolve(GENEVAERS);
		gersHome.toFile().mkdirs();
	}

}
