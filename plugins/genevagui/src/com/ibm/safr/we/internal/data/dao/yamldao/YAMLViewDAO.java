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


import java.sql.CallableStatement;
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
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.ibm.safr.we.constants.CodeCategories;
import com.ibm.safr.we.constants.Codes;
import com.ibm.safr.we.constants.ComponentType;
import com.ibm.safr.we.constants.EditRights;
import com.ibm.safr.we.constants.LogicTextType;
import com.ibm.safr.we.constants.SearchCriteria;
import com.ibm.safr.we.constants.SearchPeriod;
import com.ibm.safr.we.constants.SearchViewsIn;
import com.ibm.safr.we.constants.SortType;
import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DAOUOWInterruptedException;
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.ViewDAO;
import com.ibm.safr.we.data.transfer.ComponentAssociationTransfer;
import com.ibm.safr.we.data.transfer.DependentComponentTransfer;
import com.ibm.safr.we.data.transfer.FindTransfer;
import com.ibm.safr.we.data.transfer.LookupPathTransfer;
import com.ibm.safr.we.data.transfer.ViewFolderViewAssociationTransfer;
import com.ibm.safr.we.data.transfer.ViewTransfer;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLogicalRecordTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLookupTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewTransfer;
import com.ibm.safr.we.model.SAFRApplication;
import com.ibm.safr.we.model.query.ControlRecordQueryBean;
import com.ibm.safr.we.model.query.EnvironmentalQueryBean;
import com.ibm.safr.we.model.query.LogicalFileQueryBean;
import com.ibm.safr.we.model.query.LogicalRecordQueryBean;
import com.ibm.safr.we.model.query.UserExitRoutineQueryBean;
import com.ibm.safr.we.model.query.ViewFolderQueryBean;
import com.ibm.safr.we.model.query.ViewQueryBean;

/**
 * This class is used to implement the unimplemented methods of <b>ViewDAO</b>.
 * This class contains the methods to related to View metadata component which
 * require database access.
 * 
 */
public class YAMLViewDAO implements ViewDAO {

	static transient Logger logger = Logger.getLogger("com.ibm.safr.we.internal.data.dao.YAMLViewDAO");

	private static final String TABLE_NAME = "VIEW";

	private static final String COL_ENVID = "ENVIRONID";
	private static final String COL_ID = "VIEWID";
	private static final String COL_STATUS = "VIEWSTATUSCD";
	private static final String COL_TYPE = "VIEWTYPECD";
	private static final String COL_OUTPUTFORMAT = "OUTPUTMEDIACD";
	private static final String COL_CREATETIME = "CREATEDTIMESTAMP";
	private static final String COL_CREATEBY = "CREATEDUSERID";
	private static final String COL_MODIFYTIME = "LASTMODTIMESTAMP";
	private static final String COL_MODIFYBY = "LASTMODUSERID";
    private static final String COL_COMPILER = "COMPILER";
    private static final String COL_ACTIVATETIME = "LASTACTTIMESTAMP";
    private static final String COL_ACTIVATEBY = "LASTACTUSERID";
    
	private Connection con;
	private ConnectionParameters params;
	private UserSessionParameters safrLogin;
	private PGSQLGenerator generator = new PGSQLGenerator();

	private static Map<Integer, ViewQueryBean> viewBeans = new TreeMap<>();
	private static YAMLViewTransfer ourViewTransfer;
	private static int maxid;
	private static Map<Integer, YAMLViewTransfer> viewTxfrsByID = new TreeMap<>();
	private static Map<String, YAMLViewTransfer> viewTxfrsByName = new TreeMap<>();

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
	public YAMLViewDAO(Connection con, ConnectionParameters params,
			UserSessionParameters safrLogin) {
		this.con = con;
		this.params = params;
		this.safrLogin = safrLogin;
	}

	public ViewTransfer getView(Integer id, Integer environmentId)	throws DAOException {
		ourViewTransfer = viewTxfrsByID.get(id); 
		return ourViewTransfer;
	}

	public List<ViewTransfer> queryAllLogicBlocks() {

	    List<ViewTransfer> views = new ArrayList<ViewTransfer>();
        try {        
            String selectString = "Select ENVIRONID,VIEWID "
                + "From " + params.getSchema() + ".E_LOGICTBL   "
                + "WHERE TYPECD = 1";                    
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
                ViewTransfer view = new ViewTransfer();
                view.setEnvironmentId(rs.getInt("ENVIRONID"));
                view.setId(rs.getInt("VIEWID"));
                views.add(view);
            } 
            pst.close();
            rs.close();
        
        } catch (SQLException e) {
            throw DataUtilities.createDAOException("Database error occurred while retrieving Logic blocks", e);
        }
        return views;
	}
	
    public byte [] getLogicTextBytes(Integer id, Integer environmentId) {
        
        byte[] logicTextBytes = null;        
        try {        
            String selectString = "Select B.LOGIC "
                + "From " + params.getSchema() + ".E_LOGICTBL B  "
                + "Where B.VIEWID=? AND B.ENVIRONID=? AND B.TYPECD = 1";                    
            PreparedStatement pst = null;
            ResultSet rs = null;            
            while (true) {
                try {
                    pst = con.prepareStatement(selectString);
                    pst.setInt(1, id);
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
                logicTextBytes = rs.getBytes("LOGIC");
            } else {
                logger.info("No such View Logic in Env " + environmentId + " with ID : " + id);
            }
            pst.close();
            rs.close();
        
        } catch (SQLException e) {
            throw DataUtilities.createDAOException("Database error occurred while retrieving the View Logic with id ["+ id + "]", e);
        }
        return logicTextBytes;      
    }
    
    public List<ViewQueryBean> queryAllViews(SortType sortType, Integer environmentId, Integer viewFolderId) throws DAOException {
        List<ViewQueryBean> result = new ArrayList<ViewQueryBean>();
			logger.info("No Beans read the dir");
			maxid = 0;
			Path viewsPath = YAMLizer.getViewsPath();
			viewsPath.toFile().mkdirs();
			File[] views = viewsPath.toFile().listFiles();
			viewBeans.clear();
			if(views.length > 0) {
				Stream.of(views)
			    	      .filter(file -> file.isFile())
			    	      .forEach(lr -> addToResults(lr, environmentId));
			}
			result.addAll(viewBeans.values());
			return result;
		}

    private void addToResults(File vw, Integer environmentId) {
		YAMLViewTransfer vwt = (YAMLViewTransfer) YAMLizer.readYaml(vw.toPath(), ComponentType.View);
		if(vwt.getId() > maxid) {
			maxid = vwt.getId();
		}
		ViewQueryBean lrBean = new ViewQueryBean(
				environmentId, vwt.getId(), 
				vwt.getName(),
				vwt.getStatusCode(),
				"FILE",
				"EXTR", //vwt.getTypeCode(),
				EditRights.ReadModifyDelete,
				vwt.getCreateTime(), 
				vwt.getCreateBy(), 
				vwt.getModifyTime(), 
				vwt.getModifyBy(),
				"compilerVer",
				vwt.getCreateTime(),
				vwt.getCreateBy()); 
		viewBeans.put(vwt.getId(), lrBean);
		viewTxfrsByID.put(vwt.getId(), vwt);
		viewTxfrsByName.put(vwt.getName(), vwt);
		logger.info("Beans Add ViewTxf for View:" + vwt.getId());
	}
	
	public List<ViewQueryBean> queryAllViewsOld(SortType sortType,
			Integer environmentId, Integer viewFolderId) throws DAOException {
		List<ViewQueryBean> result = new ArrayList<ViewQueryBean>();

		boolean admin = SAFRApplication.getUserSession().isSystemAdministrator();

		String orderString = null;
		if (sortType.equals(SortType.SORT_BY_ID)) {
			orderString = " ORDER BY A.VIEWID";
		} else {
			orderString = " ORDER BY UPPER(A.VIEWNAME)";
		}
		try {
			String selectString = "";
			if (viewFolderId == -1L) {
				if (admin) {
					selectString = "SELECT A.VIEWID, A.VIEWNAME, A.VIEWSTATUSCD, A.OUTPUTMEDIACD, "
							+ "A.VIEWTYPECD, A.CREATEDTIMESTAMP, A.CREATEDUSERID, "
							+ "A.LASTMODTIMESTAMP, A.LASTMODUSERID FROM "
							+ params.getSchema() + ".E_VIEWTBL A "
							+ "WHERE A.VIEWID > 0 AND A.ENVIRONID = ?" + orderString;
				} else {
					selectString = "SELECT A.VIEWID, A.VIEWNAME, A.VIEWSTATUSCD, A.OUTPUTMEDIACD, "
							+ "A.VIEWTYPECD, C.VIEWRIGHTS, A.CREATEDTIMESTAMP, A.CREATEDUSERID, "
							+ "A.LASTMODTIMESTAMP, A.LASTMODUSERID FROM "
							+ params.getSchema() + ".E_VIEWTBL A LEFT OUTER JOIN "
							+ params.getSchema() + ".X_SECGROUPSVIEW C ON C.ENVIRONID=A.ENVIRONID AND A.VIEWID=C.VIEWID AND C.SECGROUPID= ?"
							+ " WHERE A.VIEWID > 0 AND A.ENVIRONID = ? " + orderString;
				}

			} else {
				if (admin) {
					selectString = "SELECT A.VIEWID, A.VIEWNAME, A.VIEWSTATUSCD, A.OUTPUTMEDIACD, "
							+ "A.VIEWTYPECD, A.CREATEDTIMESTAMP, A.CREATEDUSERID, "
							+ "A.LASTMODTIMESTAMP,A.LASTMODUSERID FROM "
							+ params.getSchema() + ".E_VIEWTBL A "
							+ "WHERE A.VIEWFOLDERID = ? "
							+ " AND A.VIEWID > 0 AND A.ENVIRONID = ?" + orderString;
				} else {
					selectString = "SELECT A.VIEWID, A.VIEWNAME, A.VIEWSTATUSCD, A.OUTPUTMEDIACD, "
							+ "A.VIEWTYPECD, C.VIEWRIGHTS, A.CREATEDTIMESTAMP, A.CREATEDUSERID, "
							+ "A.LASTMODTIMESTAMP, A.LASTMODUSERID FROM "
							+ params.getSchema() + ".E_VIEWTBL A LEFT OUTER JOIN "
							+ params.getSchema() + ".X_SECGROUPSVIEW C ON C.ENVIRONID=A.ENVIRONID AND A.VIEWID = C.VIEWID "
							+ "AND C.SECGROUPID = ? "
							+ " WHERE A.VIEWFOLDERID = ? "
							+ " AND A.VIEWID > 0 AND A.ENVIRONID = ? " + orderString;
				}
			}
			PreparedStatement pst = null;
			ResultSet rs = null;
			while (true) {
				try {
					pst = con.prepareStatement(selectString);
                    if (viewFolderId == -1L) {
                        if (admin) {
                        	pst.setInt(1,  environmentId);
                        } else {
                        	pst.setInt(1,  SAFRApplication.getUserSession().getGroup().getId());
                        	pst.setInt(2,  environmentId);
                        }
                    } else {
                        if (admin) {
                        	pst.setInt(1,  viewFolderId);
                        	pst.setInt(2,  environmentId);
                        } else {
                        	pst.setInt(1,  SAFRApplication.getUserSession().getGroup().getId());
                        	pst.setInt(2,  viewFolderId);                        	
                        	pst.setInt(3,  environmentId);
                        }
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
				ViewQueryBean viewQueryBean = new ViewQueryBean(environmentId,
					rs.getInt(COL_ID), 
					DataUtilities.trimString(rs.getString("VIEWNAME")), 
					DataUtilities.trimString(rs.getString(COL_STATUS)),
					DataUtilities.trimString(rs.getString(COL_OUTPUTFORMAT)),
					DataUtilities.trimString(rs.getString(COL_TYPE)),
                    admin ? EditRights.ReadModifyDelete : SAFRApplication.getUserSession().getEditRights(
                        rs.getInt("VIEWRIGHTS"), ComponentType.View, environmentId), 
					rs.getDate(COL_CREATETIME), 
					DataUtilities.trimString(rs.getString(COL_CREATEBY)), 
					rs.getDate(COL_MODIFYTIME), 
					DataUtilities.trimString(rs.getString(COL_MODIFYBY)),
                    null,
                    null, 
                    null);
				result.add(viewQueryBean);
			}
			pst.close();
			rs.close();
			return result;

		} catch (SQLException e) {
			throw DataUtilities.createDAOException("Database error occurred while querying all Views.", e);
		}
	}

    public List<ViewQueryBean> queryAllViews(SortType sortType,
        Integer environmentId, boolean admin)
        throws DAOException {
        List<ViewQueryBean> result = new ArrayList<ViewQueryBean>();
        try {
            String selectString = "";
            String orderBy = "";
            if (sortType.equals(SortType.SORT_BY_ID)) {
                orderBy = " ORDER BY A.VIEWID";
            } else {
                orderBy = " ORDER BY UPPER(A.NAME)";
            }
    
            if (admin) {
                selectString = "SELECT A.VIEWID, A.NAME AS VIEWNAME, A.VIEWSTATUSCD, A.OUTPUTMEDIACD, "
                    + "A.VIEWTYPECD, A.CREATEDTIMESTAMP, A.CREATEDUSERID, "
                    + "A.LASTMODTIMESTAMP,A.LASTMODUSERID,A.COMPILER,A.LASTACTTIMESTAMP,A.LASTACTUSERID FROM "
                    + params.getSchema() + ".VIEW A "
                    + "WHERE A.VIEWID > 0 AND A.ENVIRONID = ? " + orderBy;
            } else {
                selectString = "SELECT A.VIEWID, A.NAME AS VIEWNAME, A.VIEWSTATUSCD, A.OUTPUTMEDIACD, "
                    + "A.VIEWTYPECD, C.RIGHTS, A.CREATEDTIMESTAMP, A.CREATEDUSERID, "
                    + "A.LASTMODTIMESTAMP,A.LASTMODUSERID,A.COMPILER,A.LASTACTTIMESTAMP,A.LASTACTUSERID FROM "
                    + params.getSchema() + ".VIEW A LEFT OUTER JOIN "
                    + params.getSchema() + ".SECVIEW C ON C.ENVIRONID=A.ENVIRONID AND A.VIEWID=C.VIEWID "
                    + "AND C.GROUPID = ?"
                    + " WHERE A.VIEWID > 0 AND A.ENVIRONID = ? " + orderBy;           
            }
    
            PreparedStatement pst = null;
            ResultSet rs = null;
            while (true) {
                try {
                    pst = con.prepareStatement(selectString);
                    if(admin) {
                    	pst.setInt(1, environmentId);
                    } else {
                    	pst.setInt(1,  SAFRApplication.getUserSession().getGroup().getId());
                    	pst.setInt(2, environmentId);                    	
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
                ViewQueryBean viewQueryBean = new ViewQueryBean(environmentId,
                    rs.getInt(COL_ID), 
                    DataUtilities.trimString(rs.getString("VIEWNAME")), 
                    DataUtilities.trimString(rs.getString(COL_STATUS)),
                    DataUtilities.trimString(rs.getString(COL_OUTPUTFORMAT)),
                    DataUtilities.trimString(rs.getString(COL_TYPE)), 
                    admin ? EditRights.ReadModifyDelete : SAFRApplication.getUserSession().getEditRights(
                        rs.getInt("RIGHTS"), ComponentType.View, environmentId), 
                    rs.getDate(COL_CREATETIME), 
                    DataUtilities.trimString(rs.getString(COL_CREATEBY)), 
                    rs.getDate(COL_MODIFYTIME), 
                    DataUtilities.trimString(rs.getString(COL_MODIFYBY)),
                    DataUtilities.trimString(rs.getString(COL_COMPILER)),
                    rs.getDate(COL_ACTIVATETIME), 
                    DataUtilities.trimString(rs.getString(COL_ACTIVATEBY)));
                result.add(viewQueryBean);
            }
            pst.close();
            rs.close();
        } catch (SQLException e) {
            throw DataUtilities.createDAOException(
                    "Database error occurred while querying all Views.", e);
        }
        return result;
    }

	public ViewTransfer persistView(ViewTransfer viewTransfer) throws DAOException, SAFRNotFoundException {
		ourViewTransfer = new YAMLViewTransfer(viewTransfer);
		if (!viewTransfer.isPersistent()) {
			return (createView(ourViewTransfer));
		} else {
			return (updateView(ourViewTransfer));
		}
	}

	private ViewTransfer createView(ViewTransfer viewTransfer) throws DAOException {
		viewTransfer.setId(maxid + 1);
		//YAML views are always stored as INACTIVE to for reactivation
		viewTransfer.setStatusCode(SAFRApplication.getSAFRFactory().getCodeSet(CodeCategories.VIEWSTATUS).getCode(Codes.INACTIVE).getKey());
		viewTransfer.setCreateBy(SAFRApplication.getUserSession().getUser().getUserid());
		viewTransfer.setCreateTime(new Date());
		saveView(viewTransfer);
		return viewTransfer;
	}

	public static void saveView(ViewTransfer viewTransfer) {
		Path viewsPath = YAMLizer.getViewsPath();
		viewsPath.toFile().mkdirs();
		Path viewPath = viewsPath.resolve(viewTransfer.getName()+ ".yaml");
		logger.info("Save View " + viewPath.toString());
		YAMLizer.writeYaml(viewPath, viewTransfer);
	}

	private int prepareViewDetails(ViewTransfer viewTransfer, PreparedStatement pst, int i) throws SQLException {
		pst.setString(i++, viewTransfer.getName());
		pst.setNull(i++, Types.DATE);
		pst.setString(i++, viewTransfer.getStatusCode());
		pst.setString(i++, viewTransfer.getTypeCode());
		pst.setInt(i++, viewTransfer.getWorkFileNumber());
		pst.setString(i++, viewTransfer.getOutputFormatCode());
		Integer lrId = viewTransfer.getOutputLRId();
		if(lrId == null || lrId == 0) {
			pst.setNull(i++, Types.INTEGER);
		} else {
			pst.setInt(i++, lrId);
		}
		Integer efaId = viewTransfer.getExtractFileAssocId();
		if(efaId == null || efaId == 0) {
			pst.setNull(i++, Types.INTEGER);
		} else {
			pst.setInt(i++, efaId);
		}
		pst.setInt(i++, viewTransfer.getPageSize());
		pst.setInt(i++, viewTransfer.getLineSize());
		pst.setInt(i++, DataUtilities.booleanToInt(viewTransfer.isSuppressZeroRecords()));
		pst.setInt(i++, viewTransfer.getExtractMaxRecCount());
		pst.setInt(i++, DataUtilities.booleanToInt(viewTransfer.isAggregateBySortKey()));
		pst.setInt(i++, viewTransfer.getExtractSummaryBuffer());
		pst.setInt(i++, viewTransfer.getOutputMaxRecCount());
		pst.setInt(i++, viewTransfer.getControlRecId());
		Integer exId = viewTransfer.getWriteExitId();
		if(exId == null || exId == 0) {
			pst.setNull(i++, Types.INTEGER);
		} else {
			pst.setInt(i++, exId);
		}
		pst.setString(i++, viewTransfer.getWriteExitParams());
		Integer fexId = viewTransfer.getFormatExitId();
		if(fexId == null || fexId == 0) {
			pst.setNull(i++, Types.INTEGER);
		} else {
			pst.setInt(i++, fexId);
		}
		pst.setString(i++, viewTransfer.getFormatExitParams());
		if (viewTransfer.getFieldDelimCode() == null) {
		    pst.setNull(i++, Types.CHAR);
		} else {
		    pst.setString(i++, viewTransfer.getFieldDelimCode());
		}
		pst.setString(i++, viewTransfer.getStringDelimCode());
		pst.setString(i++, viewTransfer.getFormatFilterlogic());
		pst.setString(i++, viewTransfer.getComments());
		return i;
	}

	private ViewTransfer updateView(ViewTransfer viewTransfer) throws DAOException, SAFRNotFoundException {
		viewTransfer.setModifyBy(SAFRApplication.getUserSession().getUser().getUserid());
		viewTransfer.setModifyTime(new Date());
		saveView(viewTransfer);
		return viewTransfer;
	}

	//make an import and a normal prepare
	private PreparedStatement normalPrepareAndExecuteUpdate(
			ViewTransfer viewTransfer, String statement, PreparedStatement pst)
			throws SQLException {
		try {
			pst = con.prepareStatement(statement);

			int i = 1;
			i = prepareViewDetails(viewTransfer, pst, i);
            pst.setString(i++, safrLogin.getUserId()); //Last Modified                            
		    if (viewTransfer.isActivated()) {
                pst.setString(i++, viewTransfer.getCompilerVersion());
                pst.setString(i++, safrLogin.getUserId());  //Last Activated                            
		    }
		    pst.setInt(i++, viewTransfer.getId());
		    pst.setInt(i++, viewTransfer.getEnvironmentId());
	        ResultSet rs = pst.executeQuery();
	        rs.next();
	        int j=1;
	        if (viewTransfer.isUpdated()) {
	            viewTransfer.setModifyBy(safrLogin.getUserId());
	            viewTransfer.setModifyTime(rs.getDate(j++));
	            viewTransfer.setUpdated(false);
	        }
	        if (viewTransfer.isActivated()) {
	            viewTransfer.setActivatedBy(safrLogin.getUserId());
	            viewTransfer.setActivatedTime(rs.getDate(j++));
	            viewTransfer.setActivated(false);
	        }                        
	        rs.close();                 
	        pst.close();
		} catch (SQLException se) {
			if (con.isClosed()) {
				// lost database connection, so reconnect and retry
				con = DAOFactoryHolder.getDAOFactory().reconnect();
			} else {
				throw se;
			}
		}
		return pst;
	}

	
	//This looks like import of Migrate does not 
	private PreparedStatement importPrepareAndExecuteUpdate(
			ViewTransfer viewTransfer, String statement, PreparedStatement pst)
			throws SQLException {
		try {
			pst = con.prepareStatement(statement);

			int i = 1;
			i = prepareViewDetails(viewTransfer, pst, i);
		    pst.setInt(i++, viewTransfer.getId());
		    pst.setInt(i++, viewTransfer.getEnvironmentId());
	        int count  = pst.executeUpdate();   
	        if (count == 0) {
	            throw new SAFRNotFoundException("No Rows updated.");
	        }                       
	        pst.close();
		} catch (SQLException se) {
			if (con.isClosed()) {
				// lost database connection, so reconnect and retry
				con = DAOFactoryHolder.getDAOFactory().reconnect();
			} else {
				throw se;
			}
		}
		return pst;
	}

	public Map<ComponentType, List<EnvironmentalQueryBean>> queryViewPropertiesLists(Integer environmentId) throws DAOException {
		Map<ComponentType, List<EnvironmentalQueryBean>> result = new HashMap<ComponentType, List<EnvironmentalQueryBean>>();
		List<EnvironmentalQueryBean> innerList = null;
		
		List<ControlRecordQueryBean> crs = DAOFactoryHolder.getDAOFactory().getControlRecordDAO().queryAllControlRecords(environmentId, null);
		
		innerList = crs.stream().collect(Collectors.toList());
		result.put(ComponentType.ControlRecord, innerList);
		return result;
	}

	public Map<ComponentType, List<DependentComponentTransfer>> getInactiveDependenciesOfView(Integer environmentId, Integer viewId) throws DAOException {
		Map<ComponentType, List<DependentComponentTransfer>> result = new HashMap<ComponentType, List<DependentComponentTransfer>>();
		return result;
	}

	public void removeView(Integer id, Integer environmentId)
			throws DAOException {
		try {
			List<String> idNames = new ArrayList<String>();
			idNames.add(COL_ID);
			idNames.add(COL_ENVID);

			String statement = generator.getDeleteStatement(params.getSchema(),
					TABLE_NAME, idNames);
			PreparedStatement pst = null;

			while (true) {
				try {
					pst = con.prepareStatement(statement);

					int i = 1;
					pst.setInt(i++, id);
					pst.setInt(i++, environmentId);
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
			throw DataUtilities.createDAOException(
					"Database error occurred while deleting the View.", e);
		}
	}

	public ViewTransfer getDuplicateView(String viewName, Integer viewId, Integer environmentId) throws DAOException {
		Path viewsPath = YAMLizer.getViewsPath();
		Path viewPath = viewsPath.resolve(viewName + ".yaml");
		ViewTransfer result = (ViewTransfer) YAMLizer.readYaml(viewPath, ComponentType.View);
		if(result != null) {
			if(result.getId() != viewId) { 
				logger.info("Existing View with name '" + viewName + "' found in Environment  [" + environmentId + "]");
			} else {
				result = null; //if the id equals this must be an update				
			}
		}
		return result;
	}

	public void persistViewLogic(ViewTransfer viewTransfer,
			boolean saveCompiledLogicText) throws DAOException {
		try {
			String table = "E_LOGICTBL";
			// delete the existing LT first.
			List<String> idNames = new ArrayList<String>();
			idNames.add(COL_ID);
			idNames.add(COL_ENVID);

			int i = 1;

			String statement = generator.getDeleteStatement(params.getSchema(),
					table, idNames);
			PreparedStatement pst = null;

			while (true) {
				try {
					pst = con.prepareStatement(statement);

					i = 1;
					pst.setInt(i++, viewTransfer.getId());
					pst.setInt(i++, viewTransfer.getEnvironmentId());
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
			if (viewTransfer.getLogicTextBytes() == null) {
				return;
			}
			pst.close();
			// insert the logic text
			String[] items = { COL_ENVID, COL_ID, "TYPECD", "LOGIC",
					COL_CREATETIME, COL_CREATEBY, COL_MODIFYTIME, COL_MODIFYBY };
			idNames.clear();
			idNames = Arrays.asList(items);

			statement = generator.getInsertStatementNoIdentifier(params.getSchema(), table,
					idNames, !viewTransfer.isForImport());
			pst = null;

			while (true) {
				try {

					pst = con.prepareStatement(statement);
					i = 1;
					pst.setInt(i++, viewTransfer.getEnvironmentId());
					pst.setInt(i++, viewTransfer.getId());
					pst.setInt(i++, 1);
					pst.setBytes(i++, viewTransfer.getLogicTextBytes());
					if (viewTransfer.isForImport()) {
						pst.setTimestamp(i++, DataUtilities.getTimeStamp(viewTransfer.getCreateTime()));
					}
					pst.setString(i++,viewTransfer.isForImport() ? viewTransfer.getCreateBy() : safrLogin.getUserId());
					if (viewTransfer.isForImport()) {
						pst.setTimestamp(i++, DataUtilities.getTimeStamp(viewTransfer.getModifyTime()));
					}
					pst.setString(i++,viewTransfer.isForImport() ? viewTransfer.getModifyBy() : safrLogin.getUserId());
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
			if (saveCompiledLogicText) {
				// insert compiled LT snippet.
				while (true) {
					try {
						i = 1;
						pst.setInt(i++, viewTransfer.getEnvironmentId());
						pst.setInt(i++, viewTransfer.getId());
						pst.setInt(i++, 2);
						pst.setBytes(i++, viewTransfer.getCompiledLogicTextBytes());
						if (viewTransfer.isForImport()) {
							pst.setTimestamp(i++, DataUtilities.getTimeStamp(viewTransfer.getCreateTime()));
						}
						pst.setString(i++,viewTransfer.isForImport() ? viewTransfer.getCreateBy() : safrLogin.getUserId());
						if (viewTransfer.isForImport()) {
							pst.setTimestamp(i++, DataUtilities.getTimeStamp(viewTransfer.getModifyTime()));
						}
						pst.setString(i++,viewTransfer.isForImport() ? viewTransfer.getModifyBy() : safrLogin.getUserId());
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
			}
			pst.close();
		} catch (SQLException e) {
			throw DataUtilities.createDAOException("Database error occurred while storing the logic text bytes contained in a View.",e);
		}
	}

	public void makeViewsInactive(Collection<Integer> viewIds, Integer environmentId) throws DAOException {
	}

    private class ViewInfo {
        public Integer id;
        public String name;
        public EditRights rights;
    };
	
    public List<FindTransfer> searchViewsToReplaceLogicText(
        Integer environmentId, SearchViewsIn searchViewsIn,
        List<Integer> componentsToSearchViewList,
        SearchCriteria searchCriteria, Date dateToSearch,
        SearchPeriod searchPeriod) throws DAOException {
        
        List<ViewInfo> views = findViews(environmentId, searchViewsIn, componentsToSearchViewList, 
                searchCriteria, dateToSearch, searchPeriod);
        
        List<FindTransfer> list = new ArrayList<FindTransfer>();
        for (ViewInfo viewInfo : views) {
            list.addAll(getViewFindTransfer(environmentId,viewInfo));
        }
        return list;
    }

    
    protected List<ViewInfo> findViews(Integer environmentId, SearchViewsIn searchViewsIn,
        List<Integer> componentsToSearchViewList,
        SearchCriteria searchCriteria, Date dateToSearch, SearchPeriod searchPeriod) 
    {
        boolean admin = SAFRApplication.getUserSession().isSystemAdministrator();
        List<ViewInfo> views = new ArrayList<ViewInfo>();
        String statement = "";
        String secjoin = "";
        if (admin) {
            statement = "SELECT A.VIEWID, A.NAME FROM ";            
        } else {
            statement = "SELECT A.VIEWID, A.NAME, C.RIGHTS FROM ";                        
            secjoin = " LEFT OUTER JOIN " + 
                params.getSchema()+ ".SECVIEW C ON A.ENVIRONID=C.ENVIRONID " +
                "AND A.VIEWID=C.VIEWID AND C.GROUPID=" + 
                SAFRApplication.getUserSession().getGroup().getId() + " ";
        }
        switch (searchViewsIn) {
            case SearchAllViews :
                statement += params.getSchema() + ".VIEW A " +
                secjoin + "WHERE A.ENVIRONID = ? ";
                break;
            case SearchInSpecificViews :
                statement += params.getSchema() + ".VIEW A " + 
                    secjoin + "WHERE A.VIEWID IN (" +
                    integerListToString(componentsToSearchViewList) +
                    ") AND A.ENVIRONID = ? ";
                break;
            case SearchInViewFolders :
                statement += params.getSchema() + ".VIEW A " + 
                    "JOIN " + params.getSchema() + ".VFVASSOC B " + 
                    " ON A.ENVIRONID=B.ENVIRONID AND A.VIEWID=B.VIEWID " +
                    secjoin + " WHERE B.VIEWFOLDERID IN (" + 
                    integerListToString(componentsToSearchViewList) +
                    ") AND B.ENVIRONID = ? ";                
                break;
            default :
                break;            
        }
        switch (searchCriteria) {
            case None :
                break;
            case SearchCreated :
                switch (searchPeriod) {
                    case After :
                        statement += " AND A.CREATEDTIMESTAMP > ? ";
                        break;
                    case Before :
                        statement += " AND A.CREATEDTIMESTAMP < ? ";
                        break;
                    case On :
                        statement += " AND DAYS(A.CREATEDTIMESTAMP) = ? ";
                        break;
                    default :
                        break;                    
                }
                break;
            case SearchModified :
                switch (searchPeriod) {
                    case After :
                        statement += " AND A.LASTMODTIMESTAMP > ? ";
                        break;
                    case Before :
                        statement += " AND A.LASTMODTIMESTAMP < ? ";
                        break;
                    case On :
                        statement += " AND DAYS(A.LASTMODTIMESTAMP) = ? ";
                        break;
                    default :
                        break;                    
                }
                break;
            default :
                break;
            
        }
        try {
            PreparedStatement pst = null;
            ResultSet rs;
            while (true) {
                try {
                    pst = con.prepareStatement(statement);
                    pst.setInt(1, environmentId);
                    if (searchCriteria.equals(SearchCriteria.SearchCreated) ||
                        searchCriteria.equals(SearchCriteria.SearchModified)) {
                        if (searchPeriod.equals(SearchPeriod.On)) {
                            Integer days = (int) (dateToSearch.getTime()/(24*60*60*1000)) + 719163;
                            pst.setInt(2,  days);                            
                        } else {
                            pst.setTimestamp(2,  new java.sql.Timestamp(dateToSearch.getTime()));
                        }
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
                ViewInfo vi = new ViewInfo();
                vi.id =  rs.getInt(1);
                vi.name = rs.getString(2);
                vi.rights = admin ? EditRights.ReadModifyDelete : SAFRApplication.getUserSession().getEditRights(
                    rs.getInt(3), ComponentType.View, environmentId); 
                views.add(vi);
            }
            rs.close();
            pst.close();
        } catch (SQLException e) {
            throw DataUtilities.createDAOException("Database error getting view ids to search. ",e);
        }
        return views;
    }

    public List<FindTransfer> getViewFindTransfer(Integer envId, ViewInfo vi) {
        
        List<FindTransfer> total;
        try {
            total = new ArrayList<FindTransfer>();
            total.addAll(getFormatFilterViewLogic(envId, vi));
            total.addAll(getFormatColumnViewLogic(envId, vi));
            total.addAll(getExtractFilterandOutputViewLogic(envId, vi));
            total.addAll(getExtractColumnViewLogic(envId, vi));
        } catch (SQLException e) {
            throw DataUtilities.createDAOException("Database error getting view find transfers.",e);
        }
        return total;
    }

    private List<FindTransfer> getFormatFilterViewLogic(Integer envId, ViewInfo vi) 
        throws DAOException, SQLException {
        List<FindTransfer> list = new ArrayList<FindTransfer>();
        String formatFiltStmt = "SELECT FORMATFILTLOGIC FROM  "
            + params.getSchema() + ".VIEW "
            + "WHERE ENVIRONID = ? " 
            + "AND VIEWID = ? ";
        ResultSet rs = null;
        PreparedStatement pformatFiltStmt;
        while (true) {
            try {
                pformatFiltStmt = con.prepareStatement(formatFiltStmt);
                pformatFiltStmt.setInt(1, envId);
                pformatFiltStmt.setInt(2, vi.id);                
                rs = pformatFiltStmt.executeQuery();
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
            FindTransfer trans = new FindTransfer();
            trans.setViewId(vi.id);
            trans.setViewName(vi.name);
            trans.setRights(vi.rights);
            trans.setLogicTextType(LogicTextType.Format_Record_Filter);
            trans.setCellId(vi.id);
            trans.setReferenceId(0);
            String l = rs.getString("FORMATFILTLOGIC");
            if (l != null) {
                trans.setLogicText(l); 
                if (!l.isEmpty()) {
                    list.add(trans);
                }
            }
        }
        rs.close();
        pformatFiltStmt.close();
        return list;
    }

    private List<FindTransfer> getFormatColumnViewLogic(Integer envId, ViewInfo vi) 
        throws DAOException, SQLException {
        List<FindTransfer> list = new ArrayList<FindTransfer>();
        String formatColStmt = "SELECT VIEWCOLUMNID, FORMATCALCLOGIC FROM "
            + params.getSchema() + ".VIEWCOLUMN "
            + "WHERE ENVIRONID = ? " 
            + "AND VIEWID = ?";
        ResultSet rs = null;
        PreparedStatement pformatColStmt;
        while (true) {
            try {
                pformatColStmt = con.prepareStatement(formatColStmt);
                pformatColStmt.setInt(1, envId);
                pformatColStmt.setInt(2, vi.id);                
                rs = pformatColStmt.executeQuery();
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
            FindTransfer trans = new FindTransfer();
            trans.setViewId(vi.id);
            trans.setViewName(vi.name);            
            trans.setRights(vi.rights);
            trans.setLogicTextType(LogicTextType.Format_Column_Calculation);
            trans.setReferenceId(0);
            trans.setCellId(rs.getInt("VIEWCOLUMNID"));
            String l = rs.getString("FORMATCALCLOGIC");
            if (l != null) {
                trans.setLogicText(l);  
                if (!l.isEmpty()) {
                    list.add(trans);
                }
            }
        }
        rs.close();
        pformatColStmt.close();
        return list;
    }

    private List<FindTransfer> getExtractFilterandOutputViewLogic(Integer envId, ViewInfo vi) 
        throws DAOException, SQLException {
        List<FindTransfer> list = new ArrayList<FindTransfer>();
        String extractFiltStmt = "SELECT VIEWSOURCEID, EXTRACTFILTLOGIC, EXTRACTOUTPUTLOGIC FROM  "
            + params.getSchema() + ".VIEWSOURCE B "
            + "WHERE ENVIRONID = ? " 
            + "AND VIEWID = ?";
        ResultSet rs = null;
        PreparedStatement pextractFiltStmt;
        while (true) {
            try {
                pextractFiltStmt = con.prepareStatement(extractFiltStmt);
                pextractFiltStmt.setInt(1, envId);
                pextractFiltStmt.setInt(2, vi.id);                
                rs = pextractFiltStmt.executeQuery();
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
            int viewSourceId = rs.getInt("VIEWSOURCEID");
            String l = rs.getString("EXTRACTFILTLOGIC");
            if (l != null) {
                if (!l.isEmpty()) {
                    FindTransfer trans = new FindTransfer();
                    trans.setViewId(vi.id);
                    trans.setViewName(vi.name);            
                    trans.setRights(vi.rights);
                    trans.setLogicTextType(LogicTextType.Extract_Record_Filter);
                    trans.setLogicText(l);
                    trans.setReferenceId(0);
                    trans.setCellId(viewSourceId);
                    list.add(trans);   
                }
            }
            l = rs.getString("EXTRACTOUTPUTLOGIC");
            if (l != null) {
                if (!l.isEmpty()) {
                    FindTransfer trans = new FindTransfer();
                    trans.setViewId(vi.id);
                    trans.setViewName(vi.name);            
                    trans.setRights(vi.rights);
                    trans.setLogicTextType(LogicTextType.Extract_Record_Output);
                    trans.setLogicText(l);
                    trans.setReferenceId(0);
                    trans.setCellId(viewSourceId);
                    list.add(trans);
                }
            }       
        }
        rs.close();
        pextractFiltStmt.close();
        return list;
    }

    private List<FindTransfer> getExtractColumnViewLogic(Integer envId, ViewInfo vi) 
        throws DAOException, SQLException {
        List<FindTransfer> list = new ArrayList<FindTransfer>();
        String extractColStmt = "SELECT B.COLUMNNUMBER,C.VIEWCOLUMNSOURCEID,C.EXTRACTCALCLOGIC FROM  "
            + params.getSchema() + ".VIEWCOLUMN B, "
            + params.getSchema() + ".VIEWCOLUMNSOURCE C "
            + "WHERE B.ENVIRONID = C.ENVIRONID " 
            + "AND B.VIEWCOLUMNID = C.VIEWCOLUMNID " 
            + "AND B.ENVIRONID = ? " 
            + "AND B.VIEWID = ?";
        ResultSet rs = null;
        PreparedStatement pextractColStmt;
        while (true) {
            try {
                pextractColStmt = con.prepareStatement(extractColStmt);
                pextractColStmt.setInt(1, envId);
                pextractColStmt.setInt(2, vi.id);                
                rs = pextractColStmt.executeQuery();
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
            FindTransfer trans = new FindTransfer();
            trans.setViewId(vi.id);
            trans.setViewName(vi.name);            
            trans.setRights(vi.rights);
            trans.setLogicTextType(LogicTextType.Extract_Column_Assignment);
            trans.setReferenceId(rs.getInt("COLUMNNUMBER"));
            trans.setCellId(rs.getInt("VIEWCOLUMNSOURCEID"));
            String l = rs.getString("EXTRACTCALCLOGIC");
            if (l != null) {
                trans.setLogicText(l); 
                if (!l.isEmpty()) {
                    list.add(trans);
                }
            }
        }
        rs.close();
        pextractColStmt.close();
        return list;
    }

    public void replaceLogicText(Integer environmentId,
        List<FindTransfer> replacements) throws DAOException {
        boolean success = false;
        while (!success) {                                                                              
            try {
                // start a transaction
                DAOFactoryHolder.getDAOFactory().getDAOUOW().begin();                               
                String statement = generator.getStoredProcedure(params.getSchema(), "UPDVWLOGIC", 2);
                CallableStatement proc = null;
                while (true) {
                    try {
                        proc = con.prepareCall(statement);
                        proc.setInt(1, environmentId);
                        String xml = generateReplaceXml(replacements);
                        proc.setString("DOC", xml);
                        proc.execute();
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
                proc.close();
                success=true;
            } catch (DAOUOWInterruptedException e) {
                // UOW interrupted so retry it
                continue;                                                                                                                           
            } catch (SQLException e) {
                throw DataUtilities.createDAOException(
                    "Database error occurred while replacing logic text.",e);
            } finally {
                DAOFactoryHolder.getDAOFactory().getDAOUOW().end();
            } // end transaction try
        } // end transaction while loop              
        
    }
	
	private String generateReplaceXml(List<FindTransfer> replacements) {
        StringBuffer xml = new StringBuffer();
        xml.append("<Root>\n");        
        for (FindTransfer trans : replacements) {
            xml.append(" <Operation>\n");
            xml.append("  <LOGTYPE>" + trans.getLogicTextType().getOpStr() + "</LOGTYPE>\n");
            xml.append("  <VIEWID>" + trans.getViewId() + "</VIEWID>\n");
            if (trans.getCellId() != null) {
                xml.append("  <CELLID>" + trans.getCellId() + "</CELLID>\n");
            }
            xml.append("  <LOGIC><![CDATA[" + trans.getLogicText() + "]]></LOGIC>\n");
            xml.append(" </Operation>\n");
        }
        xml.append("</Root>");        
        return xml.toString();
    }

    private static String integerListToString(
			List<Integer> listOfIntegerVariables) {
		String commaDelimitedString = "";
		if ((listOfIntegerVariables != null)
				&& (!listOfIntegerVariables.isEmpty())) {
			for (Integer integerVariable : listOfIntegerVariables) {
				commaDelimitedString += integerVariable.toString() + ",";
			}
			commaDelimitedString = commaDelimitedString.substring(0,
					commaDelimitedString.length() - 1);
		} else {
			commaDelimitedString = "0";
		}
		return commaDelimitedString;

	}

	public void deleteView(Integer viewId, Integer environmentId) throws DAOException {
		Path viewsPath = YAMLizer.getViewsPath();
		ViewQueryBean vBean = viewBeans.get(viewId);
		if(vBean != null) {
			viewsPath.resolve(vBean.getName() + ".yaml").toFile().delete();
		} else {
			logger.severe("Unable to find view " + viewId);
		}
	}

    @Override
    public List<ViewFolderViewAssociationTransfer> getVVFAssociation(Integer environmentId, Integer id, boolean admin) {
        return new ArrayList<ViewFolderViewAssociationTransfer>();
    }

    @Override
    public ViewQueryBean queryView(Integer environmentId, Integer viewId) {
        ViewQueryBean result = null;
        try {
            String selectString = "SELECT A.VIEWID, A.NAME AS VIEWNAME, A.VIEWSTATUSCD, A.OUTPUTMEDIACD, "
                + "A.VIEWTYPECD, A.CREATEDTIMESTAMP, A.CREATEDUSERID, "
                + "A.LASTMODTIMESTAMP,A.LASTMODUSERID,A.COMPILER,A.LASTACTTIMESTAMP,A.LASTACTUSERID FROM "
                + params.getSchema() + ".VIEW A "
                + "WHERE A.VIEWID = ? " 
                + "AND A.ENVIRONID = ? ";
    
            PreparedStatement pst = null;
            ResultSet rs = null;
            while (true) {
                try {
                    pst = con.prepareStatement(selectString);
                	pst.setInt(1,  viewId);
                	pst.setInt(2,  environmentId);
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
                result = new ViewQueryBean(environmentId,
                    rs.getInt(COL_ID), 
                    DataUtilities.trimString(rs.getString("VIEWNAME")), 
                    DataUtilities.trimString(rs.getString(COL_STATUS)),
                    DataUtilities.trimString(rs.getString(COL_OUTPUTFORMAT)),
                    DataUtilities.trimString(rs.getString(COL_TYPE)), 
                    EditRights.Read, 
                    rs.getDate(COL_CREATETIME), 
                    DataUtilities.trimString(rs.getString(COL_CREATEBY)), 
                    rs.getDate(COL_MODIFYTIME), 
                    DataUtilities.trimString(rs.getString(COL_MODIFYBY)),
                    DataUtilities.trimString(rs.getString(COL_COMPILER)),
                    rs.getDate(COL_ACTIVATETIME), 
                    DataUtilities.trimString(rs.getString(COL_ACTIVATEBY)));
            }
            pst.close();
            rs.close();
        } catch (SQLException e) {
            throw DataUtilities.createDAOException(
                    "Database error occurred while querying View.", e);
        }
        return result;
    }

    public List<String> getViewSourceLogic(Integer envId, Integer viewId, Integer viewSourceId) {
        List<String> list;
        try {
            list = new ArrayList<String>();
            String extractFiltStmt = "SELECT EXTRACTFILTLOGIC FROM  "
                + params.getSchema() + ".VIEWSOURCE B "
                + "WHERE ENVIRONID = ? " 
                + "AND VIEWID = ? "
                + "AND VIEWSOURCEID = ?";
            ResultSet rs = null;
            PreparedStatement pextractFiltStmt;
            while (true) {
                try {
                    pextractFiltStmt = con.prepareStatement(extractFiltStmt);
                    pextractFiltStmt.setInt(1, envId);
                    pextractFiltStmt.setInt(2, viewId);                
                    pextractFiltStmt.setInt(3, viewSourceId);                
                    rs = pextractFiltStmt.executeQuery();
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
                String l = rs.getString("EXTRACTFILTLOGIC");
                if (l != null) {
                    if (!l.isEmpty()) {
                        list.add(l);
                    }
                }
            }
            rs.close();
            pextractFiltStmt.close();
                        
            String extractColStmt = "SELECT EXTRACTCALCLOGIC FROM  "
                + params.getSchema() + ".VIEWCOLUMNSOURCE "
                + "WHERE ENVIRONID = ? " 
                + "AND VIEWID = ? "
                + "AND VIEWSOURCEID = ?";
            rs = null;
            PreparedStatement pextractColStmt;
            while (true) {
                try {
                    pextractColStmt = con.prepareStatement(extractColStmt);
                    pextractColStmt.setInt(1, envId);
                    pextractColStmt.setInt(2, viewId);                
                    pextractColStmt.setInt(3, viewSourceId);                
                    rs = pextractColStmt.executeQuery();
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
                String l = rs.getString("EXTRACTCALCLOGIC");
                if (l != null) {
                    if (!l.isEmpty()) {
                        list.add(l);
                    }
                }
            }
            rs.close();
            pextractColStmt.close();
            
        } catch (SQLException e) {
            throw DataUtilities.createDAOException(
                "Database error occurred while querying View Source Logic.", e);
        }
        return list;        
    }
    
    public static YAMLViewTransfer getCurrentView() {
    	return ourViewTransfer;
    }
}
