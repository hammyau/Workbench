package com.ibm.safr.we.internal.data.dao.yamldao;

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
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.ViewColumnDAO;
import com.ibm.safr.we.data.transfer.ComponentAssociationTransfer;
import com.ibm.safr.we.data.transfer.ViewColumnTransfer;
import com.ibm.safr.we.data.transfer.ViewSourceTransfer;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewColumnTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewSourceTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewTransfer;
import com.ibm.safr.we.model.SAFRApplication;

public class YAMLViewColumnDAO implements ViewColumnDAO {

	static transient Logger logger = Logger.getLogger("com.ibm.safr.we.internal.data.dao.PGViewColumnDAO");

	private Connection con;
	private ConnectionParameters params;
	private UserSessionParameters safrLogin;
	private PGSQLGenerator generator = new PGSQLGenerator();
	private static int maxid;

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
	public YAMLViewColumnDAO(Connection con, ConnectionParameters params,
			UserSessionParameters safrLogin) {
		this.con = con;
		this.params = params;
		this.safrLogin = safrLogin;
	}

	public List<ViewColumnTransfer> getViewColumns(Integer viewId,	Integer environmentId) throws DAOException {
		maxid=1;
		List<ViewColumnTransfer> vcTransferList = new ArrayList<ViewColumnTransfer>();
		YAMLViewTransfer vt = YAMLViewDAO.getCurrentView();
		vt.getViewColumns().stream().forEach(c -> addColumnToTxfrList(vcTransferList, c));
		return vcTransferList;
	}

	private void addColumnToTxfrList(List<ViewColumnTransfer> vcTransferList, YAMLViewColumnTransfer c) {
		ViewColumnTransfer vct = c.getColumn();
		vct.setId(maxid++);
		vct.setEnvironmentId(0);
		vct.setPersistent(false);
		vcTransferList.add(vct);
	}

	public List<ViewColumnTransfer> persistViewColumns(List<ViewColumnTransfer> viewColTransferList) throws DAOException {

		//Make YAMLViewColumnTransfer contain a ViewColumnTransfer and ignore the bits we don't want at ViewColumnTransfer class
		YAMLViewTransfer vt = YAMLViewDAO.getCurrentView();
		List<YAMLViewColumnTransfer> ourViewColTransferList = new ArrayList<>(); 
		viewColTransferList.stream().forEach(c -> addToOurTransferlist(ourViewColTransferList, c));
		vt.setViewColumns(ourViewColTransferList);
		YAMLViewDAO.saveView(vt);
		return viewColTransferList;
	}

	private void addToOurTransferlist(List<YAMLViewColumnTransfer> ourViewColTransferList, ViewColumnTransfer c) {
		YAMLViewColumnTransfer yvct = new YAMLViewColumnTransfer();
		c.setId(c.getColumnNo());
		yvct.setColumn(c);
		ourViewColTransferList.add(yvct);
	}

	/*
	 * We need this do be done after the updates
	 * Consider the case where we add a column before an existing
	 * We get a new column to add
	 * And an old one to change to column number on
	 * This fixup happens AFTER the update 
	 * so we do not get two entries with the same column number
	 * That then messes things up later for the view column sources
	 */
    protected void fixUpCreatedColumns(List<ViewColumnTransfer> viewColCreate) {
        Map<Integer, Integer> idMap;
        try {
            idMap = getColIDMap(viewColCreate);
            for(Entry<Integer, Integer> e : idMap.entrySet()) {
            	System.out.println(e.getKey() + " " + e.getValue());
            }
            for (ViewColumnTransfer col : viewColCreate) {
                col.setPersistent(true);
                if (!col.isForImportOrMigration()) {
                    col.setId(idMap.get(col.getColumnNo()));
                }
            }
        } catch (SQLException e) {
			throw DataUtilities.createDAOException(
					"Unable to get column ID map", e);
        }
    }
	
    private void getXml(ViewColumnTransfer srcFld, StringBuffer buf) throws SQLException, DAOException {
        
        buf.append("  <VIEWID>"+ srcFld.getViewId() + "</VIEWID>\n");
        buf.append("  <COLUMNNUMBER>"+ srcFld.getColumnNo() + "</COLUMNNUMBER>\n");
        if (srcFld.getDataType() != null) {
            buf.append("  <FLDFMTCD>"+ srcFld.getDataType() + "</FLDFMTCD>\n");
        }
        buf.append("  <SIGNEDIND>"+ DataUtilities.booleanToInt(srcFld.isSigned()) + "</SIGNEDIND>\n");
        if (srcFld.getStartPosition() != null) {        
            buf.append("  <STARTPOSITION>"+ srcFld.getStartPosition() + "</STARTPOSITION>\n");
        }        
        buf.append("  <MAXLEN>"+ srcFld.getLength() + "</MAXLEN>\n");
        if (srcFld.getOrdinalPosition() != null) {
            buf.append("  <ORDINALPOSITION>"+ srcFld.getOrdinalPosition() + "</ORDINALPOSITION>\n");                
        }
        buf.append("  <DECIMALCNT>"+ srcFld.getDecimalPlaces() + "</DECIMALCNT>\n");
        buf.append("  <ROUNDING>"+ srcFld.getScalingFactor() + "</ROUNDING>\n");
        if (srcFld.getDateTimeFormat() != null) {
            buf.append("  <FLDCONTENTCD>"+ srcFld.getDateTimeFormat() + "</FLDCONTENTCD>\n");
        }
        if (srcFld.getDataAlignmentCode() != null) {
            buf.append("  <JUSTIFYCD>" + srcFld.getDataAlignmentCode() + "</JUSTIFYCD>\n");
        }
        if (srcFld.getDefaultValue() != null) {
            String str = generator.handleSpecialChars(srcFld.getDefaultValue());
            buf.append("  <DEFAULTVAL>"+ str + "</DEFAULTVAL>\n");                
        }
        buf.append("  <VISIBLE>"+ DataUtilities.booleanToInt(srcFld.isVisible()) + "</VISIBLE>\n");
        if (srcFld.getSubtotalTypeCode() != null) {
            buf.append("  <SUBTOTALTYPECD>"+ srcFld.getSubtotalTypeCode() + "</SUBTOTALTYPECD>\n");
        }
        buf.append("  <SPACESBEFORECOLUMN>"+ srcFld.getSpacesBeforeColumn() + "</SPACESBEFORECOLUMN>\n");
        if (srcFld.getExtractAreaCode() != null) {
            buf.append("  <EXTRACTAREACD>"+ srcFld.getExtractAreaCode() + "</EXTRACTAREACD>\n");
        }
        if (srcFld.getExtractAreaPosition()!= null) {
            buf.append("  <EXTRAREAPOSITION>"+ srcFld.getExtractAreaPosition() + "</EXTRAREAPOSITION>\n");
        }
        if (srcFld.getSortkeyFooterLabel() != null) {
            buf.append("  <SUBTLABEL>"+ srcFld.getSortkeyFooterLabel() + "</SUBTLABEL>\n");
        }            
        if (srcFld.getNumericMask() != null) {            
            buf.append("  <RPTMASK>"+ srcFld.getNumericMask() + "</RPTMASK>\n");
        }
        if (srcFld.getHeaderAlignment() != null) {
            buf.append("  <HDRJUSTIFYCD>" + srcFld.getHeaderAlignment() + "</HDRJUSTIFYCD>\n");
        }
        if (srcFld.getColumnHeading1() != null) {
            String heading = generator.handleSpecialChars(srcFld.getColumnHeading1());
            buf.append("  <HDRLINE1>"+ heading + "</HDRLINE1>\n");                                
        }                                        
        if (srcFld.getColumnHeading2() != null) {
            String heading = generator.handleSpecialChars(srcFld.getColumnHeading2());
            buf.append("  <HDRLINE2>"+ heading + "</HDRLINE2>\n");                                
        }                                        
        if (srcFld.getColumnHeading3() != null) {
            String heading = generator.handleSpecialChars(srcFld.getColumnHeading3());            
            buf.append("  <HDRLINE3>"+ heading + "</HDRLINE3>\n");                                
        }                
        if (srcFld.getFormatColumnLogic() != null) {            
            buf.append("  <FORMATCALCLOGIC><![CDATA["+ srcFld.getFormatColumnLogic() + "]]></FORMATCALCLOGIC>\n");
        }
    }
    
	
	
    protected Map<Integer, Integer> getColIDMap(List<ViewColumnTransfer> viewColCreateList) throws SQLException {
        
        Map<Integer, Integer> idMap = new HashMap<Integer, Integer>();
        List<Integer> colNoList = new ArrayList<Integer>();
        for (ViewColumnTransfer col : viewColCreateList) {
            colNoList.add(col.getColumnNo());
        }        
        String colNoStr = DataUtilities.integerListToString(colNoList);
        String chkSt = "select columnnumber,viewcolumnid " +
            "from " + params.getSchema() + ".viewcolumn " +
            "where environid=" + viewColCreateList.get(0).getEnvironmentId() + " " +
            "and viewid=" + viewColCreateList.get(0).getViewId() + " " +
            "and columnnumber in " + colNoStr;                    
        PreparedStatement pst = null;
        ResultSet rs = null;
        pst = con.prepareCall(chkSt);
        rs = pst.executeQuery();                    
        // form map of column number to id
        while (rs.next()) {
            Integer colNo = rs.getInt(1);
            Integer id = rs.getInt(2);
            idMap.put(colNo, id);
        }
        rs.close();
        pst.close();
        
        return idMap;
    }

    
    public void removeViewColumns(List<Integer> vwColumnIds, Integer environmentId) throws DAOException {
		if (vwColumnIds == null || vwColumnIds.size() == 0) {
			return;
		}
		try {
			String placeholders = generator.getPlaceholders(vwColumnIds.size());
			// deleting the column sources related to these View Columns.
			String deleteColSourcesQuery = "Delete From " + params.getSchema()
					+ ".VIEWCOLUMNSOURCE Where VIEWCOLUMNID IN ("
					+ placeholders + " ) AND ENVIRONID = ? ";
			PreparedStatement pst = null;

			while (true) {
				try {
					pst = con.prepareStatement(deleteColSourcesQuery);
					int ndx = 1;
					for(int i=0; i<vwColumnIds.size(); i++) {
						pst.setInt(ndx++, vwColumnIds.get(i));
					}
					pst.setInt(ndx, environmentId);
					pst.executeUpdate();
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

			// deleting Sort Keys related to these View Columns
			String deleteSortKeysQuery = "Delete From " + params.getSchema()
					+ ".VIEWSORTKEY Where VIEWCOLUMNID IN ("
					+ placeholders + " ) AND ENVIRONID = ? ";
			PreparedStatement pst1 = null;

			while (true) {
				try {
					pst1 = con.prepareStatement(deleteSortKeysQuery);
					int ndx = 1;
					for(int i=0; i<vwColumnIds.size(); i++) {
						pst1.setInt(ndx++, vwColumnIds.get(i));
					}
					pst1.setInt(ndx, environmentId);
					pst1.executeUpdate();
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
			pst1.close();

			// deleting the View Columns
			String deleteViewColumnQuery = "Delete From " + params.getSchema()
					+ ".VIEWCOLUMN Where VIEWCOLUMNID IN ( "
					+ placeholders + " ) AND ENVIRONID = ? ";
			PreparedStatement pst2 = null;

			while (true) {
				try {
					pst2 = con.prepareStatement(deleteViewColumnQuery);
					int ndx = 1;
					for(int i=0; i<vwColumnIds.size(); i++) {
						pst2.setInt(ndx++, vwColumnIds.get(i));
					}
					pst2.setInt(ndx, environmentId);
					pst2.executeUpdate();
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
			pst2.close();

		} catch (SQLException e) {
			throw DataUtilities.createDAOException("Database error occurred while deleting View Columns.", e);
		}

	}

}
