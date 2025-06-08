package com.ibm.safr.we.internal.data.dao.yamldao;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.ibm.safr.we.constants.LookupPathSourceFieldType;
import com.ibm.safr.we.data.ConnectionParameters;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DataUtilities;
import com.ibm.safr.we.data.UserSessionParameters;
import com.ibm.safr.we.data.dao.LookupPathStepDAO;
import com.ibm.safr.we.data.transfer.ComponentAssociationTransfer;
import com.ibm.safr.we.data.transfer.DependentComponentTransfer;
import com.ibm.safr.we.data.transfer.LRFieldTransfer;
import com.ibm.safr.we.data.transfer.LookupPathSourceFieldTransfer;
import com.ibm.safr.we.data.transfer.LookupPathStepTransfer;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.internal.data.PGSQLGenerator;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLogicalRecordTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLookupPathSourceFieldTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLookupPathStepTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLookupTransfer;
import com.ibm.safr.we.model.SAFRApplication;

public class YAMLLookupPathStepDAO implements LookupPathStepDAO {

	static transient Logger logger = Logger.getLogger("com.ibm.safr.we.internal.data.dao.PGLookupPathStepDAO");

	private static final String TABLE_NAME_STEP = "LOOKUPSTEP";
	private static final String TABLE_NAME_SOURCEFIELD = "LOOKUPSRCKEY";
	private static final String COL_ENVID = "ENVIRONID";
	private static final String COL_ID = "LOOKUPSTEPID";
	private static final String COL_LOOKUPID = "LOOKUPID";
	private static final String COL_STEPSEQNBR = "STEPSEQNBR";
	private static final String COL_SOURCE = "SRCLRID";
	private static final String COL_LRLFASSOCID = "LRLFASSOCID";
	private static final String COL_CREATETIME = "CREATEDTIMESTAMP";
	private static final String COL_CREATEBY = "CREATEDUSERID";
	private static final String COL_MODIFYTIME = "LASTMODTIMESTAMP";
	private static final String COL_MODIFYBY = "LASTMODUSERID";

	private static final String COL_KEYSEQNBR = "KEYSEQNBR";

	private Connection con;
	private ConnectionParameters params;
	private UserSessionParameters safrLogin;
	private PGSQLGenerator generator = new PGSQLGenerator();

	private static YAMLLookupTransfer ourLkTxf;
	private static YAMLLookupPathStepTransfer currentStep;

	public YAMLLookupPathStepDAO(Connection con, ConnectionParameters params, UserSessionParameters safrLogin) {
		this.con = con;
		this.params = params;
		this.safrLogin = safrLogin;
	}

	public List<LookupPathStepTransfer> getAllLookUpPathSteps(Integer environmentId, Integer lookupPathId) throws DAOException {
		List<LookupPathStepTransfer> result = new ArrayList<LookupPathStepTransfer>();
		ourLkTxf = YAMLLookupDAO.getCurrentLKTransfer();;
		ourLkTxf.getSteps().stream().forEach(st -> addToStepResult(result, st));   
		return result;
	}

    private void addToStepResult(List<LookupPathStepTransfer> result, YAMLLookupPathStepTransfer lk) {
		List<ComponentAssociationTransfer> lfa = DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getAssociatedLogicalFiles(ourLkTxf.getTargetLR(), null);
		ComponentAssociationTransfer lrlfa =  DAOFactoryHolder.getDAOFactory().getLogicalRecordDAO().getLRLFAssociation(ourLkTxf.getTargetLR(), ourLkTxf.getTargetLF(), null);
		if(lrlfa == null) {
			logger.severe("No association for target LR " + ourLkTxf.getTargetLR());
		} else {
			lk.setTargetXLRFileId(lrlfa.getAssociationId());
		}
		result.add(lk);
	}

	public DependentComponentTransfer getAssociatedLFDependency(
        Integer environmentId, Integer lrlfId) throws DAOException  {
        DependentComponentTransfer depCompTransfer = null;

        String schema = params.getSchema();
        
        String selectString =  "SELECT A.LOGFILEID,A.NAME " + 
                               "FROM " + 
                               schema + ".LOGFILE A, " +
                               schema + ".LRLFASSOC B, " +
                               schema + ".LOGREC C " +
                               "WHERE A.ENVIRONID=B.ENVIRONID " +
                               "AND A.LOGFILEID=B.LOGFILEID " +
                               "AND B.ENVIRONID=? " +
                               "AND B.LRLFASSOCID=? " +
                               "AND B.ENVIRONID=C.ENVIRONID " +
                               "AND B.LOGRECID=C.LOGRECID " +
                               "AND ( C.LOOKUPEXITID IS NULL OR " +
                               "      C.LOOKUPEXITID=0 ) " +
                               "AND ( SELECT COUNT(*) FROM " + 
                               schema + ".LFPFASSOC D " + 
                               "      WHERE B.ENVIRONID=D.ENVIRONID " +
                               "      AND B.LOGFILEID=D.LOGFILEID) > 1;";

        try {
            PreparedStatement pst = null;
            ResultSet rs = null;
            while (true) {
                try {
                    pst = con.prepareStatement(selectString);
                    pst.setInt(1, environmentId);
                    pst.setInt(2, lrlfId);
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
                Integer joinId = rs.getInt("LOGFILEID");                
                depCompTransfer = new DependentComponentTransfer();
                depCompTransfer.setId(joinId);
                depCompTransfer.setName(DataUtilities.trimString(rs.getString("NAME")));
                depCompTransfer.setDependencyInfo("[Logical File Properties]");                
            }
            pst.close();
            rs.close();
        } catch (SQLException e) {
            throw DataUtilities.createDAOException(
                "Database error occurred while retrieving the list of dependencies of Lookup Path to an LF.",e);
        }
        
        return depCompTransfer;
    }
	
	public LookupPathStepTransfer getLookUpPathStep(Integer environmentId,	Integer lookupPathStepId) throws DAOException {
		LookupPathStepTransfer result = null;
		ourLkTxf = YAMLLookupDAO.getCurrentLKTransfer();
		result = ourLkTxf.getSteps().get(lookupPathStepId);
		return result;
	}

    public int getTargetLookUpPathLrId(Integer environmentId, String lookupPathName) throws DAOException {
        int result = 0;
        try {
            String selectString = "SELECT A.LOGRECID FROM " +
                params.getSchema() + ".LRLFASSOC A," +
                params.getSchema() + ".LOOKUPSTEP B " +
                "WHERE A.ENVIRONID=B.ENVIRONID " +
                "AND A.LRLFASSOCID=B.LRLFASSOCID " +
                "AND B.STEPSEQNBR = (" +
                "SELECT MAX(C.STEPSEQNBR) FROM " +
                params.getSchema() + ".LOOKUPSTEP C," +
                params.getSchema() + ".LOOKUP D " +
                "WHERE C.ENVIRONID=D.ENVIRONID "+
                "AND C.LOOKUPID=D.LOOKUPID "+
                "AND B.LOOKUPID=D.LOOKUPID "+
                "AND B.ENVIRONID=D.ENVIRONID " +
                "AND D.NAME=? " +
                "AND D.ENVIRONID=? " +
                "GROUP BY D.LOOKUPID)";
            PreparedStatement pst = null;
            ResultSet rs = null;
            while (true) {
                try {
                    pst = con.prepareStatement(selectString);
                    int i = 1;
                    pst.setString(i++, lookupPathName);
                    pst.setInt(i++, environmentId);
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
                result = rs.getInt(1);
            } 
            pst.close();
            rs.close();
        } catch (SQLException e) {
            throw DataUtilities.createDAOException(
                "Database error occurred while retrieving the Lookup Path Step.",e);
        }
        return result;
    }
	
		
	public LookupPathStepTransfer persistLookupPathStep(LookupPathStepTransfer lkupPathStepTransfer) throws DAOException {
		if (!lkupPathStepTransfer.isPersistent()) {
			return (createLookupPathStep(lkupPathStepTransfer));
		} else {
			return (updateLookupPathStep(lkupPathStepTransfer));
		}
	}

	/**
	 * This method is to create a Lookup Path Step in LOOKUPSTEP.
	 * 
	 * @param lkupPathStepTransfer
	 *            : The transfer object which has the values for the columns of
	 *            the Lookup Path Step which is being created.
	 * @return A LookupPathStepTransfer object.
	 * @throws DAOException
	 */
	private LookupPathStepTransfer createLookupPathStep(LookupPathStepTransfer lkupPathStepTransfer) throws DAOException {
		YAMLLookupDAO lkdao = (YAMLLookupDAO)DAOFactoryHolder.getDAOFactory().getLookupDAO();
		ourLkTxf = YAMLLookupDAO.getCurrentLKTransfer();
		currentStep = new YAMLLookupPathStepTransfer();
		currentStep.setEnvironmentId(lkupPathStepTransfer.getEnvironmentId());
		currentStep.setId(lkupPathStepTransfer.getSequenceNumber());
		currentStep.setJoinId(lkupPathStepTransfer.getJoinId());
		currentStep.setName(lkupPathStepTransfer.getName());
		currentStep.setSequenceNumber(lkupPathStepTransfer.getSequenceNumber());
		currentStep.setSourceLRId(lkupPathStepTransfer.getSourceLRId());
		ourLkTxf.addStep(currentStep);
		//But need to add it to the YAMLLogicalFileTransfer object
//        for (LRFieldTransfer lrft : lrFieldCreateList) {
//        	lrt.addField(lrft);
//        }
		//We could have a set parent function to save the reread?
//		Path lksPath = YAMLizer.getLookupsPath();
//		Path lkPath = lksPath.resolve(lkt.getName() + ".yaml");
//		YAMLizer.writeYaml(lkPath, lkt);
		return lkupPathStepTransfer;
	}

	/**
	 * This method is to update a Lookup Path Step in LOOKUPSTEP.
	 * 
	 * @param lkupPathStepTransfer
	 *            : The transfer object which has the values for the columns of
	 *            the Lookup Path Step which is being updated.
	 * @return A LookupPathStepTransfer object.
	 */
	private LookupPathStepTransfer updateLookupPathStep(LookupPathStepTransfer lkupPathStepTransfer) throws DAOException {
		
		YAMLLookupDAO lkdao = (YAMLLookupDAO)DAOFactoryHolder.getDAOFactory().getLookupDAO();
		ourLkTxf = YAMLLookupDAO.getCurrentLKTransfer();
		currentStep = new YAMLLookupPathStepTransfer();
		currentStep.setEnvironmentId(lkupPathStepTransfer.getEnvironmentId());
		currentStep.setId(lkupPathStepTransfer.getSequenceNumber());
		currentStep.setJoinId(lkupPathStepTransfer.getJoinId());
		currentStep.setName(lkupPathStepTransfer.getName());
		currentStep.setSequenceNumber(lkupPathStepTransfer.getSequenceNumber());
		currentStep.setSourceLRId(lkupPathStepTransfer.getSourceLRId());
		ourLkTxf.addStep(currentStep);
		return lkupPathStepTransfer;
	}

	public void removeLookupPathStep(Integer lkupPathStepId,
			Integer environmentId) throws DAOException {

		try {
			// first remove all the source Fields for that step from
			// LOOKUPSRCKEY
			removeLookupPathStepSourceField(lkupPathStepId, environmentId);

			// remove the Lookup Path Step
			List<String> idNames = new ArrayList<String>();
			idNames.add(COL_ID);
			idNames.add(COL_ENVID);

			String statement = generator.getDeleteStatement(params.getSchema(),
					TABLE_NAME_STEP, idNames);
			PreparedStatement pst = null;

			while (true) {
				try {
					pst = con.prepareStatement(statement);
					pst.setInt(1, lkupPathStepId);
					pst.setInt(2, environmentId);
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
			throw DataUtilities.createDAOException("Database error occurred while deleting the Lookup Path Step.",e);
		}

	}

	public List<LookupPathSourceFieldTransfer> getLookUpPathStepSourceFields(Integer environmentId, Integer lookupPathStepId) throws DAOException {
		ourLkTxf = YAMLLookupDAO.getCurrentLKTransfer();;
		YAMLLookupPathStepTransfer step = ourLkTxf.getSteps().get(lookupPathStepId - 1);
		return  step.getSources();
	}

    public void persistLookupPathStepsSourceFields(List<Integer> lookupPathStepIds, List<LookupPathSourceFieldTransfer> sourceFieldsTrans) throws DAOException {
        // Delete any existing SourceFields for list of Lookup Path Steps
        //removeLookupPathStepsSourceField(lookupPathStepIds, sourceFieldsTrans.get(0).getEnvironmentId());
        for (LookupPathSourceFieldTransfer sourceField : sourceFieldsTrans) {
        	sourceField.setId(sourceField.getKeySeqNbr());
        	((YAMLLookupPathStepTransfer)currentStep).addSource(sourceField);
        }
        saveLookup();
    }

	private void saveLookup() {
		Path lksPath = YAMLizer.getLookupsPath();
		lksPath.toFile().mkdirs();
		Path lkPath = lksPath.resolve(ourLkTxf.getName()+ ".yaml");
		YAMLizer.writeYaml(lkPath, ourLkTxf);
	}
    
    private List<LookupPathSourceFieldTransfer> createLookupPathStepSourceFields(
            List<LookupPathSourceFieldTransfer> srcFlds) throws DAOException {
        try {    

			String statement = generator.getSelectFromFunction(params.getSchema(), "insertlookupsrcfieldWithId", 1);
			if (srcFlds.isEmpty() || !srcFlds.get(0).isForImportOrMigration()) {
            	statement = generator.getSelectFromFunction(params.getSchema(), "insertlookupsrcfield", 1);
			}
			PreparedStatement proc = null;
            
            proc = con.prepareStatement(statement);
            String xmlVal = getCreateArgs(srcFlds);            
            proc.setString(1, xmlVal);
            proc.execute();
            proc.close();
            
        } catch (SQLException e) {
            throw DataUtilities.createDAOException("Database error occurred while inserting lookup fields.", e);
        }
        return srcFlds;        
    }
	
    private String getCreateArgs(List<LookupPathSourceFieldTransfer> srcFlds) throws SQLException {
        StringBuffer buf = new StringBuffer();
        buf.append("<Root>\n");
        for (LookupPathSourceFieldTransfer srcFld : srcFlds) {
            buf.append(" <Record>\n");
            buf.append("  <ENVIRONID>"+ srcFld.getEnvironmentId() + "</ENVIRONID>\n");
            buf.append("  <LOOKUPSTEPID>"+ srcFld.getLookupPathStepId() + "</LOOKUPSTEPID>\n");
            buf.append("  <KEYSEQNBR>"+ srcFld.getKeySeqNbr() + "</KEYSEQNBR>\n");
            buf.append("  <FLDTYPE>"+ enumToInt(srcFld.getSourceFieldType()) + "</FLDTYPE>\n");
            if(srcFld.getSourceXLRFLDId() != null) {
            	buf.append("  <LRFIELDID>"+ srcFld.getSourceXLRFLDId() + "</LRFIELDID>\n");
            }
            if(srcFld.getSourceXLRFileId() != null && srcFld.getSourceXLRFileId() != 0) {
            	buf.append("  <LRLFASSOCID>"+ srcFld.getSourceXLRFileId() + "</LRLFASSOCID>\n");
            }
            buf.append("  <LOOKUPID>"+ srcFld.getSourceJoinId() + "</LOOKUPID>\n");            
            if (srcFld.getDataType() != null) {
                buf.append("  <VALUEFMTCD>"+ srcFld.getDataType() + "</VALUEFMTCD>\n");
            }
            buf.append("  <SIGNED>"+ DataUtilities.booleanToInt(srcFld.isSigned()) + "</SIGNED>\n");
            buf.append("  <VALUELEN>"+ srcFld.getLength() + "</VALUELEN>\n");
            buf.append("  <DECIMALCNT>"+ srcFld.getDecimalPlaces() + "</DECIMALCNT>\n");
            if (srcFld.getDateTimeFormat() != null) {
                buf.append("  <FLDCONTENTCD>"+ srcFld.getDateTimeFormat() + "</FLDCONTENTCD>\n");
            }
            buf.append("  <ROUNDING>"+ srcFld.getScalingFactor() + "</ROUNDING>\n");
            buf.append("  <JUSTIFYCD>LEFT</JUSTIFYCD>\n");
            if (srcFld.getNumericMask() != null) {            
                buf.append("  <MASK>"+ srcFld.getNumericMask() + "</MASK>\n");
            }
            if (srcFld.getSymbolicName() != null) {  
                String str = generator.handleSpecialChars(srcFld.getSymbolicName());                                
                buf.append("  <SYMBOLICNAME>"+ str + "</SYMBOLICNAME>\n");
            }
            if (srcFld.getSourceValue() != null) {       
                String str = generator.handleSpecialChars(srcFld.getSourceValue());                
                buf.append("  <VALUE>"+ str + "</VALUE>\n");
            }            
            if (srcFld.isForImportOrMigration()) {
                buf.append("  <CREATEDTIMESTAMP>"+ generator.genTimeParm(srcFld.getCreateTime()) + "</CREATEDTIMESTAMP>\n");
                buf.append("  <CREATEDUSERID>"+ srcFld.getCreateBy() + "</CREATEDUSERID>\n");
                buf.append("  <LASTMODTIMESTAMP>"+ generator.genTimeParm(srcFld.getModifyTime()) + "</LASTMODTIMESTAMP>\n");
                buf.append("  <LASTMODUSERID>"+ srcFld.getModifyBy() + "</LASTMODUSERID>\n");
            }
            else {
                buf.append("  <CREATEDUSERID>"+ safrLogin.getUserId() + "</CREATEDUSERID>\n");
                buf.append("  <LASTMODUSERID>"+ safrLogin.getUserId() + "</LASTMODUSERID>\n");
            }
            
            buf.append(" </Record>\n");            
        }
        buf.append("</Root>");
        return buf.toString();
    }
    
    public void removeLookupPathStepsSourceField(List<Integer> lkupPathStepIds, Integer environmentId) throws DAOException {
    	YAMLLookupTransfer lkTxf = YAMLLookupDAO.getCurrentLKTransfer();
    	lkTxf.clearSteps();
//        try {
//            SAFRApplication.getTimingMap().startTiming("PGLookupPathStepDAO.removeLookupPathStepsSourceField");
//            String placeholders = generator.getPlaceholders(lkupPathStepIds.size());
//            
//            String statement = "DELETE FROM "
//                + params.getSchema()
//                + "." + TABLE_NAME_SOURCEFIELD + " WHERE "
//                + COL_ENVID + "= ? AND "
//                + COL_ID + " IN (" + placeholders + " )"; 
//            
//            PreparedStatement pst = null;
//
//            while (true) {
//                try {
//                    pst = con.prepareStatement(statement);
//                    int ndx = 1;
//                    pst.setInt(ndx++, environmentId);
//            		for( int i = 0 ; i < lkupPathStepIds.size(); i++ ) {
//                        pst.setInt(ndx++, lkupPathStepIds.get(i));
//            		}
//                    pst.execute();
//                    break;
//                } catch (SQLException se) {
//                    if (con.isClosed()) {
//                        // lost database connection, so reconnect and retry
//                        con = DAOFactoryHolder.getDAOFactory().reconnect();
//                    } else {
//                        throw se;
//                    }
//                }
//            }
//            pst.close();
//
//        } catch (SQLException e) {
//            throw DataUtilities.createDAOException("Database error occurred while deleting the source fields belonging to the specified Lookup Path Step.",e);
//        }
//        SAFRApplication.getTimingMap().stopTiming("PGLookupPathStepDAO.removeLookupPathStepsSourceField");
       
    }
	
	public void removeLookupPathStepSourceField(Integer lkupPathStepId,
			Integer environmentId) throws DAOException {
		try {
			List<String> idNames = new ArrayList<String>();
			idNames.add(COL_ID);
			idNames.add(COL_ENVID);

			String statement = generator.getDeleteStatement(params.getSchema(),
					TABLE_NAME_SOURCEFIELD, idNames);
			PreparedStatement pst = null;

			while (true) {
				try {
					pst = con.prepareStatement(statement);
					pst.setInt(1, lkupPathStepId);
					pst.setInt(2, environmentId);
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
			throw DataUtilities.createDAOException("Database error occurred while deleting the source fields belonging to the specified Lookup Path Step.",e);
		}
		
	}
	
	public LookupPathStepTransfer getCurrentStep() {
		return currentStep;
	}

	/**
	 * This method is to convert an integer into its equivalent enum value of
	 * sourceFieldType.
	 * 
	 * @param sourceFieldTypeInt
	 *            : The integer returned from the database
	 * @return : The equivalent enum value.
	 */
	private LookupPathSourceFieldType intToEnum(int sourceFieldTypeInt) {
		if (sourceFieldTypeInt == 0) {
			return LookupPathSourceFieldType.LRFIELD;
		} else if (sourceFieldTypeInt == 1) {
			return LookupPathSourceFieldType.CONSTANT;
		} else if (sourceFieldTypeInt == 3) {
			return LookupPathSourceFieldType.SYMBOL;
		} else {
			return null;
		}
	}

	/**
	 * This method is to convert enum value of edit rights into an integer.
	 * 
	 * @param sourceFieldTypeEnum
	 *            : The enum value of sourceFieldType.
	 * @return : The equivalent integer value.
	 */
	private int enumToInt(LookupPathSourceFieldType sourceFieldTypeEnum) {
		if (sourceFieldTypeEnum == LookupPathSourceFieldType.LRFIELD) {
			return 0;
		} else if (sourceFieldTypeEnum == LookupPathSourceFieldType.CONSTANT) {
			return 1;
		} else if (sourceFieldTypeEnum == LookupPathSourceFieldType.SYMBOL) {
			return 3;
		} else {
			return 10;
		}
	}

}
