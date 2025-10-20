package com.ibm.safr.we.model.view;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.flogger.FluentLogger;
import com.ibm.safr.we.constants.CodeCategories;
import com.ibm.safr.we.constants.Codes;
import com.ibm.safr.we.constants.ComponentType;
import com.ibm.safr.we.constants.OutputFormat;
import com.ibm.safr.we.constants.SAFRPersistence;
import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DAOUOWInterruptedException;
import com.ibm.safr.we.data.transfer.HeaderFooterItemTransfer;
import com.ibm.safr.we.data.transfer.ViewColumnSourceTransfer;
import com.ibm.safr.we.data.transfer.ViewColumnTransfer;
import com.ibm.safr.we.data.transfer.ViewLogicDependencyTransfer;
import com.ibm.safr.we.data.transfer.ViewSortKeyTransfer;
import com.ibm.safr.we.data.transfer.ViewSourceTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewSourceTransfer;
import com.ibm.safr.we.data.transfer.ViewTransfer;
import com.ibm.safr.we.exceptions.SAFRCancelException;
import com.ibm.safr.we.exceptions.SAFRException;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.exceptions.SAFRValidationException;
import com.ibm.safr.we.model.SAFRApplication;
import com.ibm.safr.we.model.associations.ComponentAssociation;
import com.ibm.safr.we.model.associations.SAFRAssociationFactory;
import com.ibm.safr.we.model.base.SAFRComponent;
import com.ibm.safr.we.model.base.SAFRPersistentObject;
import com.ibm.safr.we.model.query.LookupQueryBean;
import com.ibm.safr.we.model.ControlRecord;

/**
 * 
 * Treat the view as one thing.
 * 
 *
 */
public class WholeView extends View {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

	public WholeView(Integer environmentId) {
		super(environmentId);
		logger.atInfo().log("Hey make the whole thing");
	}

	public WholeView(ViewTransfer viewTransfer) {
		//get dependent components before constructing the super
		//Or that should be done in the DAO?
		super(viewTransfer);
	}

	@Override
	public void store() throws SAFRException, DAOException {
		logger.atInfo().log("Hey store the whole thing");
		ViewTransfer viewTrans = DAOFactoryHolder.getDAOFactory().getViewDAO().getView(getId(), getEnvironmentId());
		setTransferData(viewTrans);
		viewTrans.setFormatFilterlogic(super.getFormatRecordFilter());
		viewTrans.setForImport(isForImport()); // retain import flag
		setObjectData(viewTrans);

		// save the ViewSource contents
		List<ViewSourceTransfer> transList = new ArrayList<ViewSourceTransfer>();
		HashMap<ViewSourceTransfer, ViewSource> viewSourceMap = new HashMap<ViewSourceTransfer, ViewSource>();
		for (ViewSource viewSource : super.getViewSources()) {
			ViewSourceTransfer viewSourceTransfer = new ViewSourceTransfer();
			viewSource.setTransferData(viewSourceTransfer);
			YAMLViewSourceTransfer yviewSourceTransfer = new YAMLViewSourceTransfer(viewSourceTransfer);
			yviewSourceTransfer.setLogicalRecord(viewSource.getLrFileAssociation().getAssociatingComponentName());
			yviewSourceTransfer.setLogicalFile(viewSource.getLrFileAssociation().getAssociatedComponentName());
			
			transList.add(yviewSourceTransfer);
			viewSourceMap.put(viewSourceTransfer,viewSource);
		}
		DAOFactoryHolder.getDAOFactory().getViewSourceDAO().persistViewSources(transList);
//		for (int i = 0; i < transList.size(); i++) {
//			ViewSourceTransfer viewSourceTrans = transList.get(i);
//			ViewSource tmpSrc = viewSourceMap.get(viewSourceTrans);
//			tmpSrc.setObjectData(viewSourceTrans);
//		}
		List<ViewColumnTransfer> colTransList = new ArrayList<ViewColumnTransfer>();
		HashMap<ViewColumnTransfer, ViewColumn> viewColumnMap = new HashMap<ViewColumnTransfer, ViewColumn>();
		for (ViewColumn viewColumn : super.getViewColumns()) {
			ViewColumnTransfer viewColumnTransfer = new ViewColumnTransfer();
			viewColumn.setTransferData(viewColumnTransfer);
			colTransList.add(viewColumnTransfer);
			viewColumnMap.put(viewColumnTransfer, viewColumn);
		}
		if (colTransList.size() > 0) {
			DAOFactoryHolder.getDAOFactory().getViewColumnDAO().persistViewColumns(colTransList);
			for (int i = 0; i < colTransList.size(); i++) {
				ViewColumnTransfer viewColumnTrans = colTransList.get(i);
				ViewColumn tmpCol = viewColumnMap.get(viewColumnTrans);
				tmpCol.setObjectData(viewColumnTrans);
			}
		}

		List<ViewColumnSourceTransfer> colSourceTransList = new ArrayList<ViewColumnSourceTransfer>();
		HashMap<ViewColumnSourceTransfer, ViewColumnSource> viewColumnSourceMap = new HashMap<ViewColumnSourceTransfer, ViewColumnSource>();
		for (ViewColumnSource viewColumnSource : super.getViewColumnSources()) {

			ViewColumnSourceTransfer viewColumnSourceTransfer = new ViewColumnSourceTransfer();
			viewColumnSource.setTransferData(viewColumnSourceTransfer);
			colSourceTransList.add(viewColumnSourceTransfer);
			viewColumnSourceMap.put(viewColumnSourceTransfer,viewColumnSource);
		}
		if (colSourceTransList.size() > 0) {
			DAOFactoryHolder.getDAOFactory().getViewColumnSourceDAO().persistViewColumnSources(colSourceTransList);
			for (int i = 0; i < colSourceTransList.size(); i++) {
				ViewColumnSourceTransfer viewColumnSourceTrans = colSourceTransList.get(i);
				ViewColumnSource tmpViewCS = viewColumnSourceMap.get(viewColumnSourceTrans);
				tmpViewCS.setObjectData(viewColumnSourceTrans);
			}
		}

		// save view sort key contents.
		List<ViewSortKeyTransfer> sortKeyTransList = new ArrayList<ViewSortKeyTransfer>();
		HashMap<ViewSortKeyTransfer, ViewSortKey> viewSortKeyMap = new HashMap<ViewSortKeyTransfer, ViewSortKey>();
		for (ViewSortKey viewSortKey : super.getViewSortKeys()) {
			ViewSortKeyTransfer viewSortKeyTransfer = new ViewSortKeyTransfer();
			viewSortKey.setTransferData(viewSortKeyTransfer);
			sortKeyTransList.add(viewSortKeyTransfer);
			viewSortKeyMap.put(viewSortKeyTransfer, viewSortKey);
		}
		if (sortKeyTransList.size() > 0) {
			DAOFactoryHolder.getDAOFactory().getViewSortKeyDAO().persistViewSortKeys(sortKeyTransList);
			for (int i = 0; i < sortKeyTransList.size(); i++) {
				ViewSortKeyTransfer viewSortKeyTrans = sortKeyTransList.get(i);
				ViewSortKey tmpSK = viewSortKeyMap.get(viewSortKeyTrans);
				tmpSK.setObjectData(viewSortKeyTrans);
			}
		}
		// save view dependencies
		if (this.statusCode.getGeneralId() == Codes.INACTIVE ){
			viewLogicDependencies = null; // dependency not applicable
		}
		List<ViewLogicDependencyTransfer> logicDependenciesTransfers = new ArrayList<ViewLogicDependencyTransfer>();
		if (viewLogicDependencies != null) {
			for (ViewLogicDependency viewLogicDepend : viewLogicDependencies) {
				ViewLogicDependencyTransfer trans = new ViewLogicDependencyTransfer();
				viewLogicDepend.setTransferData(trans);
				logicDependenciesTransfers.add(trans);
			}
		}
		DAOFactoryHolder.getDAOFactory().getViewLogicDependencyDAO().persistViewLogicDependencies(logicDependenciesTransfers, getId(),getEnvironmentId());

		viewTrans = DAOFactoryHolder.getDAOFactory().getViewDAO().persistView(viewTrans);

	}
	
    @Override
    public SAFRComponent saveAs(String newName) throws SAFRValidationException, SAFRException {
        
            View viewCopy = SAFRApplication.getSAFRFactory().createView();
            
            viewCopy.setConfirmWarningStrategy(this.getConfirmWarningStrategy());

            // copy the general properties...
            viewCopy.setName(newName);
            viewCopy.setStatusCode(SAFRApplication.getSAFRFactory()
                    .getCodeSet(CodeCategories.VIEWSTATUS)
                    .getCode(Codes.INACTIVE));

            viewCopy.setOutputFormat(this.getOutputFormat());
            viewCopy.setExtractAggregateBufferSize(this.getExtractAggregateBufferSize());
            viewCopy.setExtractAggregateBySortKey(this.isExtractAggregateBySortKey());
            viewCopy.setComment(this.getComment());
            ControlRecord cr = this.getControlRecord();
            cr.saveAs(cr.getName());
            viewCopy.setControlRecord(cr);
            
            viewCopy.setExtractFileAssociation(this.getExtractFileAssociation());
            viewCopy.setExtractMaxRecords(this.getExtractMaxRecords());
            viewCopy.setExtractPhaseOutputLimit(this.extractPhaseOutputLimit);
            viewCopy.setExtractWorkFileNo(this.getExtractWorkFileNo());
            viewCopy.setFileFieldDelimiterCode(this.getFileFieldDelimiterCode());
            viewCopy.setFileStringDelimiterCode(this
                    .getFileStringDelimiterCode());
            // Jaydeep 18th June 2010: CQ 8118 : added setFormatPhaseUsage which
            // was missing.
            viewCopy.setFormatExit(this.getFormatExit());
            viewCopy.setFormatExitParams(this.getFormatExitParams());
            viewCopy.setFormatPhaseOutputLimit(this.formatPhaseOutputLimit);
            viewCopy.setFormatPhaseRecordAggregationOn(this
                    .isFormatPhaseRecordAggregationOn());
            viewCopy.setFormatRecordFilter(this.getFormatRecordFilter());
            viewCopy.setLinesPerPage(this.getLinesPerPage());
            viewCopy.setOutputMaxRecCount(this.getOutputMaxRecCount());
            viewCopy.setReportWidth(this.getReportWidth());
            viewCopy.setSuppressZeroRecords(this.suppressZeroRecords);
            viewCopy.setHeaderRow(this.headerRow);
            viewCopy.setWriteExit(this.getWriteExit());
            viewCopy.setWriteExitParams(this.getWriteExitParams());
            if (this.outputLRId != null && this.outputLRId > 0) {
                viewCopy.setOutputLRId(this.outputLRId);
            }

            // copy View Sources...
            for (ViewSource viewSource : this.getViewSources().getActiveItems()) {
                ViewSource vwSource = viewCopy.addViewSource();
                vwSource.setSequenceNo(viewSource.getSequenceNo());
                vwSource.setLrFileAssociation(viewSource.getLrFileAssociation());
                vwSource.setExtractRecordFilter(viewSource.getExtractRecordFilter());
                vwSource.setExtractFileAssociation(viewSource.getExtractFileAssociation());
                vwSource.setWriteExit(viewSource.getWriteExit());
                vwSource.setWriteExitParams(viewSource.getWriteExitParams());
                vwSource.setExtractOutputOverride(viewSource.isExtractOutputOverriden());
                vwSource.setExtractRecordOutput(viewSource.getExtractRecordOutput());
            }
            // copy ViewColumn Contents
            for (ViewColumn viewColumn : this.getViewColumns().getActiveItems()) {
                ViewColumn vwColumn = viewCopy.addViewColumn(0);

                // copy View Column sources.
                List<ViewColumnSource> oldViewColumnSources = viewColumn
                        .getViewColumnSources().getActiveItems();

                for (int i = 0; i < vwColumn.getViewColumnSources().size(); i++) {
                    ViewColumnSource viewColumnSource = oldViewColumnSources
                            .get(i);
                    ViewColumnSource newViewColumnSource = vwColumn
                            .getViewColumnSources().get(i);
                    // CQ 8056. Nikita. 22/06/2010.
                    // Source type should be set first to avoid resetting of
                    // other fields.
                    newViewColumnSource.setSourceType(viewColumnSource
                            .getSourceType());
                    if (viewColumnSource.getSourceType().getGeneralId() == Codes.SOURCE_FILE_FIELD) {
                        newViewColumnSource.setLRFieldColumn(viewColumnSource
                                .getLRField());
                    } else if (viewColumnSource.getSourceType().getGeneralId() == Codes.LOOKUP_FIELD) {
                        newViewColumnSource.setLRFieldColumn(viewColumnSource
                                .getLRField());
                    }
                    newViewColumnSource.setSourceValue(viewColumnSource
                            .getSourceValue());
                    if(viewColumnSource.getEffectiveDateLRField() != null) {
                    	newViewColumnSource.setEffectiveDateLRField(viewColumnSource.getEffectiveDateLRField());
                    }
                    newViewColumnSource
                            .setEffectiveDateTypeCode(viewColumnSource
                                    .getEffectiveDateTypeCode());
                    newViewColumnSource.setEffectiveDateValue(viewColumnSource
                            .getEffectiveDateValue());
                    newViewColumnSource
                            .setExtractColumnAssignment(viewColumnSource
                                    .getExtractColumnAssignment());
                    newViewColumnSource
                            .setLogicalRecordQueryBean(viewColumnSource
                                    .getLogicalRecordQueryBean());
                    newViewColumnSource.setLookupQueryBean(viewColumnSource
                            .getLookupQueryBean());
                    newViewColumnSource
                            .setSortKeyTitleLogicalRecordQueryBean(viewColumnSource
                                    .getSortKeyTitleLogicalRecordQueryBean());
                    LookupQueryBean lqb =viewColumnSource.getSortKeyTitleLookupPathQueryBean();
                    if(lqb != null) {
                    	newViewColumnSource.setSortKeyTitleLookupPathQueryBean(lqb);
                    }
                    if(viewColumnSource.getSortKeyTitleLRField() != null) {
                        newViewColumnSource.setSortKeyTitleLRField(viewColumnSource.getSortKeyTitleLRField());
                    }

                }

                vwColumn.setColumnNo(viewColumn.getColumnNo());
                vwColumn.setDataAlignmentCode(viewColumn.getDataAlignmentCode());
                vwColumn.setDataTypeCode(viewColumn.getDataTypeCode());
                vwColumn.setDateTimeFormatCode(viewColumn
                        .getDateTimeFormatCode());
                vwColumn.setDecimals(viewColumn.getDecimals());
                vwColumn.setDefaultValue(viewColumn.getDefaultValue());
                vwColumn.setExtractAreaCode(viewColumn.getExtractAreaCode());
                vwColumn.setExtractAreaPosition(viewColumn
                        .getExtractAreaPosition());
                vwColumn.setFormatColumnCalculation(viewColumn
                        .getFormatColumnCalculation());
                vwColumn.setGroupAggregationCode(viewColumn
                        .getGroupAggregationCode());
                vwColumn.setHeaderAlignmentCode(viewColumn
                        .getHeaderAlignmentCode());
                vwColumn.setHeading1(viewColumn.getHeading1());
                vwColumn.setHeading2(viewColumn.getHeading2());
                vwColumn.setHeading3(viewColumn.getHeading3());
                vwColumn.setLength(viewColumn.getLength());
                vwColumn.setName(viewColumn.getName());
                vwColumn.setNumericMaskCode(viewColumn.getNumericMaskCode());
                vwColumn.setOrdinalPosition(viewColumn.getOrdinalPosition());
                vwColumn.setRecordAggregationCode(viewColumn
                        .getRecordAggregationCode());
                vwColumn.setScaling(viewColumn.getScaling());
                vwColumn.setSigned(viewColumn.isSigned());
                vwColumn.setSortkeyFooterLabel(viewColumn
                        .getSortkeyFooterLabel());
                vwColumn.setSortKeyLabel(viewColumn.getSortKeyLabel());
                vwColumn.setSpacesBeforeColumn(viewColumn
                        .getSpacesBeforeColumn());
                vwColumn.setStartPosition(viewColumn.getStartPosition());
                vwColumn.setSubtotalLabel(viewColumn.getSubtotalLabel());
                vwColumn.setVisible(viewColumn.isVisible());

                // if View column is a sort key column then copy sort key
                // properties.
                if (viewColumn.isSortKey()) {
                    ViewSortKey viewSortKey = viewColumn.getViewSortKey();
                    ViewSortKey vwSortKey = viewCopy.addSortKey(vwColumn);
                    vwSortKey.setDataTypeCode(viewSortKey.getDataTypeCode());
                    vwSortKey.setDateTimeFormatCode(viewSortKey
                            .getDateTimeFormatCode());
                    vwSortKey.setDecimalPlaces(viewSortKey.getDecimalPlaces());
                    vwSortKey.setDisplayModeCode(viewSortKey
                            .getDisplayModeCode());
                    vwSortKey
                            .setFooterOption(viewSortKey.getFooterOptionCode());
                    vwSortKey
                            .setHeaderOption(viewSortKey.getHeaderOptionCode());
                    vwSortKey.setKeySequenceNo(viewSortKey.getKeySequenceNo());
                    vwSortKey.setLength(viewSortKey.getLength());
                    vwSortKey.setSigned(viewSortKey.isSigned());
                    vwSortKey.setSortkeyLabel(viewSortKey.getSortkeyLabel());
                    vwSortKey.setSortSequenceCode(viewSortKey
                            .getSortSequenceCode());
                    vwSortKey.setStartPosition(viewSortKey.getStartPosition());
                    vwSortKey.setTitleField(viewSortKey.getTitleField());
                    vwSortKey.setTitleLength(viewSortKey.getTitleLength());
                    vwSortKey.setView(viewCopy);

                }
            }

            // copy view header footer
//            if (viewCopy.getOutputFormat() != null && 
//                viewCopy.getOutputFormat().equals(OutputFormat.Format_Report)) {
//                if (this.header != null || this.footer != null) {
//                    if (header != null) {
//                        for (HeaderFooterItem HFitem : this.header.getItems()) {
//
//                            HeaderFooterItem item = new HeaderFooterItem(
//                                    viewCopy.getHeader(),
//                                    HFitem.getFunctionCode(),
//                                    HFitem.getJustifyCode(), HFitem.getRow(),
//                                    HFitem.getColumn(), HFitem.getItemText(),
//                                    HFitem.getEnvironment().getId());
//
//                            viewCopy.getHeader().addItemInit(item);
//
//                        }
//                    }
//                    if (footer != null) {
//                        for (HeaderFooterItem HFitem : this.footer.getItems()) {
//                            HeaderFooterItem item = new HeaderFooterItem(
//                                    viewCopy.getFooter(),
//                                    HFitem.getFunctionCode(),
//                                    HFitem.getJustifyCode(), HFitem.getRow(),
//                                    HFitem.getColumn(), HFitem.getItemText(),
//                                    HFitem.getEnvironment().getId());
//                            viewCopy.footer.addItemInit(item);
//
//                        }
//                    }
//                }
//            }

            viewCopy.validate();
            //Want the view sources LR and LF names not assoc ids
            //do the factory swap here?
            //make sure the view sources is defined in terms of LR and LF names
            viewCopy.store();
            return viewCopy;
    }
	
    @Override
    public ViewSource addViewSource() throws SAFRException {
        WholeViewSource vs = new WholeViewSource(this);
        viewSources.add(vs);
        vs.setSequenceNo(this.viewSources.getActiveItems().size());
        vs.setId(66 * -1);        
        return vs;
    }
}
