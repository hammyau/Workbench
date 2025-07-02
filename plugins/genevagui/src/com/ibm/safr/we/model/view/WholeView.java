package com.ibm.safr.we.model.view;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.flogger.FluentLogger;
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
import com.ibm.safr.we.data.transfer.ViewTransfer;
import com.ibm.safr.we.exceptions.SAFRCancelException;
import com.ibm.safr.we.exceptions.SAFRException;
import com.ibm.safr.we.exceptions.SAFRNotFoundException;
import com.ibm.safr.we.model.SAFRApplication;
import com.ibm.safr.we.model.associations.SAFRAssociationFactory;
import com.ibm.safr.we.model.base.SAFRPersistentObject;

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
                                transList.add(viewSourceTransfer);
                                viewSourceMap.put(viewSourceTransfer,viewSource);
                        }
                            DAOFactoryHolder.getDAOFactory().getViewSourceDAO().persistViewSources(transList);
                            for (int i = 0; i < transList.size(); i++) {
                                ViewSourceTransfer viewSourceTrans = transList.get(i);
                                ViewSource tmpSrc = viewSourceMap.get(viewSourceTrans);
                                tmpSrc.setObjectData(viewSourceTrans);
                            }
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
//                    for (ViewColumnSource viewColumnSource : viewColumnSources) {
//                        // set extract column assignment text and the
//                        // compiled version if this view is active.
//                        if (!isForImport() && !isForMigration() && viewColumnSource.getSourceType().getGeneralId() == Codes.FORMULA) {
//                            if (viewColumnSource.getExtractColumnAssignment() == null || viewColumnSource.getExtractColumnAssignment().isEmpty()) {
//                                logger.warning("No logic text for View Column Source " + viewColumnSource.getId());
//                                if (!getConfirmWarningStrategy().confirmWarning("Saving View",
//                                    "Extract Column Assignment in Column: "+ 
//                                    viewColumnSource.getViewColumn().getColumnNo()
//                                    + ", View Source: "+ viewColumnSource.getViewSource().getSequenceNo()
//                                    + " contains no logic text. Continue saving?")) {
//                                    SAFRCancelException svce = new SAFRCancelException();
//                                    throw svce;
//                                }
//                            }
//                        }
//                    }
                    
                    // TC18596 removed the conditional check !batchActivated
                    // because activation can modify a View Sort Key
                    
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
//                    // If view batch activated, Headers/Footers haven't changed so don't save them
//                    if (!batchActivated) {
//                        if (header != null || footer != null) {
//                            // save view header footer for view of type hardcopy
//                            List<HeaderFooterItemTransfer> viewHeaderFooterItemsTransfers = new ArrayList<HeaderFooterItemTransfer>();
//                            if (this.outputFormat == OutputFormat.Format_Report) {
//                                // merging the list of HeaderFooterItems for Header
//                                // and
//                                // Footer
//                                if (header != null) {
//                                    for (HeaderFooterItem HFitem : this.header
//                                            .getItems()) {
//                                        HeaderFooterItemTransfer hfItemTransfer = new HeaderFooterItemTransfer();
//                                        HFitem.setTransferData(hfItemTransfer);
//                                        viewHeaderFooterItemsTransfers
//                                                .add(hfItemTransfer);
//
//                                    }
//                                }
//                                if (footer != null) {
//                                    for (HeaderFooterItem HFitem : this.footer
//                                            .getItems()) {
//                                        HeaderFooterItemTransfer hfItemTransfer = new HeaderFooterItemTransfer();
//                                        HFitem.setTransferData(hfItemTransfer);
//                                        viewHeaderFooterItemsTransfers
//                                                .add(hfItemTransfer);
//
//                                    }
//                                }
//                            }
//                            // Persist the list of items.
//                            DAOFactoryHolder
//                                    .getDAOFactory()
//                                    .getHeaderFooterDAO()
//                                    .persistHeaderFooter(
//                                            viewHeaderFooterItemsTransfers,
//                                            this.getId(),
//                                            this.getEnvironmentId());
//                        }
//                    }
                    
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
                    
//                    if (!existsAllViewsAssociation()) {
//                        DAOFactoryHolder.getDAOFactory().getViewFolderDAO().
//                            addAllViewsAssociation(getId(),getEnvironmentId());
//                        vfAssociations = SAFRAssociationFactory.getViewToViewFolderAssociations(this,false);
//                    }
//                    success = true;
                    
//                    SAFRApplication.getModelCount().incCount(this.getClass(), 1);       
//                    SAFRApplication.getModelCount().incCount(ViewSource.class, viewSources.size());       
//                    SAFRApplication.getModelCount().incCount(ViewColumn.class, viewColumns.size());       
//                    SAFRApplication.getModelCount().incCount(ViewColumnSource.class, viewColumnSources.size());       
//                    SAFRApplication.getModelCount().incCount(ViewSortKey.class, viewSortKeys.size()); 
//                    if (viewLogicDependencies != null) {
//                        SAFRApplication.getModelCount().incCount(ViewLogicDependency.class, viewLogicDependencies.size());                         
//                    }
//                    if (header != null) {
//                        SAFRApplication.getModelCount().incCount(HeaderFooterItem.class, header.getItems().size());                               
//                    }
//                    if (footer != null) {
//                        SAFRApplication.getModelCount().incCount(HeaderFooterItem.class, footer.getItems().size());                               
//                    }
                                        
                    viewTrans = DAOFactoryHolder.getDAOFactory().getViewDAO().persistView(viewTrans);

	}
}
