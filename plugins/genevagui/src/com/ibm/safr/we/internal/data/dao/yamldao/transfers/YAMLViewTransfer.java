package com.ibm.safr.we.internal.data.dao.yamldao.transfers;

import java.util.ArrayList;
import java.util.List;

import com.ibm.safr.we.data.transfer.ViewColumnTransfer;
import com.ibm.safr.we.data.transfer.ViewColumnSourceTransfer;
import com.ibm.safr.we.data.transfer.ViewSourceTransfer;
import com.ibm.safr.we.data.transfer.ViewSortKeyTransfer;
import com.ibm.safr.we.data.transfer.ViewTransfer;

public class YAMLViewTransfer extends ViewTransfer {
	
	private List<YAMLViewSourceTransfer> viewSources = new ArrayList<>();
	private List<YAMLViewColumnTransfer> viewColumns = new ArrayList<>();
	private List<ViewSortKeyTransfer> viewSortKeys = new ArrayList<>();
	
	public YAMLViewTransfer() {
		super();
	}

	public YAMLViewTransfer(ViewTransfer viewTransfer) {
//		this.setActivated(viewTransfer.isActivated());
//		this.setActivatedBy(viewTransfer.getActivatedBy());
//		this.setActivatedTime(viewTransfer.getActivatedTime());
//		this.setComments(viewTransfer.getComments());
//		this.setCompilerVersion(viewTransfer.getCompilerVersion());
		this.setControlRecId(viewTransfer.getControlRecId());
		this.setEnvironmentId(viewTransfer.getEnvironmentId());
		this.setExtractMaxRecCount(viewTransfer.getExtractMaxRecCount());
//		this.setExtractSummaryBuffer(viewTransfer.getExtractSummaryBuffer());
//		this.setFieldDelimCode(viewTransfer.getFieldDelimCode());
//		this.setHeaderRow(viewTransfer.isHeaderRow());
//		this.setFormatExitParams(viewTransfer.getFormatExitParams());
//		this.setFormatExitId(viewTransfer.getFormatExitId());
//		this.setFormatFilterlogic(viewTransfer.getFormatFilterlogic());
//		this.setExtractFileAssocId(viewTransfer.getExtractFileAssocId());
//		this.setOutputFormatCode(viewTransfer.getOutputFormatCode());
//		this.setWorkFileNumber(viewTransfer.getWorkFileNumber());
		this.setId(viewTransfer.getId());
//		this.setLineSize(viewTransfer.getLineSize());
		this.setName(viewTransfer.getName());
		this.setOutputFormatCode(viewTransfer.getOutputFormatCode());
		this.setOutputMaxRecCount(viewTransfer.getOutputMaxRecCount());
//		this.setPageSize(viewTransfer.getPageSize());
		this.setStatusCode(viewTransfer.getStatusCode());
//		this.setStringDelimCode(viewTransfer.getStringDelimCode());
		this.setTypeCode(viewTransfer.getTypeCode());
//		this.setWriteExitId(viewTransfer.getWriteExitId());
//		this.setWriteExitParams(viewTransfer.getWriteExitParams());
//		this.setZeroSuppressInd(viewTransfer.isSuppressZeroRecords());
	}
	
	public Boolean isExtractSummaryIndicator() {
		return isAggregateBySortKey();
	}

	public Boolean isZeroSuppressInd() {
		return isSuppressZeroRecords();
	}


	public List<YAMLViewSourceTransfer> getViewSources() {
		return viewSources;
	}

	public void setViewSources(List<YAMLViewSourceTransfer> viewSources) {
		this.viewSources = viewSources;
	}
	
	public void addViewSource(YAMLViewSourceTransfer vst) {
		viewSources.add(vst);
	}

	public List<YAMLViewColumnTransfer> getViewColumns() {
		return viewColumns;
	}

	public void setViewColumns(List<YAMLViewColumnTransfer> viewColumns) {
		this.viewColumns = viewColumns;
	}
	
	public void addViewColumn(YAMLViewColumnTransfer vct) {
		viewColumns.add(vct);
	}

	public List<ViewSortKeyTransfer> getViewSortKeys() {
		return viewSortKeys;
	}

	public void setViewSortKeys(List<ViewSortKeyTransfer> viewSortKeys) {
		this.viewSortKeys = viewSortKeys;
	}
	
	public void addViewSortKey(ViewSortKeyTransfer vskt) {
		viewSortKeys.add(vskt);
	}

	

}
