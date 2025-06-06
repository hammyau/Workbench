package com.ibm.safr.we.internal.data.dao.yamldao.transfers;

import com.ibm.safr.we.data.transfer.LookupPathTransfer;
import com.ibm.safr.we.data.transfer.LRIndexFieldTransfer;
import com.ibm.safr.we.data.transfer.LRIndexTransfer;
import com.ibm.safr.we.data.transfer.LookupPathSourceFieldTransfer;
import com.ibm.safr.we.data.transfer.LookupPathStepTransfer;
import com.ibm.safr.we.data.transfer.LRFieldTransfer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class YAMLLookupTransfer extends LookupPathTransfer {
	
	private List<YAMLLookupPathStepTransfer> steps = new ArrayList<>();
	private int targetLR;
	private int targetLF;

	public YAMLLookupTransfer() {
		
	}
	
	public void addStep(YAMLLookupPathStepTransfer s) {
		steps.add(s);
	}
	
	public List<YAMLLookupPathStepTransfer> getSteps() {
		return steps;
	}
	
	public void setSteps(List<YAMLLookupPathStepTransfer> steps) {
		this.steps = steps;
	}
	
	public void clearSteps() {
		steps.clear();
	}
	
	public int getTargetLF() {
		return targetLF;
	}
	
	public int getTargetLR() {
		return targetLR;
	}
	
	public void setTargetLF(int targetLF) {
		this.targetLF = targetLF;
	}
	
	public void setTargetLR(int targetLR) {
		this.targetLR = targetLR;
	}

}
