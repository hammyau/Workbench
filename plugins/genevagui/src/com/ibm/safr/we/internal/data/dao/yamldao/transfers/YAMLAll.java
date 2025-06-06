package com.ibm.safr.we.internal.data.dao.yamldao.transfers;

import com.ibm.safr.we.data.transfer.*;
import java.util.ArrayList;
import java.util.List;

public class YAMLAll {

	private String logpoint;
	private List<ControlRecordTransfer> crs = new ArrayList<>();
	private List<UserExitRoutineTransfer> exits = new ArrayList<>();
	private List<PhysicalFileTransfer> pfs = new ArrayList<>();
	private List<YAMLLogicalFileTransfer> lfs = new ArrayList<>();
	private List<YAMLLogicalRecordTransfer> lrs = new ArrayList<>();
	private List<ComponentAssociationTransfer> lrlfs = new ArrayList<>();
	private List<YAMLLookupTransfer> lks = new ArrayList<>();
	private List<YAMLLookupPathStepTransfer> lksteps = new ArrayList<>();
	private List<YAMLLookupPathSourceFieldTransfer> lkstepsources = new ArrayList<>();
	public List<ControlRecordTransfer> getCrs() {
		return crs;
	}
	public void setCrs(List<ControlRecordTransfer> crs) {
		this.crs = crs;
	}
	public List<UserExitRoutineTransfer> getExits() {
		return exits;
	}
	public void setExits(List<UserExitRoutineTransfer> exits) {
		this.exits = exits;
	}
	public List<PhysicalFileTransfer> getPfs() {
		return pfs;
	}
	public void setPfs(List<PhysicalFileTransfer> pfs) {
		this.pfs = pfs;
	}
	public List<YAMLLogicalFileTransfer> getLfs() {
		return lfs;
	}
	public void setLfs(List<YAMLLogicalFileTransfer> lfs) {
		this.lfs = lfs;
	}
	public List<YAMLLogicalRecordTransfer> getLrs() {
		return lrs;
	}
	public void setLrs(List<YAMLLogicalRecordTransfer> lrs) {
		this.lrs = lrs;
	}
	public List<ComponentAssociationTransfer> getLrlfs() {
		return lrlfs;
	}
	public void setLrlfs(List<ComponentAssociationTransfer> lrlfs) {
		this.lrlfs = lrlfs;
	}
	public List<YAMLLookupTransfer> getLks() {
		return lks;
	}
	public void setLks(List<YAMLLookupTransfer> lks) {
		this.lks = lks;
	}
	public List<YAMLLookupPathStepTransfer> getLksteps() {
		return lksteps;
	}
	public void setLksteps(List<YAMLLookupPathStepTransfer> lksteps) {
		this.lksteps = lksteps;
	}
	public List<YAMLLookupPathSourceFieldTransfer> getLkstepsources() {
		return lkstepsources;
	}
	public void setLkstepsources(List<YAMLLookupPathSourceFieldTransfer> lkstepsources) {
		this.lkstepsources = lkstepsources;
	}
	public String getLogpoint() {
		return logpoint;
	}
	public void setLogpoint(String logpoint) {
		this.logpoint = logpoint;
	}
	
	
	
}
