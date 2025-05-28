package com.ibm.safr.we.internal.data.dao.yamldao.transfers;

import com.ibm.safr.we.data.transfer.LogicalRecordTransfer;
import java.util.Map;
import java.util.TreeMap;

public class YAMLLogicalRecordTransfer extends LogicalRecordTransfer {
	private Map<Integer, String> lfs = new TreeMap<>();
	
	public void addLF(Integer id, String name) {
		lfs.put(id, name);
	}
	
	public Map<Integer, String> getPfs() {
		return lfs;
	}
	
	public void setLfs(Map<Integer, String> ps) {
		lfs = ps;
	}
	
	public void removeLF(Integer id) {
		lfs.remove(id);
	}
}
