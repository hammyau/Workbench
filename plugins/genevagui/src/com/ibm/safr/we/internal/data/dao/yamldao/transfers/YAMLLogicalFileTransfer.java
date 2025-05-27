package com.ibm.safr.we.internal.data.dao.yamldao.transfers;

import com.ibm.safr.we.data.transfer.LogicalFileTransfer;
import java.util.Map;
import java.util.TreeMap;

public class YAMLLogicalFileTransfer extends LogicalFileTransfer {
	private Map<Integer, String> pfs = new TreeMap<>();
	
	public void addPF(Integer id, String name) {
		pfs.put(id, name);
	}
	
	public Map<Integer, String> getPfs() {
		return pfs;
	}
	
	public void setPfs(Map<Integer, String> ps) {
		pfs = ps;
	}
	
	public void removePF(Integer id) {
		pfs.remove(id);
	}
}
