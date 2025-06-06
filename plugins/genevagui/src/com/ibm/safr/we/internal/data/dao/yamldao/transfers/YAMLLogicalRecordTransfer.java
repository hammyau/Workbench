package com.ibm.safr.we.internal.data.dao.yamldao.transfers;

import com.ibm.safr.we.data.transfer.LogicalRecordTransfer;
import com.ibm.safr.we.data.transfer.LRIndexFieldTransfer;
import com.ibm.safr.we.data.transfer.LRIndexTransfer;
import com.ibm.safr.we.data.transfer.LRFieldTransfer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class YAMLLogicalRecordTransfer extends LogicalRecordTransfer {
	private Map<Integer, String> lfs = new TreeMap<>();
	private Map<Integer, LRFieldTransfer> fields = new TreeMap<>();
	private List<LRIndexFieldTransfer> indexFields = new ArrayList<>();
	private LRIndexTransfer index;
	
	public void addLF(Integer id, String name) {
		lfs.put(id, name);
	}
	
	public Map<Integer, String> getLfs() {
		return lfs;
	}
	
	/* This is stored as a map is name pair. Could just be a list of names
	 * Do not confuse the is with the association id*/
	public void setLfs(Map<Integer, String> ps) {
		lfs = ps;
	}
	
	public void removeLF(Integer id) {
		lfs.remove(id);
	}
	
	public void addField(LRFieldTransfer f) {
		fields.put(f.getId(), f);
	}
	
	public void removeField(Integer f) {
		fields.remove(f);
	}
	
	public Map<Integer, LRFieldTransfer> getFields() {
		return fields;
	}
	
	public void setFields(Map<Integer, LRFieldTransfer> fields) {
		this.fields = fields;
	}
	
	public void clearLFs() {
		lfs.clear();
	}
	public void addIndex(LRIndexFieldTransfer i) {
		indexFields.add(i);
	}
	
	public List<LRIndexFieldTransfer> getIndexes() {
		return indexFields;
	}
	
	public void setIndexes(List<LRIndexFieldTransfer> ndxs) {
		this.indexFields = ndxs;
	}
	
	public void clearIndexes() {
		indexFields.clear();
	}
	
	public void setIndex(LRIndexTransfer i ) {
		index = i;
	}
	
	public LRIndexTransfer getIndex() {
		return index;
	}
}
