package com.ibm.safr.we.internal.data.dao.yamldao;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.genevaers.repository.data.ComponentCollection;

import com.ibm.safr.we.data.transfer.ControlRecordTransfer;

public class YAMLCache {

	private static Map<Integer, ControlRecordTransfer> crs = new TreeMap<>();
	private static Map<String, ControlRecordTransfer> crsByName = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
	//private static ComponentCollection<ControlRecordTransfer> crs = new ComponentCollection<ControlRecordTransfer>();
	
    public void addCR(ControlRecordTransfer c, int id, String name) {
		crs.put(id, c);
		crsByName.put(name, c);
	}

    public void addCR(ControlRecordTransfer c, int id) {
		crs.put(id, c);
		crsByName.put(c.getName(), c);
	}

	public int crsize() {
		return crs.size();
	}

	public Collection<ControlRecordTransfer> getCRValues() {
		return crs.values();
	}

	public ControlRecordTransfer getCR(int id) {
		return crs.get(id);
	}

	public ControlRecordTransfer getCR(String name) {
		return crsByName.get(name);
	}

    public Iterator<ControlRecordTransfer> getCRIterator() {
		return crs.values().iterator();
    }
    
    public void removeCR(ControlRecordTransfer c) {
		crs.remove(c.getId());
		crsByName.remove(c.getName());
	}
    
    public void clearCRs() {
    	crs.clear();
    	crsByName.clear();
    }
	
}
