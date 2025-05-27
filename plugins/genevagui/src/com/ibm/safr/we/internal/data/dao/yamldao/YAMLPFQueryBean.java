package com.ibm.safr.we.internal.data.dao.yamldao;

import com.ibm.safr.we.model.query.PhysicalFileQueryBean;

public class YAMLPFQueryBean extends PhysicalFileQueryBean {
	
	public YAMLPFQueryBean(String n, int i) {
		this.setId(i);
		this.setName(n);
	}

}
