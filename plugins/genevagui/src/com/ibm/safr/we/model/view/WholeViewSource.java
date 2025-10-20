package com.ibm.safr.we.model.view;

public class WholeViewSource extends ViewSource {
	
	String lr;
	String lf;

	WholeViewSource(View parentView) {
		super(parentView);
	}

	public String getLr() {
		return lr;
	}

	public void setLr(String lr) {
		this.lr = lr;
	}

	public String getLf() {
		return lf;
	}

	public void setLf(String lf) {
		this.lf = lf;
	}

	
}
