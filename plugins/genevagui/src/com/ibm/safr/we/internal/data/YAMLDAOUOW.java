package com.ibm.safr.we.internal.data;

/*
 * Copyright Contributors to the GenevaERS Project. SPDX-License-Identifier: Apache-2.0 (c) Copyright IBM Corporation 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.sql.Connection;
import java.sql.SQLException;

import com.ibm.safr.we.data.DAOException;
import com.ibm.safr.we.data.DAOFactoryHolder;
import com.ibm.safr.we.data.DAOUOW;
import com.ibm.safr.we.data.DataUtilities;

/**
 * A DB2 specific implementation of DAOUOW, based on the singleton pattern.
 * 
 */
public class YAMLDAOUOW extends DAOUOW {

	// Package private ctor to restrict instantiation to dao factory.
	YAMLDAOUOW() {
	}

	protected void doBegin() throws DAOException {
	}

	protected void doEnd() throws DAOException {
	}

	protected void doFail() throws DAOException {
	}

}
