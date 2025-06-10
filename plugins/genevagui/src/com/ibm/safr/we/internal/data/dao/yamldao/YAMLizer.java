package com.ibm.safr.we.internal.data.dao.yamldao;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.flogger.FluentLogger;
import com.ibm.safr.we.constants.ComponentType;
import com.ibm.safr.we.data.transfer.SAFRTransfer;
import com.ibm.safr.we.model.CodeSet;
import com.ibm.safr.we.model.SAFRApplication;
import com.ibm.safr.we.data.transfer.ControlRecordTransfer;
import com.ibm.safr.we.data.transfer.PhysicalFileTransfer;
import com.ibm.safr.we.data.transfer.UserExitRoutineTransfer;
import com.ibm.safr.we.internal.data.YAMLDAOFactory;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLogicalFileTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLogicalRecordTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLLookupTransfer;
import com.ibm.safr.we.internal.data.dao.yamldao.transfers.YAMLViewTransfer;

public class YAMLizer {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());;

    public static SAFRTransfer readYaml(Path input, ComponentType ctype) {
    	SAFRTransfer txfr = null;
        yamlMapper.findAndRegisterModules();
        logger.atInfo().log("Read %s %s", ctype, input);
        // cache.contains ? cache.get : read from disk
        try {
        	switch(ctype) {
        	case ControlRecord:
        		return  yamlMapper.readValue(input.toFile(), ControlRecordTransfer.class);
        	case UserExitRoutine:
        		return  yamlMapper.readValue(input.toFile(), UserExitRoutineTransfer.class);
        	case PhysicalFile:
        		return yamlMapper.readValue(input.toFile(), PhysicalFileTransfer.class);
        	case LogicalFile:
        		return yamlMapper.readValue(input.toFile(), YAMLLogicalFileTransfer.class);
        	case LogicalRecord:
        		return yamlMapper.readValue(input.toFile(), YAMLLogicalRecordTransfer.class);
        	case LookupPath:
        		return yamlMapper.readValue(input.toFile(), YAMLLookupTransfer.class);
        	case View:
        		return yamlMapper.readValue(input.toFile(), YAMLViewTransfer.class);
        	}
        } catch (IOException e) {
            logger.atSevere().log("read yaml failed for type %s %s", ctype, e.getMessage());
        };
        return txfr;
    }
    

    
    public static void writeYaml(Path output, SAFRTransfer txfr) {
        try {
            yamlMapper.writeValue(output.toFile(), txfr);
        } catch (IOException e) {
            logger.atSevere().log("write yaml failed\n%s", e.getMessage());
        }
    }
    public static void writeYamlCodes(Path output, Map<String, CodeSet> codemap) {
        try {
            yamlMapper.writeValue(output.toFile(), codemap);
        } catch (IOException e) {
            logger.atSevere().log("write yaml failed\n%s", e.getMessage());
        }
    }
    public static Map<String, CodeSet> readYamlCodes(Path input) {
    	Map<String, CodeSet> codeSet = new TreeMap<>();
        try {
        	TypeReference<TreeMap<String, CodeSet>> typeRef = new TypeReference<TreeMap<String, CodeSet>>() {};
        	return  codeSet = yamlMapper.readValue(input.toFile(), typeRef);
        } catch (IOException e) {
            logger.atSevere().log("write coverage failed\n%s", e.getMessage());
        }
        return codeSet;
    }

	public static Path getLookupsPath() {
		return YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("lks");
	}

	public static Path getLRsPath() {
		return YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("lrs");
	}

	public static Path getLFsPath() {
		return YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("lfs");
	}

	public static Path getPFsPath() {
		return YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("pfs");
	}

	public static Path getExitsPath() {
		return YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("exits");
	}

	public static Path getCRsPath() {
		return YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("crs");
	}

	public static Path getViewsPath() {
		return YAMLDAOFactory.getGersHome().resolve(SAFRApplication.getUserSession().getEnvironment().getName()).resolve("views");
	}

}
